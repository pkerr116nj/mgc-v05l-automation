from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_order_adjustment_planner import (
    BROKER_FLAT_NO_REPLACE,
    DO_NOT_REPLACE_DUPLICATE_RISK,
    MODIFY_IN_PLACE_ELIGIBLE,
    NO_ACTION_NEEDED,
    REVIEW_REQUIRED_SUSPICIOUS_STATE,
    TARGETED_CANCEL_REPLACE_REQUIRED,
    TrackBOrderAdjustmentPlannerConfig,
    WAIT_FOR_WORKING_ORDER,
    build_track_b_order_adjustment_plan,
    main,
    write_track_b_order_adjustment_plan,
)


NOW = datetime(2026, 5, 23, 14, 30, tzinfo=UTC)


def test_no_open_order_has_no_action_needed_plan(tmp_path: Path) -> None:
    _seed_base(tmp_path)

    payload = _build(tmp_path)

    assert payload["classification"] == NO_ACTION_NEEDED
    assert payload["plans"] == []
    assert payload["dry_run"] is True
    assert payload["mutation_authority"] is False


def test_clean_working_close_away_from_market_is_modify_in_place_eligible(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[
            _managed_order(
                classification="WORKING_CLOSE_ORDER",
                marketable=False,
            )
        ],
        broker_positions=[_broker_position()],
    )

    payload = _build(tmp_path)

    assert payload["classification"] == MODIFY_IN_PLACE_ELIGIBLE
    plan = payload["plans"][0]
    assert plan["classification"] == MODIFY_IN_PLACE_ELIGIBLE
    assert plan["identity_complete_for_modify"] is True
    assert plan["mutation_planned"] is False
    assert plan["managed_close_reprice_policy"]["classification"] == "MANAGED_CLOSE_PRICED"
    assert plan["managed_close_reprice_policy"]["limit_price"] == "29547.75"
    assert plan["managed_close_reprice_policy"]["marketable_limit_offset_ticks"] == 8.0


def test_unfilled_working_close_reprice_escalates_but_caps_slippage(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[
            {
                **_managed_order(classification="WORKING_CLOSE_ORDER", marketable=False),
                "reprice_attempt_count": 20,
                "marketability": {
                    "marketable": False,
                    "market_reference": {"reference_price": "29549.75", "reference_age_seconds": 30.0},
                },
            }
        ],
        broker_positions=[_broker_position()],
    )

    payload = _build(tmp_path)

    policy = payload["plans"][0]["managed_close_reprice_policy"]
    assert policy["classification"] == "MANAGED_CLOSE_PRICED"
    assert policy["limit_price"] == "29545.75"
    assert policy["marketable_limit_offset_ticks"] == 16.0


def test_stale_working_close_reference_blocks_reprice_policy(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[
            {
                **_managed_order(classification="WORKING_CLOSE_ORDER", marketable=False),
                "marketability": {
                    "marketable": False,
                    "market_reference": {"reference_price": "29549.75", "reference_age_seconds": 121.0},
                },
            }
        ],
        broker_positions=[_broker_position()],
    )

    payload = _build(tmp_path)

    policy = payload["plans"][0]["managed_close_reprice_policy"]
    assert policy["classification"] == "MANAGED_CLOSE_PRICING_BLOCKED"
    assert policy["stale_reference_blocker"] == "MANAGED_CLOSE_REFERENCE_STALE"


def test_suspicious_sentinel_order_requires_review_not_auto_replace(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[
            _managed_order(
                classification="CLOSE_ORDER_SUSPICIOUS",
                suspicious_reasons=["sentinel_filled_quantity"],
                source_order={
                    **_source_order(),
                    "filled_quantity": "1.7976931348623157e+308",
                    "remaining_quantity": None,
                },
            )
        ],
        broker_positions=[_broker_position()],
    )

    payload = _build(tmp_path)

    assert payload["classification"] == REVIEW_REQUIRED_SUSPICIOUS_STATE
    plan = payload["plans"][0]
    assert plan["classification"] == REVIEW_REQUIRED_SUSPICIOUS_STATE
    assert plan["recommended_operator_action"] == "OPERATOR_REVIEW_OR_MANUAL_TWS_PATH"
    assert "sentinel_filled_quantity" in plan["suspicious_reasons"]


def test_known_working_close_tolerates_ibkr_sentinel_status_gap(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[
            _managed_order(
                classification="WORKING_CLOSE_ORDER",
                marketable=False,
                source_order={
                    **_source_order(),
                    "filled_quantity": "1.7976931348623157e+308",
                    "remaining_quantity": None,
                },
            )
        ],
        broker_positions=[_broker_position()],
    )

    payload = _build(tmp_path)

    assert payload["classification"] == MODIFY_IN_PLACE_ELIGIBLE
    plan = payload["plans"][0]
    assert plan["classification"] == MODIFY_IN_PLACE_ELIGIBLE
    assert plan["tolerated_ibkr_status_gaps"] == ["missing_remaining_quantity", "sentinel_filled_quantity"]
    assert plan["blocking_suspicious_reasons"] == []


def test_stale_known_close_prefers_modify_in_place_over_cancel_replace(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[
            _managed_order(
                classification="CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
                source_order={
                    **_source_order(),
                    "filled_quantity": "1.7976931348623157e+308",
                    "remaining_quantity": None,
                },
            )
        ],
        broker_positions=[_broker_position()],
    )

    payload = _build(tmp_path)

    assert payload["classification"] == MODIFY_IN_PLACE_ELIGIBLE
    plan = payload["plans"][0]
    assert plan["source_managed_order_classification"] == "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED"
    assert plan["recommended_operator_action"] == "MODIFY_IN_PLACE_CANDIDATE"


def test_cancelled_order_with_still_open_position_requires_targeted_cancel_replace(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[
            _managed_order(
                classification="ORDER_TERMINAL_CANCELLED",
                broker_status="Cancelled",
            )
        ],
        broker_positions=[_broker_position()],
    )

    payload = _build(tmp_path)

    assert payload["classification"] == TARGETED_CANCEL_REPLACE_REQUIRED
    assert payload["plans"][0]["terminal_state_confirmed"] is True
    assert payload["plans"][0]["position_open"] is True


def test_broker_flat_with_working_close_does_not_replace(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[_managed_order(classification="BROKER_FLAT_WITH_WORKING_CLOSE")],
        broker_positions=[],
    )

    payload = _build(tmp_path)

    assert payload["classification"] == BROKER_FLAT_NO_REPLACE
    assert payload["plans"][0]["recommended_operator_action"] == "REVIEW_BROKER_FLAT_WITH_OPEN_CLOSE"


def test_duplicate_close_order_blocks_replacement(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[
            _managed_order(classification="DUPLICATE_CLOSE_ORDER_BLOCKED", broker_order_id="27"),
            _managed_order(classification="DUPLICATE_CLOSE_ORDER_BLOCKED", broker_order_id="28", perm_id="1002"),
        ],
        broker_positions=[_broker_position()],
    )

    payload = _build(tmp_path)

    assert payload["classification"] == DO_NOT_REPLACE_DUPLICATE_RISK
    assert {plan["classification"] for plan in payload["plans"]} == {DO_NOT_REPLACE_DUPLICATE_RISK}


def test_marketable_working_close_waits_for_order(tmp_path: Path) -> None:
    _seed_base(
        tmp_path,
        managed_orders=[_managed_order(classification="WORKING_CLOSE_ORDER", marketable=True)],
        broker_positions=[_broker_position()],
    )

    payload = _build(tmp_path)

    assert payload["classification"] == WAIT_FOR_WORKING_ORDER
    assert payload["plans"][0]["recommended_operator_action"] == "WAIT"


def test_write_authority_artifact(tmp_path: Path) -> None:
    _seed_base(tmp_path)
    config = TrackBOrderAdjustmentPlannerConfig(repo_root=tmp_path)
    payload = build_track_b_order_adjustment_plan(config=config, now=NOW)

    path = write_track_b_order_adjustment_plan(config=config, payload=payload)

    assert path == tmp_path / "outputs/track_b_execution_core/managed_orders/latest_order_adjustment_plan.json"
    assert json.loads(path.read_text(encoding="utf-8"))["classification"] == NO_ACTION_NEEDED


def test_cli_uses_shared_truth_refresh_precondition(tmp_path: Path, capsys) -> None:
    _seed_shared_truth_refresh_inputs(tmp_path)

    exit_code = main(["--repo-root", str(tmp_path)])

    output = capsys.readouterr().out
    assert exit_code == 0
    assert "classification=NO_ACTION_NEEDED" in output
    plan = json.loads(
        (tmp_path / "outputs/track_b_execution_core/managed_orders/latest_order_adjustment_plan.json").read_text(
            encoding="utf-8"
        )
    )
    assert plan["shared_truth_refresh"]["classifications"]["Open Order Truth"] == "NO_OPEN_ORDERS"


def _build(root: Path) -> dict:
    return build_track_b_order_adjustment_plan(config=TrackBOrderAdjustmentPlannerConfig(repo_root=root), now=NOW)


def _seed_base(
    root: Path,
    *,
    managed_orders: list[dict] | None = None,
    broker_positions: list[dict] | None = None,
) -> None:
    managed_orders = managed_orders or []
    broker_positions = broker_positions or []
    _write_json(
        root / "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json",
        {
            "schema_version": "track_b_managed_order_registry_v1",
            "generated_at": NOW.isoformat(),
            "classification": "NO_MANAGED_ORDERS" if not managed_orders else managed_orders[0]["classification"],
            "managed_orders": managed_orders,
            "summary": {"managed_order_count": len(managed_orders)},
            "projection_only": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/open_order_truth/latest_open_order_truth.json",
        {
            "schema_version": "track_b_open_order_truth_v1",
            "generated_at": NOW.isoformat(),
            "classification": "NO_OPEN_ORDERS" if not managed_orders else "OPEN_CLOSE_ORDER_WORKING",
            "order_states": [],
            "broker_positions_without_close_order": [],
            "projection_only": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/position_truth/latest_position_truth.json",
        {
            "schema_version": "track_b_position_truth_v1",
            "generated_at": NOW.isoformat(),
            "summary": {
                "overall_classification": "CLEAN_FLAT_READY" if not broker_positions else "ATTENTION_REQUIRED",
                "broker_exposure_present": bool(broker_positions),
            },
            "broker_positions": broker_positions,
            "open_broker_orders": [],
            "projection_only": False,
        },
    )
    _write_json(
        root / "outputs/track_b_execution_core/phase1_runtime_market_data/MNQ/1m/latest_runtime_candles.json",
        {
            "generated_at": NOW.isoformat(),
            "candles": [{"bar_end": NOW.isoformat(), "close": "29549.75"}],
        },
    )


def _seed_shared_truth_refresh_inputs(root: Path) -> None:
    generated_at = datetime.now(UTC).isoformat()
    _write_json(
        root / "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
        {
            "generated_at": generated_at,
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "live_money_eligible": False,
            "track_b_broker_positions": [],
            "track_b_broker_open_orders": [],
            "track_b_lifecycle_positions": [],
            "unknown_broker_open_orders": [],
            "known_managed_exit_orders": [],
            "unresolved_submit_intent_ownership_records": [],
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "review_required_count": 0,
            "unresolved_submit_intent_ownership_count": 0,
            "lifecycle_open_position_count": 0,
            "lifecycle_open_order_count": 0,
            "position_match_report": {"state": "BROKER_AND_LIFECYCLE_FLAT", "matched": True},
            "blockers": [],
        },
    )
    broker_truth = {
        "classification": "BROKER_TRUTH_REFRESH_READY",
        "account": "DUM882026",
        "generated_at": generated_at,
        "positions_complete": True,
        "open_orders_complete": True,
        "positions": [],
        "open_orders": [],
        "live_money_eligible": False,
    }
    _write_json(
        root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json",
        {
            **broker_truth,
            "last_successful_broker_truth": broker_truth,
            "latest_attempt_status": broker_truth,
        },
    )
    _write_json(root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_latest_attempt_status.json", broker_truth)
    _write_json(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json",
        {"generated_at": generated_at, "open_position_count": 0, "open_positions": [], "review_required_positions": []},
    )
    _write_json(
        root / "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_paper_trade_summary.json",
        {"generated_at": generated_at, "review_required_count": 0, "unknown_open_order_count": 0},
    )
    _write_json(
        root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        {"generated_at": generated_at, "classification": "NO_MANAGED_POSITIONS", "managed_positions": []},
    )


def _managed_order(
    *,
    classification: str,
    broker_order_id: str = "27",
    perm_id: str = "347068546",
    broker_status: str = "Submitted",
    marketable: bool = False,
    suspicious_reasons: list[str] | None = None,
    source_order: dict | None = None,
) -> dict:
    return {
        "classification": classification,
        "recommended_next_action": "WAIT",
        "symbol": "MNQ",
        "contract": "MNQM6",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "action": "SELL",
        "quantity": "1",
        "broker_order_id": broker_order_id,
        "perm_id": perm_id,
        "broker_status": broker_status,
        "limit_price": "29555.50",
        "working": broker_status.upper() not in {"CANCELLED", "APICANCELLED", "INACTIVE", "FILLED"},
        "marketability": {
            "marketable": marketable,
            "market_reference": {"reference_price": "29549.75"},
        },
        "suspicious_reasons": suspicious_reasons or [],
        "source_order": source_order or _source_order(broker_order_id=broker_order_id, perm_id=perm_id, broker_status=broker_status),
        "lifecycle_id": "bridge_fill_MNQ|1m|2026-05-23T14:00:00Z|BUY_TO_OPEN",
        "manifest_id": "MNQ|1m|2026-05-23T14:00:00Z|BUY_TO_OPEN",
        "ownership_id": "submit_owner_test",
    }


def _source_order(
    *,
    broker_order_id: str = "27",
    perm_id: str = "347068546",
    broker_status: str = "Submitted",
) -> dict:
    return {
        "account_id": "DUM882026",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "broker_order_id": broker_order_id,
        "perm_id": perm_id,
        "action": "SELL",
        "quantity": "1",
        "filled_quantity": "0",
        "remaining_quantity": "1",
        "status": broker_status,
    }


def _broker_position() -> dict:
    return {
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "track_b_root": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
    }


def _write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
