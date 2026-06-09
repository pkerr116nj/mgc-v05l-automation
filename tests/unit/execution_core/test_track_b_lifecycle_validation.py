from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_lifecycle_validation import (
    LifecycleValidationClassification,
    TrackBLifecycleValidationConfig,
    build_track_b_lifecycle_validation_report,
    run_track_b_lifecycle_validation_report,
)


NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def test_flat_instrument_waits_for_entry(tmp_path: Path) -> None:
    payload = _build(tmp_path, _inputs(broker_positions=[]), active_instruments=["MES"])

    row = payload["rows"][0]
    assert row["instrument"] == "MES"
    assert row["group"] == 1
    assert row["classification"] == LifecycleValidationClassification.WAITING_FOR_ENTRY.value
    assert row["failure_class"] is None


def test_open_position_holding(tmp_path: Path) -> None:
    payload = _build(tmp_path, _inputs(), active_instruments=["MES"], decision_inputs={"life-mes": {"timebox_due": False}})

    row = payload["rows"][0]
    assert row["classification"] == LifecycleValidationClassification.HOLDING.value
    assert row["position_state"]["position_count"] == 1
    assert row["exit_decision"]["actions"] == ["HOLD"]


def test_due_position_with_allowed_intent_reports_authority_allowed(tmp_path: Path) -> None:
    payload = _build(tmp_path, _inputs(), active_instruments=["MES"], decision_inputs={"life-mes": {"timebox_due": True}})

    row = payload["rows"][0]
    assert row["classification"] == LifecycleValidationClassification.EXIT_AUTHORITY_ALLOWED.value
    assert row["exit_intent"]["intent_count"] == 1
    assert row["exit_authority_decision"]["decisions"][0]["decision"] == "ALLOWED"


def test_v1_1_blocked_reports_authority_blocked_with_reason(tmp_path: Path) -> None:
    inputs = _inputs()
    unknown = {"account_id": "DUM882026", "local_symbol": "MESM6", "con_id": 770561194, "quantity": "1"}
    inputs["reconciliation"]["unknown_broker_open_order_count"] = 1
    inputs["reconciliation"]["unknown_broker_open_orders"] = [unknown]
    inputs["open_order_truth"]["unknown_open_order_count"] = 1
    inputs["open_order_truth"]["unknown_open_orders"] = [unknown]

    payload = _build(tmp_path, inputs, active_instruments=["MES"], decision_inputs={"life-mes": {"timebox_due": True}})

    row = payload["rows"][0]
    assert row["classification"] == LifecycleValidationClassification.EXIT_AUTHORITY_BLOCKED.value
    assert row["failure_class"] == "v1_1_blocked"
    assert "same_contract_unknown_order_over_close_risk" in row["exit_authority_decision"]["decisions"][0]["block_reasons"]


def test_closed_settled_ods_flat_reports_pass(tmp_path: Path) -> None:
    payload = _build(
        tmp_path,
        _inputs(broker_positions=[]),
        active_instruments=["MES"],
        settlement_by_instrument={
            "MES": {
                "settled_flat": True,
                "reconciliation_clean": True,
                "managed_clean": True,
                "ods_flat": True,
            }
        },
        ods={"broker_state": "FLAT", "first_blocker": None, "next_safe_action": "NO_ACTION"},
    )

    row = payload["rows"][0]
    assert row["classification"] == LifecycleValidationClassification.PASS.value
    assert row["settlement"]["settled_flat"] is True
    assert row["ods"]["broker_state"] == "FLAT"


def test_no_instrument_specific_branching_after_grouping() -> None:
    source = Path("src/mgc_v05l/execution_core/track_b_lifecycle_validation.py").read_text(encoding="utf-8")

    assert 'instrument == "MES"' not in source
    assert 'instrument == "MNQ"' not in source
    assert 'instrument == "MGC"' not in source
    assert "GROUP_1" in source
    assert "GROUP_2" in source


def test_historical_debris_is_diagnostic_only(tmp_path: Path) -> None:
    inputs = _inputs(broker_positions=[])
    inputs["managed_positions"]["managed_positions"] = [
        {
            "classification": "REVIEW_REQUIRED",
            "local_symbol": "MESM6",
            "historical_only": True,
            "current_scope_linked": False,
        }
    ]

    payload = _build(tmp_path, inputs, active_instruments=["MES"])

    row = payload["rows"][0]
    assert row["classification"] == LifecycleValidationClassification.WAITING_FOR_ENTRY.value
    assert row["historical_debris_diagnostic_only"] is True
    assert row["position_state"]["position_count"] == 0


def test_run_can_write_report_without_mutation_paths(tmp_path: Path) -> None:
    config = TrackBLifecycleValidationConfig(repo_root=tmp_path, output_path=Path("validation.json"))

    payload = run_track_b_lifecycle_validation_report(config=config, now=NOW, write=True)

    assert (tmp_path / "validation.json").exists()
    assert payload["read_only"] is True
    assert payload["broker_state_mutated"] is False
    assert payload["submit_attempted"] is False
    assert payload["cancel_attempted"] is False
    assert payload["close_attempted"] is False
    assert payload["service_started"] is False
    assert payload["runtime_restarted"] is False


def _build(
    tmp_path: Path,
    inputs: dict,
    *,
    active_instruments: list[str],
    decision_inputs: dict[str, dict] | None = None,
    settlement_by_instrument: dict[str, dict] | None = None,
    ods: dict | None = None,
) -> dict:
    return build_track_b_lifecycle_validation_report(
        config=TrackBLifecycleValidationConfig(repo_root=tmp_path),
        now=NOW,
        input_overrides={
            **inputs,
            "active_instruments": active_instruments,
            "active_lanes": [_lane(symbol) for symbol in active_instruments],
            "settlement_by_instrument": settlement_by_instrument or {},
            "ods": ods or {"broker_state": "EXPOSED_MANAGED", "first_blocker": None, "next_safe_action": "WAIT"},
        },
        decision_inputs=decision_inputs or {},
    )


def _inputs(*, broker_positions: list[dict] | None = None) -> dict:
    positions = [_broker_position()] if broker_positions is None else broker_positions
    managed_positions = [_managed_position()] if positions else []
    managed_orders = [_managed_order()] if positions else []
    return {
        "reconciliation": {
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_positions": positions,
            "track_b_broker_position_count": len(positions),
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "unknown_broker_open_orders": [],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "managed_positions": {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_MATCHED" if managed_positions else "NO_MANAGED_POSITIONS",
            "managed_positions": managed_positions,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "managed_orders": {
            "generated_at": NOW.isoformat(),
            "classification": "POSITION_WITHOUT_CLOSE_ORDER" if managed_orders else "NO_MANAGED_ORDERS",
            "managed_orders": managed_orders,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "open_order_truth": {
            "generated_at": NOW.isoformat(),
            "classification": "NO_OPEN_ORDERS",
            "broker_open_orders": [],
            "open_orders": [],
            "unknown_open_order_count": 0,
            "unknown_open_orders": [],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "broker_session_authority": {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
            "allowed_uses": {"managed_risk_reducing_close": True},
            "degraded_exact_risk_reducing_close_context": {"ready": True, "blockers": []},
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "guardian": {
            "generated_at": NOW.isoformat(),
            "classification": "BROKER_POSITION_GUARDIAN_READY",
            "managed_close_authority": {"allowed": True},
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "safe_state": {
            "generated_at": NOW.isoformat(),
            "classification": "SAFE_STATE_NORMAL",
            "close_authority": {"allowed": True},
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    }


def _lane(symbol: str) -> dict:
    return {
        "lane_id": f"{symbol.lower()}_lane",
        "strategy_id": f"{symbol.lower()}_strategy",
        "symbol": symbol,
        "observed_instruments": [symbol],
        "managed_exit_policy_id": "PAPER_DIAGNOSTIC_TIME_BOXED_3X5M_EXIT_V1",
        "paper_only": True,
        "live_money_eligible": False,
    }


def _broker_position() -> dict:
    return {
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "symbol": "MES",
        "track_b_root": "MES",
        "quantity": "-1",
    }


def _managed_position() -> dict:
    return {
        "classification": "OPEN_MANAGED_MATCHED",
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "side": "SHORT",
        "quantity": "1",
        "lifecycle_id": "life-mes",
        "trade_id": "trade-mes",
        "strategy_id": "mes_strategy",
        "lane_id": "mes_lane",
    }


def _managed_order() -> dict:
    return {
        "classification": "POSITION_WITHOUT_CLOSE_ORDER",
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "1",
        "lifecycle_id": "life-mes",
        "trade_id": "trade-mes",
        "strategy_id": "mes_strategy",
        "lane_id": "mes_lane",
        "required_close_action": "BUY",
        "required_close_quantity": "1",
        "working": False,
    }
