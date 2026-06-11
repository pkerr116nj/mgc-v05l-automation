from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_managed_exit_pipeline_dry_run import (
    ManagedExitPipelineDryRunClassification,
    TrackBManagedExitPipelineDryRunConfig,
    build_track_b_managed_exit_pipeline_dry_run_report,
    run_track_b_managed_exit_pipeline_dry_run_report,
)


NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def test_flat_state_produces_no_positions(tmp_path: Path) -> None:
    payload = _build(tmp_path, _inputs(broker_positions=[]))

    assert payload["classification"] == ManagedExitPipelineDryRunClassification.NO_POSITIONS.value
    assert payload["positions"] == []
    assert payload["generated_exit_intents"] == []


def test_not_due_position_produces_hold_only(tmp_path: Path) -> None:
    payload = _build(tmp_path, _inputs(), decision_inputs={"life-mes": {"timebox_due": False}})

    assert payload["classification"] == ManagedExitPipelineDryRunClassification.HOLD_ONLY.value
    assert payload["decisions"][0]["action"] == "HOLD"
    assert payload["generated_exit_intents"] == []


def test_timebox_due_long_builds_sell_intent_and_allowed_decision(tmp_path: Path) -> None:
    inputs = _inputs(
        broker_positions=[
            _broker_position(quantity="2", local_symbol="MESM6", con_id=770561194, symbol="MES")
        ],
        managed_side="LONG",
        managed_qty="2",
    )

    payload = _build(tmp_path, inputs, decision_inputs={"life-mes": {"timebox_due": True}})

    assert payload["classification"] == ManagedExitPipelineDryRunClassification.EXIT_INTENT_ALLOWED.value
    intent = payload["generated_exit_intents"][0]
    assert intent["close_action"] == "SELL"
    assert intent["close_qty"] == "2"
    assert payload["exit_authority_decisions"][0]["decision"] == "ALLOWED"


def test_timebox_due_with_broker_missing_con_id_uses_enriched_position_identity(tmp_path: Path) -> None:
    inputs = _inputs(
        broker_positions=[
            _broker_position(quantity="1", local_symbol="MESM6", con_id=0, symbol="MES")
        ],
        managed_side="LONG",
        managed_qty="1",
    )
    inputs["managed_positions"]["managed_positions"][0].update(
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "exit_due": True,
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        }
    )

    payload = _build(tmp_path, inputs)

    assert payload["position_state"]["positions"][0]["con_id"] == 770561194
    assert payload["generated_exit_intents"][0]["con_id"] == 770561194
    assert payload["classification"] == ManagedExitPipelineDryRunClassification.EXIT_INTENT_ALLOWED.value
    assert payload["exit_authority_decisions"][0]["decision"] == "ALLOWED"


def test_due_replacement_contract_short_builds_buy_intent_with_enriched_identity(tmp_path: Path) -> None:
    inputs = _inputs(
        broker_positions=[
            _broker_position(quantity="-1", local_symbol="", con_id=0, symbol="MNQ")
        ],
        managed_side="SHORT",
        managed_qty="1",
    )
    inputs["managed_positions"]["managed_positions"][0].update(
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "local_symbol": "MNQU6",
            "con_id": 793356225,
            "symbol": "MNQ",
            "contract_key": "MNQ-202609",
            "exit_due": True,
            "managed_exit_policy_id": "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_60M_EXIT_V1",
        }
    )
    inputs["managed_orders"]["managed_orders"][0].update(
        {
            "local_symbol": "MNQU6",
            "con_id": 793356225,
            "symbol": "MNQ",
            "required_close_action": "BUY",
            "required_close_quantity": "1",
            "working": False,
        }
    )

    payload = _build(tmp_path, inputs)

    position = payload["position_state"]["positions"][0]
    assert position["con_id"] == 793356225
    assert position["local_symbol"] == "MNQU6"
    assert position["instrument"] == "MNQ"
    intent = payload["generated_exit_intents"][0]
    assert intent["local_symbol"] == "MNQU6"
    assert intent["con_id"] == 793356225
    assert intent["close_action"] == "BUY"
    assert intent["close_qty"] == "1"
    assert payload["exit_authority_decisions"][0]["decision"] == "ALLOWED"


def test_timebox_due_short_builds_buy_intent_and_allowed_decision(tmp_path: Path) -> None:
    payload = _build(tmp_path, _inputs(), decision_inputs={"life-mes": {"timebox_due": True}})

    assert payload["classification"] == ManagedExitPipelineDryRunClassification.EXIT_INTENT_ALLOWED.value
    intent = payload["generated_exit_intents"][0]
    assert intent["position_side"] == "SHORT"
    assert intent["close_action"] == "BUY"
    assert payload["exit_authority_decisions"][0]["decision"] == "ALLOWED"


def test_partial_policy_builds_partial_exit_intent(tmp_path: Path) -> None:
    payload = _build(
        tmp_path,
        _inputs(broker_positions=[_broker_position(quantity="-3")], managed_qty="3"),
        decision_inputs={
            "life-mes": {
                "partial_scale_out_due": True,
                "reduce_qty": "1",
                "close_qty_source": "RISK_POLICY",
            }
        },
    )

    intent = payload["generated_exit_intents"][0]
    assert payload["classification"] == ManagedExitPipelineDryRunClassification.EXIT_INTENT_ALLOWED.value
    assert intent["exit_type"] == "PARTIAL_SCALE_OUT"
    assert intent["close_qty"] == "1"
    assert intent["remaining_qty_after"] == "2"
    assert payload["exit_authority_decisions"][0]["decision"] == "ALLOWED"


def test_missing_attribution_produces_broker_scoped_degraded_allowed_exit(tmp_path: Path) -> None:
    inputs = _inputs()
    inputs["managed_positions"]["managed_positions"] = []
    inputs["managed_orders"]["managed_orders"] = []

    payload = _build(tmp_path, inputs, decision_inputs={"MESM6": {"timebox_due": True}})

    assert payload["classification"] == ManagedExitPipelineDryRunClassification.EXIT_INTENT_ALLOWED.value
    decision = payload["exit_authority_decisions"][0]
    assert decision["decision"] == "DEGRADED_ALLOWED"
    assert decision["attribution_status"] == "UNATTRIBUTED"
    assert decision["block_reasons"] == []


def test_over_close_partial_policy_blocks_before_authority(tmp_path: Path) -> None:
    payload = _build(
        tmp_path,
        _inputs(),
        decision_inputs={
            "life-mes": {
                "partial_scale_out_due": True,
                "reduce_qty": "2",
                "close_qty_source": "RISK_POLICY",
            }
        },
    )

    assert payload["classification"] == ManagedExitPipelineDryRunClassification.EXIT_INTENT_BLOCKED.value
    assert payload["pipeline_blockers"][0]["reason"] == "pipeline_contract_blocked"
    assert "reduce_qty cannot exceed qty" in payload["pipeline_blockers"][0]["detail"]


def test_same_priority_selector_tie_blocks_with_review_classification(tmp_path: Path) -> None:
    exit_decision = _exit_decision_with_tie()

    payload = _build(tmp_path, _inputs(), input_overrides={"exit_decision": exit_decision})

    assert payload["classification"] == ManagedExitPipelineDryRunClassification.EXIT_INTENT_BLOCKED.value
    assert payload["exit_strategy_selector"]["classification"] == "EXIT_STRATEGY_SELECTOR_REVIEW_REQUIRED"
    assert payload["generated_exit_intents"] == []


def test_run_can_write_report_without_execution_side_effects(tmp_path: Path) -> None:
    config = TrackBManagedExitPipelineDryRunConfig(repo_root=tmp_path, output_path=Path("pipeline.json"))

    payload = run_track_b_managed_exit_pipeline_dry_run_report(config=config, now=NOW, write=True)

    assert (tmp_path / "pipeline.json").exists()
    assert payload["broker_state_mutated"] is False
    assert payload["submit_attempted"] is False
    assert payload["cancel_attempted"] is False
    assert payload["close_attempted"] is False
    assert payload["service_started"] is False
    assert payload["runtime_restarted"] is False
    assert payload["actuator_integrated"] is False


def _build(
    tmp_path: Path,
    inputs: dict,
    *,
    decision_inputs: dict[str, dict] | None = None,
    input_overrides: dict[str, dict] | None = None,
) -> dict:
    overrides = {**inputs, **(input_overrides or {})}
    return build_track_b_managed_exit_pipeline_dry_run_report(
        config=TrackBManagedExitPipelineDryRunConfig(repo_root=tmp_path),
        now=NOW,
        input_overrides=overrides,
        decision_inputs=decision_inputs or {},
    )


def _inputs(
    *,
    broker_positions: list[dict] | None = None,
    managed_side: str = "SHORT",
    managed_qty: str = "1",
) -> dict:
    positions = [_broker_position()] if broker_positions is None else broker_positions
    managed_positions = []
    managed_orders = []
    if positions:
        managed_positions = [_managed_position(side=managed_side, quantity=managed_qty)]
        managed_orders = [_managed_order(quantity=managed_qty)]
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


def _broker_position(
    *,
    account_id: str = "DUM882026",
    local_symbol: str = "MESM6",
    con_id: int = 770561194,
    symbol: str = "MES",
    quantity: str = "-1",
) -> dict:
    return {
        "account_id": account_id,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "symbol": symbol,
        "track_b_root": symbol,
        "quantity": quantity,
    }


def _managed_position(
    *,
    account_id: str = "DUM882026",
    local_symbol: str = "MESM6",
    con_id: int = 770561194,
    side: str = "SHORT",
    quantity: str = "1",
) -> dict:
    return {
        "classification": "OPEN_MANAGED_MATCHED",
        "account_id": account_id,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "side": side,
        "quantity": quantity,
        "lifecycle_id": "life-mes",
        "trade_id": "trade-mes",
        "strategy_id": "mes_strategy",
        "lane_id": "mes_lane",
    }


def _managed_order(*, quantity: str = "1") -> dict:
    return {
        "classification": "POSITION_WITHOUT_CLOSE_ORDER",
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": quantity,
        "lifecycle_id": "life-mes",
        "trade_id": "trade-mes",
        "strategy_id": "mes_strategy",
        "lane_id": "mes_lane",
        "required_close_action": "BUY",
        "required_close_quantity": quantity,
        "working": False,
    }


def _exit_decision_with_tie() -> dict:
    base = {
        "schema_version": "track_b_exit_decision_report_v1",
        "generated_at": NOW.isoformat(),
        "classification": "EXIT_DECISION_READY",
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    decision_a = _decision(decision_id="profit_a", reason="profit_target_a")
    decision_b = _decision(decision_id="profit_b", reason="profit_target_b")
    base["decisions"] = [decision_a, decision_b]
    return base


def _decision(*, decision_id: str, reason: str) -> dict:
    position = _managed_position()
    position.update(
        {
            "schema_version": "track_b_position_state_v1",
            "execution_domain": "TRACK_B_PAPER",
            "qty": "1",
            "owned_qty": "1",
            "attribution_status": "ATTRIBUTED",
            "current_scope": True,
            "source_artifact_refs": [],
            "diagnostic_rows": [],
        }
    )
    return {
        "schema_version": "track_b_exit_decision_v1",
        "decision_id": decision_id,
        "action": "FULL_CLOSE",
        "execution_domain": "TRACK_B_PAPER",
        "account_id": "DUM882026",
        "con_id": 770561194,
        "local_symbol": "MESM6",
        "instrument": "MES",
        "side": "SHORT",
        "qty": "1",
        "reason": reason,
        "priority": 50,
        "urgency": "NORMAL",
        "source_policy_id": "POLICY_1",
        "position": position,
        "source_artifact_refs": [],
        "diagnostics": [],
    }
