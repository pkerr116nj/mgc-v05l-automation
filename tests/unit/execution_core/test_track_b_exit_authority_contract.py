from __future__ import annotations

from datetime import UTC, datetime

from mgc_v05l.execution_core.track_b_exit_authority_contract import (
    EXIT_AUTHORITY_VALIDATOR_VERSION,
    AttributionStatus,
    CloseQtySource,
    ExecutionDomain,
    ExitAuthorityDecision,
    ExitAuthorityDecisionValue,
    ExitAuthorityValidator,
    ExitIntent,
    SourceArtifactRef,
    build_exit_intent_idempotency_key,
    validate_exit_authority,
)


NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def test_attributed_full_close_allowed() -> None:
    decision = validate_exit_authority(intent=_intent(), current_state=_state(), validated_at=NOW)

    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.attribution_status == AttributionStatus.ATTRIBUTED
    assert decision.execution_domain == ExecutionDomain.TRACK_B_PAPER
    assert decision.account_id == "DUM882026"
    assert decision.validated_close_qty == 1
    assert decision.validated_remaining_qty == 0
    assert decision.block_reasons == ()
    assert decision.validator_version == EXIT_AUTHORITY_VALIDATOR_VERSION


def test_attributed_short_full_close_allowed() -> None:
    decision = validate_exit_authority(
        intent=_intent(position_side="SHORT", close_action="BUY"),
        current_state=_state(broker_position_side="SHORT"),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.block_reasons == ()


def test_unattributed_broker_scoped_full_risk_exit_degraded_allowed() -> None:
    decision = validate_exit_authority(
        intent=_intent(attribution={}, lifecycle_id=None, trade_id=None, strategy_id=None, lane_id=None),
        current_state=_state(attribution_status="UNATTRIBUTED"),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.DEGRADED_ALLOWED
    assert decision.attribution_status == AttributionStatus.UNATTRIBUTED
    assert decision.attribution_diagnostics["blocks_authority"] is False
    assert decision.block_reasons == ()


def test_attributed_partial_close_allowed() -> None:
    decision = validate_exit_authority(
        intent=_intent(
            owned_qty=3,
            close_qty=1,
            remaining_qty_after=2,
            exit_type="PARTIAL_SCALE_OUT",
            allow_partial=True,
            partial_policy_supported=True,
        ),
        current_state=_state(broker_position_qty=3),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.validated_close_qty == 1
    assert decision.validated_remaining_qty == 2
    assert decision.conditional_risk_checks["partial_close_qty_source"]["passed"] is True


def test_partial_close_blocked_when_not_declared() -> None:
    decision = validate_exit_authority(
        intent=_intent_payload(
            owned_qty=3,
            close_qty=1,
            remaining_qty_after=2,
            exit_type="PARTIAL_SCALE_OUT",
            allow_partial=False,
            partial_policy_supported=True,
        ),
        current_state=_state(broker_position_qty=3),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert any("partial closes require allow_partial=true" in reason for reason in decision.block_reasons)


def test_unattributed_partial_close_with_operator_qty_degraded_allowed() -> None:
    decision = validate_exit_authority(
        intent=_intent(
            attribution={},
            lifecycle_id=None,
            trade_id=None,
            strategy_id=None,
            lane_id=None,
            owned_qty=3,
            close_qty=1,
            remaining_qty_after=2,
            close_qty_source="OPERATOR_INSTRUCTION",
            exit_type="PARTIAL_SCALE_OUT",
            allow_partial=True,
            partial_policy_supported=True,
        ),
        current_state=_state(broker_position_qty=3, attribution_status="UNATTRIBUTED"),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.DEGRADED_ALLOWED
    assert decision.attribution_status == AttributionStatus.UNATTRIBUTED
    assert decision.block_reasons == ()


def test_missing_lifecycle_trade_strategy_does_not_block() -> None:
    decision = validate_exit_authority(
        intent=_intent(lifecycle_id=None, trade_id=None, strategy_id=None, lane_id=None, attribution={}),
        current_state=_state(attribution_status="UNATTRIBUTED"),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.DEGRADED_ALLOWED
    assert "attribution_incomplete" == decision.diagnostic_checks["attribution_status"]["code"]
    assert decision.block_reasons == ()


def test_wrong_account_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent(),
        current_state=_state(account_id="OTHER_ACCOUNT"),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "account_mismatch" in decision.block_reasons


def test_wrong_domain_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent(execution_domain="TRACK_B_PAPER"),
        current_state=_state(execution_domain="TRACK_B_LIVE"),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "execution_domain_mismatch" in decision.block_reasons


def test_close_qty_greater_than_broker_position_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent(owned_qty=4, close_qty=4, remaining_qty_after=0),
        current_state=_state(broker_position_qty=3),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "close_qty_out_of_bounds" in decision.block_reasons


def test_close_qty_zero_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent_payload(close_qty=0, remaining_qty_after=1),
        current_state=_state(),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert any("close_qty must be a positive" in reason for reason in decision.block_reasons)


def test_wrong_close_side_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent_payload(position_side="LONG", close_action="BUY"),
        current_state=_state(),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert any("long positions require SELL" in reason for reason in decision.block_reasons)


def test_flip_reverse_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent_payload(allow_reverse=True),
        current_state=_state(),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert any("reverse/flip exits" in reason for reason in decision.block_reasons)


def test_same_contract_working_close_over_close_risk_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent(
            owned_qty=3,
            close_qty=2,
            remaining_qty_after=1,
            exit_type="PARTIAL_SCALE_OUT",
            allow_partial=True,
            partial_policy_supported=True,
        ),
        current_state=_state(broker_position_qty=3, same_contract_working_close_qty=2),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "same_contract_working_close_over_close_risk" in decision.block_reasons


def test_unrelated_unknown_order_does_not_block() -> None:
    decision = validate_exit_authority(
        intent=_intent(),
        current_state=_state(unrelated_unknown_order_count=2),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.diagnostic_checks["unrelated_unknown_orders"]["passed"] is False
    assert decision.block_reasons == ()


def test_same_contract_unknown_order_with_over_close_ruled_out_is_degraded_allowed() -> None:
    decision = validate_exit_authority(
        intent=_intent(),
        current_state=_state(
            same_contract_unknown_order_count=1,
            same_contract_unknown_order_could_over_close=False,
            same_contract_unknown_order_over_close_ruled_out=True,
        ),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.DEGRADED_ALLOWED
    assert decision.conditional_risk_checks["same_contract_unknown_order_risk"]["passed"] is True
    assert decision.block_reasons == ()


def test_same_contract_unknown_order_possible_over_close_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent(),
        current_state=_state(
            same_contract_unknown_order_count=1,
            same_contract_unknown_order_could_over_close=True,
            same_contract_unknown_order_over_close_ruled_out=False,
        ),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "same_contract_unknown_order_over_close_risk" in decision.block_reasons


def test_live_money_blocks_outside_explicit_live_domain() -> None:
    decision = validate_exit_authority(
        intent=_intent_payload(live_money_eligible=True),
        current_state=_state(),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert any("live_money_eligible requires TRACK_B_LIVE" in reason for reason in decision.block_reasons)


def test_live_money_allowed_only_inside_explicit_live_domain() -> None:
    decision = validate_exit_authority(
        intent=_intent(execution_domain="TRACK_B_LIVE", live_money_eligible=True, live_money_allowed=True),
        current_state=_state(execution_domain="TRACK_B_LIVE", live_money_eligible=True, live_money_allowed=True),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.execution_domain == ExecutionDomain.TRACK_B_LIVE


def test_paper_proof_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent_payload(paper_proof_invoked=True),
        current_state=_state(),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert any("paper_proof_invoked" in reason for reason in decision.block_reasons)


def test_safe_state_hard_halt_blocks() -> None:
    decision = validate_exit_authority(
        intent=_intent(),
        current_state=_state(safe_state_hard_halt=True),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "safe_state_hard_halt" in decision.block_reasons


def test_diagnostics_do_not_block_safety_authority() -> None:
    decision = validate_exit_authority(
        intent=_intent(),
        current_state=_state(
            reconciliation_clean=False,
            safe_state_allows_managed_close=False,
            guardian_allows_exact_close=False,
            bsa_managed_risk_reducing_close=False,
            bsa_degraded_exact_close_ready=False,
            diagnostics={"ods_fresh": False, "runtime_live": False},
        ),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.diagnostic_checks["reconciliation_clean"]["passed"] is False
    assert decision.diagnostic_checks["safe_state_allows_managed_close"]["passed"] is False
    assert decision.diagnostic_checks["guardian_allows_exact_close"]["passed"] is False
    assert decision.diagnostic_checks["bsa_close_authority"]["passed"] is False
    assert decision.diagnostic_checks["ods_fresh"]["passed"] is False


def test_decision_values_do_not_include_diagnostic_only() -> None:
    assert {item.value for item in ExitAuthorityDecisionValue} == {
        "ALLOWED",
        "DEGRADED_ALLOWED",
        "BLOCKED",
    }


def test_decision_output_carries_v1_1_sections() -> None:
    decision = ExitAuthorityDecision(
        exit_intent_id="exit-intent-1",
        decision="DEGRADED_ALLOWED",
        attribution_status="PARTIALLY_ATTRIBUTED",
        attribution_diagnostics={"blocks_authority": False},
        hard_required_checks={"known_position": {"passed": True}},
        conditional_risk_checks={"same_contract_unknown_order_risk": {"passed": True}},
        diagnostic_checks={"ods_fresh": {"passed": False}},
        execution_domain="TRACK_B_PAPER",
        account_id="DUM882026",
        validated_close_qty=1,
        validated_remaining_qty=0,
        source_artifact_refs=(_source_artifact("bsa"),),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.DEGRADED_ALLOWED
    assert decision.attribution_status == AttributionStatus.PARTIALLY_ATTRIBUTED
    assert decision.conditional_checks == decision.conditional_risk_checks
    assert decision.source_artifact_refs[0].name == "bsa"


def test_idempotency_key_is_deterministic() -> None:
    first = _intent(exit_intent_id="exit-intent-a")
    second = _intent(exit_intent_id="exit-intent-b")

    assert first.idempotency_key == second.idempotency_key
    assert first.idempotency_key == build_exit_intent_idempotency_key(first)
    assert first.idempotency_key.startswith("track_b_exit_intent:")


def test_source_artifact_refs_are_carried_through() -> None:
    source = _source_artifact("managed_positions")
    intent = _intent(source_artifact_refs=(source,))
    state = _state(source_artifact_refs=(_source_artifact("broker_truth_lease"),))

    decision = ExitAuthorityValidator().validate(intent=intent, current_state=state, validated_at=NOW)

    assert intent.source_artifact_refs[0].name == "managed_positions"
    assert [row.name for row in decision.source_artifact_refs] == [
        "managed_positions",
        "broker_truth_lease",
    ]


def _intent(**overrides) -> ExitIntent:
    return ExitIntent(**_intent_payload(**overrides))


def _intent_payload(**overrides) -> dict:
    payload = {
        "exit_intent_id": "exit-intent-1",
        "execution_domain": "TRACK_B_PAPER",
        "account_id": "DUM882026",
        "instrument": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "position_side": "LONG",
        "owned_qty": "1",
        "close_action": "SELL",
        "close_qty": "1",
        "remaining_qty_after": "0",
        "close_qty_source": "STRATEGY_POLICY",
        "exit_type": "FULL_CLOSE",
        "exit_reason": "timebox close",
        "priority": 10,
        "urgency": "NORMAL",
        "price_policy": {"type": "GUARDED_LIMIT", "reference": "current_quote"},
        "idempotency_key": "",
        "allow_partial": False,
        "allow_reverse": False,
        "source_policy_id": "timebox-3x5m-v1",
        "generated_at": NOW,
        "attribution": {
            "lifecycle_id": "life-1",
            "trade_id": "trade-1",
            "strategy_id": "strategy-1",
            "lane_id": "lane-1",
        },
        "lifecycle_id": "life-1",
        "trade_id": "trade-1",
        "strategy_id": "strategy-1",
        "lane_id": "lane-1",
        "source_artifact_refs": (_source_artifact("reconciliation"),),
        "partial_policy_supported": False,
        "live_money_eligible": False,
        "live_money_allowed": False,
        "paper_proof_invoked": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
    }
    payload.update(overrides)
    return payload


def _state(**overrides) -> dict:
    payload = {
        "execution_domain": "TRACK_B_PAPER",
        "known_position": True,
        "broker_position_side": "LONG",
        "broker_position_qty": "1",
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "safe_state_hard_halt": False,
        "same_contract_working_close_qty": "0",
        "unrelated_unknown_order_count": 0,
        "same_contract_unknown_order_count": 0,
        "same_contract_unknown_order_could_over_close": False,
        "same_contract_unknown_order_over_close_ruled_out": False,
        "reconciliation_clean": True,
        "safe_state_allows_managed_close": True,
        "guardian_allows_exact_close": True,
        "bsa_managed_risk_reducing_close": True,
        "bsa_degraded_exact_close_ready": False,
        "live_money_eligible": False,
        "live_money_allowed": False,
        "paper_proof_invoked": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
        "attribution_status": "ATTRIBUTED",
        "attribution_diagnostics": {},
        "diagnostics": {},
        "source_artifact_refs": (_source_artifact("bsa"),),
    }
    payload.update(overrides)
    return payload


def _source_artifact(name: str) -> SourceArtifactRef:
    return SourceArtifactRef(
        name=name,
        path=f"outputs/track_b_execution_core/{name}/latest_{name}.json",
        generated_at=NOW,
        authority_layer="Exit Authority Contract",
    )
