from __future__ import annotations

from datetime import UTC, datetime

import pytest

from mgc_v05l.execution_core.models import TrackBModelError
from mgc_v05l.execution_core.track_b_exit_authority_contract import (
    EXIT_AUTHORITY_VALIDATOR_VERSION,
    EXIT_INTENT_SCHEMA_VERSION,
    CloseAction,
    ExitAuthorityDecision,
    ExitAuthorityCheckCategory,
    ExitAuthorityCurrentState,
    ExitAuthorityDecisionValue,
    ExitAuthorityValidator,
    ExitIntent,
    ExitType,
    ExitUrgency,
    PositionSide,
    SourceArtifactRef,
    build_exit_intent_idempotency_key,
    validate_exit_authority,
    validate_exit_intent,
)


NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def test_full_close_long_contract_is_valid() -> None:
    intent = _intent(position_side="LONG", close_action="SELL", owned_qty=1, close_qty=1)

    decision = validate_exit_intent(intent)

    assert intent.schema_version == EXIT_INTENT_SCHEMA_VERSION
    assert intent.position_side == PositionSide.LONG
    assert intent.close_action == CloseAction.SELL
    assert intent.remaining_qty_after == 0
    assert intent.live_money_eligible is False
    assert intent.paper_proof_invoked is False
    assert intent.broad_flatten_allowed is False
    assert intent.global_flatten_allowed is False
    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.hard_required_checks["risk_reducing_action"] is True


def test_full_close_short_contract_is_valid() -> None:
    intent = _intent(position_side="SHORT", close_action="BUY", owned_qty=1, close_qty=1)

    decision = validate_exit_intent(intent)

    assert intent.position_side == PositionSide.SHORT
    assert intent.close_action == CloseAction.BUY
    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED


def test_partial_close_allowed_when_policy_supports_partial() -> None:
    intent = _intent(
        owned_qty=3,
        close_qty=1,
        remaining_qty_after=2,
        exit_type="PARTIAL_SCALE_OUT",
        allow_partial=True,
        partial_policy_supported=True,
    )

    decision = validate_exit_intent(intent)

    assert intent.remaining_qty_after == 2
    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.conditional_checks["partial_close_allowed"] is True


def test_partial_close_blocked_when_not_allowed() -> None:
    payload = _intent_payload(owned_qty=3, close_qty=1, remaining_qty_after=2, exit_type="PARTIAL_SCALE_OUT")

    decision = validate_exit_intent(payload)

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "partial closes require allow_partial=true" in decision.block_reasons[0]


def test_partial_close_blocked_without_policy_support() -> None:
    payload = _intent_payload(
        owned_qty=3,
        close_qty=1,
        remaining_qty_after=2,
        exit_type="PARTIAL_SCALE_OUT",
        allow_partial=True,
        partial_policy_supported=False,
    )

    decision = validate_exit_intent(payload)

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "partial closes require partial_policy_supported=true" in decision.block_reasons[0]


def test_close_qty_greater_than_owned_qty_is_blocked() -> None:
    with pytest.raises(TrackBModelError, match="close_qty must be less than or equal"):
        _intent(owned_qty=1, close_qty=2, remaining_qty_after=0)


def test_close_qty_zero_is_blocked() -> None:
    with pytest.raises(TrackBModelError, match="close_qty must be a positive"):
        _intent(owned_qty=1, close_qty=0, remaining_qty_after=1)


def test_wrong_close_side_is_blocked() -> None:
    with pytest.raises(TrackBModelError, match="long positions require SELL"):
        _intent(position_side="LONG", close_action="BUY")


def test_reverse_flip_is_blocked() -> None:
    with pytest.raises(TrackBModelError, match="reverse/flip exits must be modeled separately"):
        _intent(allow_reverse=True)


def test_remaining_qty_must_match_owned_minus_close() -> None:
    with pytest.raises(TrackBModelError, match="remaining_qty_after must equal"):
        _intent(owned_qty=3, close_qty=1, remaining_qty_after=1, allow_partial=True, partial_policy_supported=True)


def test_idempotency_key_is_deterministic() -> None:
    first = _intent(exit_intent_id="exit-intent-a")
    second = _intent(exit_intent_id="exit-intent-b")

    assert first.idempotency_key == second.idempotency_key
    assert first.idempotency_key == build_exit_intent_idempotency_key(first)
    assert first.idempotency_key.startswith("track_b_exit_intent:")


def test_mismatched_idempotency_key_is_blocked() -> None:
    with pytest.raises(TrackBModelError, match="idempotency_key must match deterministic"):
        _intent(idempotency_key="not-the-deterministic-key")


def test_source_artifact_refs_are_carried_through() -> None:
    source = SourceArtifactRef(
        name="managed_positions",
        path="outputs/track_b_execution_core/managed_positions/latest_managed_positions.json",
        generated_at=NOW,
        authority_layer="Position State",
    )
    intent = _intent(source_artifact_refs=(source,))
    decision = validate_exit_intent(intent)

    assert intent.source_artifact_refs[0].name == "managed_positions"
    assert decision.source_artifact_refs[0].path.endswith("latest_managed_positions.json")


def test_live_money_and_paper_proof_are_explicitly_blocked() -> None:
    for field_name in ("live_money_eligible", "paper_proof_invoked"):
        payload = _intent_payload()
        payload[field_name] = True

        decision = validate_exit_intent(payload)

        assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
        assert field_name in decision.block_reasons[0]


def test_broad_and_global_cancel_semantics_are_blocked() -> None:
    for field_name in ("broad_flatten_allowed", "global_flatten_allowed"):
        payload = _intent_payload()
        payload[field_name] = True

        decision = validate_exit_intent(payload)

        assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
        assert field_name in decision.block_reasons[0]


def test_blocked_authority_decision_requires_block_reasons() -> None:
    with pytest.raises(TrackBModelError, match="blocked authority decisions require block_reasons"):
        ExitAuthorityDecision(
            exit_intent_id="exit-intent-1",
            decision="BLOCKED",
            validated_at=NOW,
        )


def test_allowed_authority_decision_cannot_carry_block_reasons() -> None:
    with pytest.raises(TrackBModelError, match="allowed authority decisions must not carry"):
        ExitAuthorityDecision(
            exit_intent_id="exit-intent-1",
            decision="ALLOWED",
            block_reasons=("stale_authority",),
            validated_at=NOW,
        )


def test_authority_decision_carries_check_sections_and_validator_version() -> None:
    decision = ExitAuthorityDecision(
        exit_intent_id="exit-intent-1",
        decision="DEGRADED_ALLOWED",
        diagnostics={"mode": "degraded_exact_close"},
        hard_required_checks={"bsa_close_authority": True},
        conditional_checks={"control_plane_hard_hold_absent": True},
        diagnostic_checks={"ods_fresh": False},
        source_artifact_refs=(
            {
                "name": "bsa",
                "path": "outputs/operator_dashboard/runtime/latest_broker_session_authority.json",
                "generated_at": NOW,
                "authority_layer": "Exit Authority Validator",
            },
        ),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.DEGRADED_ALLOWED
    assert decision.validator_version == EXIT_AUTHORITY_VALIDATOR_VERSION
    assert decision.diagnostics["mode"] == "degraded_exact_close"
    assert decision.source_artifact_refs[0].name == "bsa"


def test_exit_authority_validator_allows_valid_full_close() -> None:
    decision = validate_exit_authority(intent=_intent(), current_state=_state(), validated_at=NOW)

    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.block_reasons == ()
    assert decision.hard_required_checks["reconciliation_clean"]["category"] == ExitAuthorityCheckCategory.HARD_REQUIRED
    assert decision.hard_required_checks["bsa_close_authority"]["passed"] is True
    assert decision.conditional_checks["partial_close_policy"]["passed"] is True


def test_exit_authority_validator_allows_valid_degraded_exact_close() -> None:
    state = _state(bsa_managed_risk_reducing_close=False, bsa_degraded_exact_close_ready=True)

    decision = ExitAuthorityValidator().validate(intent=_intent(), current_state=state, validated_at=NOW)

    assert decision.decision == ExitAuthorityDecisionValue.DEGRADED_ALLOWED
    assert decision.diagnostics["degraded_exact_close_authority"] is True
    assert decision.hard_required_checks["bsa_close_authority"]["passed"] is True


def test_exit_authority_validator_allows_valid_partial_close_when_declared() -> None:
    intent = _intent(
        owned_qty=3,
        close_qty=1,
        remaining_qty_after=2,
        exit_type="PARTIAL_SCALE_OUT",
        allow_partial=True,
        partial_policy_supported=True,
    )
    state = _state(position_qty=3)

    decision = validate_exit_authority(intent=intent, current_state=state, validated_at=NOW)

    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.conditional_checks["partial_close_policy"]["passed"] is True


def test_exit_authority_validator_blocks_partial_close_when_not_declared() -> None:
    payload = _intent_payload(owned_qty=3, close_qty=1, remaining_qty_after=2, exit_type="PARTIAL_SCALE_OUT")
    state = _state(position_qty=3)

    decision = validate_exit_authority(intent=payload, current_state=state, validated_at=NOW)

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "partial closes require allow_partial=true" in decision.block_reasons[0]


def test_exit_authority_validator_blocks_dirty_reconciliation() -> None:
    decision = validate_exit_authority(intent=_intent(), current_state=_state(reconciliation_clean=False), validated_at=NOW)

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "reconciliation_not_clean" in decision.block_reasons


def test_exit_authority_validator_blocks_unknown_or_open_orders() -> None:
    for override, expected in (
        ({"open_order_count": 1}, "open_orders_present"),
        ({"unknown_open_order_count": 1}, "unknown_open_orders_present"),
    ):
        decision = validate_exit_authority(intent=_intent(), current_state=_state(**override), validated_at=NOW)

        assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
        assert expected in decision.block_reasons


def test_exit_authority_validator_blocks_duplicate_close() -> None:
    decision = validate_exit_authority(
        intent=_intent(),
        current_state=_state(duplicate_close_order_exists=True),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "duplicate_close_exists" in decision.block_reasons


def test_exit_authority_validator_blocks_safe_state_or_guardian() -> None:
    for override, expected in (
        ({"safe_state_allows_managed_close": False}, "safe_state_blocks_managed_close"),
        ({"guardian_allows_exact_close": False}, "guardian_blocks_exact_close"),
    ):
        decision = validate_exit_authority(intent=_intent(), current_state=_state(**override), validated_at=NOW)

        assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
        assert expected in decision.block_reasons


def test_exit_authority_validator_blocks_bsa_close_false() -> None:
    decision = validate_exit_authority(
        intent=_intent(),
        current_state=_state(bsa_managed_risk_reducing_close=False, bsa_degraded_exact_close_ready=False),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "bsa_close_authority_false" in decision.block_reasons


def test_exit_authority_validator_blocks_live_money_or_paper_proof() -> None:
    for override in ({"live_money_eligible": True}, {"paper_proof_invoked": True}):
        decision = validate_exit_authority(intent=_intent(), current_state=_state_payload(**override), validated_at=NOW)

        assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
        assert "must be explicit false" in decision.block_reasons[0]


def test_exit_authority_validator_diagnostics_do_not_block() -> None:
    decision = validate_exit_authority(
        intent=_intent(),
        current_state=_state(diagnostics={"ods_fresh": False, "control_plane_ready_for_entries": False}),
        validated_at=NOW,
    )

    assert decision.decision == ExitAuthorityDecisionValue.ALLOWED
    assert decision.diagnostic_checks["ods_fresh"]["category"] == ExitAuthorityCheckCategory.DIAGNOSTIC
    assert decision.diagnostic_checks["ods_fresh"]["passed"] is False
    assert decision.block_reasons == ()


def test_exit_authority_validator_blocks_position_identity_mismatch() -> None:
    decision = validate_exit_authority(intent=_intent(), current_state=_state(local_symbol="MNQM6"), validated_at=NOW)

    assert decision.decision == ExitAuthorityDecisionValue.BLOCKED
    assert "position_identity_mismatch" in decision.block_reasons


def _intent(**overrides) -> ExitIntent:
    return ExitIntent(**_intent_payload(**overrides))


def _intent_payload(**overrides) -> dict:
    payload = {
        "exit_intent_id": "exit-intent-1",
        "lifecycle_id": "life-1",
        "trade_id": "trade-1",
        "strategy_id": "strategy-1",
        "lane_id": "lane-1",
        "account": "DUM882026",
        "instrument": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "position_side": "LONG",
        "owned_qty": "1",
        "close_action": "SELL",
        "close_qty": "1",
        "remaining_qty_after": "0",
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
        "source_artifact_refs": (
            {
                "name": "reconciliation",
                "path": "outputs/reports/track_b_paper_broker_reconciliation/latest_track_b_paper_broker_reconciliation.json",
                "generated_at": NOW,
                "authority_layer": "Position State",
            },
        ),
        "partial_policy_supported": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
    }
    payload.update(overrides)
    return payload


def _state(**overrides) -> ExitAuthorityCurrentState:
    return ExitAuthorityCurrentState(**_state_payload(**overrides))


def _state_payload(**overrides) -> dict:
    payload = {
        "known_position": True,
        "owned_position": True,
        "position_side": "LONG",
        "position_qty": "1",
        "account": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "open_order_count": 0,
        "unknown_open_order_count": 0,
        "duplicate_close_order_exists": False,
        "reconciliation_clean": True,
        "safe_state_allows_managed_close": True,
        "guardian_allows_exact_close": True,
        "bsa_managed_risk_reducing_close": True,
        "bsa_degraded_exact_close_ready": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
        "diagnostics": {},
        "source_artifact_refs": (
            {
                "name": "bsa",
                "path": "outputs/operator_dashboard/runtime/latest_broker_session_authority.json",
                "generated_at": NOW,
                "authority_layer": "Exit Authority Validator",
            },
        ),
    }
    payload.update(overrides)
    return payload
