from __future__ import annotations

from pathlib import Path

from mgc_v05l.execution_core.track_b_paper_lifecycle_validation_entry_authority import (
    PAPER_LIFECYCLE_VALIDATION_ENTRY_ALLOWED,
    PAPER_LIFECYCLE_VALIDATION_ENTRY_BLOCKED,
    PAPER_LIFECYCLE_VALIDATION_ENTRY_DEGRADED_ALLOWED,
    PaperLifecycleValidationEntryAuthorityInput,
    evaluate_paper_lifecycle_validation_entry_authority,
)


def _request(tmp_path: Path, **overrides: object) -> PaperLifecycleValidationEntryAuthorityInput:
    payload = {
        "repo_root": tmp_path,
        "execution_domain": "TRACK_B_PAPER",
        "mode": "PAPER",
        "account_id": "DUM882026",
        "symbol": "MES",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "action": "BUY",
        "intent_type": "BUY_TO_OPEN",
        "quantity": 1.0,
        "paper_only": True,
        "caller_path": "track_b_paper_leak_test_apply",
        "caller_metadata": {
            "lane_id": "mes_globex_active_participation_long",
            "strategy_id": "PAPER_ACTIVE_EVIDENCE_MES_GLOBEX_PARTICIPATION_LONG_V1",
            "trade_id": "trade-validation-1",
            "local_symbol": "MESM6",
        },
        "authorization_check": {"passed": True},
        "authorization_artifact": {
            "artifact_type": "TRACK_B_PAPER_LEAK_TEST_AUTHORIZATION",
            "mode": "PAPER",
            "account_id": "DUM882026",
            "symbol": "MES",
            "local_symbol": "MESM6",
            "qty": 1,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "reconciliation": {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "review_required_count": 0,
            "track_b_broker_open_order_count": 0,
            "track_b_broker_position_count": 0,
            "track_b_broker_positions": [],
            "blockers": [],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "open_order_truth": {
            "classification": "NO_OPEN_ORDERS",
            "open_order_count": 0,
            "unknown_open_order_count": 0,
        },
        "broker_truth_status": {
            "classification": "BROKER_TRUTH_REFRESH_READY",
            "positions_complete": True,
            "open_orders_complete": True,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "safe_state": {
            "classification": "SAFE_STATE_NORMAL",
            "broker_mutation_allowed": True,
            "submit_allowed": True,
            "entry_mutation_allowed": True,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    }
    payload.update(overrides)
    return PaperLifecycleValidationEntryAuthorityInput(**payload)


def test_allows_clean_controlled_paper_lifecycle_validation_entry(tmp_path: Path) -> None:
    decision = evaluate_paper_lifecycle_validation_entry_authority(_request(tmp_path))

    assert decision["classification"] == PAPER_LIFECYCLE_VALIDATION_ENTRY_ALLOWED
    assert decision["allowed"] is True
    assert decision["block_reasons"] == []
    assert decision["not_runtime_start_authority"] is True
    assert decision["not_autonomous_strategy_entry_authority"] is True


def test_blocks_live_money(tmp_path: Path) -> None:
    request = _request(
        tmp_path,
        authorization_artifact={
            "artifact_type": "TRACK_B_PAPER_LEAK_TEST_AUTHORIZATION",
            "mode": "PAPER",
            "account_id": "DUM882026",
            "symbol": "MES",
            "local_symbol": "MESM6",
            "qty": 1,
            "live_money_eligible": True,
            "paper_proof_invoked": False,
        },
    )

    decision = evaluate_paper_lifecycle_validation_entry_authority(request)

    assert decision["classification"] == PAPER_LIFECYCLE_VALIDATION_ENTRY_BLOCKED
    assert "live_money_false" in decision["block_reasons"]


def test_blocks_dirty_reconciliation(tmp_path: Path) -> None:
    request = _request(
        tmp_path,
        reconciliation={
            "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT",
            "broker_reconciled": False,
            "review_required_count": 1,
            "blockers": ["position_mismatch"],
        },
    )

    decision = evaluate_paper_lifecycle_validation_entry_authority(request)

    assert decision["classification"] == PAPER_LIFECYCLE_VALIDATION_ENTRY_BLOCKED
    assert "reconciliation_clean" in decision["block_reasons"]


def test_blocks_unknown_or_conflicting_orders(tmp_path: Path) -> None:
    request = _request(
        tmp_path,
        open_order_truth={
            "classification": "UNKNOWN_OPEN_ORDERS",
            "open_order_count": 1,
            "unknown_open_order_count": 1,
        },
    )

    decision = evaluate_paper_lifecycle_validation_entry_authority(request)

    assert decision["classification"] == PAPER_LIFECYCLE_VALIDATION_ENTRY_BLOCKED
    assert "no_unknown_open_orders" in decision["block_reasons"]
    assert "no_conflicting_open_orders" in decision["block_reasons"]


def test_degraded_allow_when_only_diagnostic_safe_state_entry_flag_is_false(tmp_path: Path) -> None:
    request = _request(
        tmp_path,
        safe_state={
            "classification": "SAFE_STATE_NORMAL",
            "broker_mutation_allowed": True,
            "submit_allowed": True,
            "entry_mutation_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )

    decision = evaluate_paper_lifecycle_validation_entry_authority(request)

    assert decision["classification"] == PAPER_LIFECYCLE_VALIDATION_ENTRY_DEGRADED_ALLOWED
    assert decision["allowed"] is True
    assert "safe_state_entry_and_broker_mutation" in decision["degraded_reasons"]


def test_blocks_disallowed_instrument_or_size(tmp_path: Path) -> None:
    request = _request(tmp_path, symbol="CL", local_symbol="CLM6", quantity=2.0)

    decision = evaluate_paper_lifecycle_validation_entry_authority(request)

    assert decision["classification"] == PAPER_LIFECYCLE_VALIDATION_ENTRY_BLOCKED
    assert "controlled_instrument_allowlisted" in decision["block_reasons"]
    assert "validation_qty_within_limit" in decision["block_reasons"]
