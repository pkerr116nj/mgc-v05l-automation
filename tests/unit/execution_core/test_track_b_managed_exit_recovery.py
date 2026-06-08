from __future__ import annotations

import json
from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_managed_exit_recovery import (
    EXIT_DUE_CLOSE_BLOCKED,
    EXIT_DUE_CLOSE_PARTIAL_READY,
    EXIT_DUE_CLOSE_READY,
    NO_EXIT_DUE_POSITIONS,
    TrackBManagedExitRecoveryConfig,
    build_track_b_managed_exit_recovery_plan,
)


NOW = datetime(2026, 6, 5, 4, 55, tzinfo=UTC)


def test_runtime_down_exit_due_exact_candidate_is_ready() -> None:
    payload = _build(_inputs(runtime_down=True))

    assert payload["classification"] == EXIT_DUE_CLOSE_READY
    assert payload["eligible_count"] == 1
    assert payload["apply_eligible_count"] == 1
    assert payload["diagnostic_close_candidate_count"] == 1
    candidate = payload["eligible_positions"][0]["close_candidate"]
    assert candidate["action"] == "SELL"
    assert candidate["quantity"] == "1"
    assert candidate["local_symbol"] == "MNQM6"
    assert payload["submit_attempted"] is False
    assert payload["broker_state_mutated"] is False
    assert payload["paper_proof_invoked"] is False
    assert payload["live_money_eligible"] is False


def test_runtime_healthy_but_maintenance_stale_is_ready() -> None:
    inputs = _inputs(runtime_down=False)
    inputs["managed_orders"]["classification"] = "POSITION_WITHOUT_CLOSE_ORDER"
    inputs["safe_state"]["classification"] = "SAFE_STATE_NORMAL"

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_READY
    assert payload["eligible_positions"][0]["blockers"] == []
    assert payload["eligible_positions"][0]["apply_eligible"] is True
    assert payload["eligible_positions"][0]["diagnostic_close_candidate_ready"] is True


def test_order_status_unreliable_reports_candidate_but_blocks_apply() -> None:
    inputs = _inputs()
    inputs["broker_session_authority"] = _broker_session_authority(
        classification="BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
        connection_mode="ORDER_STATUS_UNRELIABLE",
        close_allowed=False,
    )

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_BLOCKED
    assert payload["diagnostic_close_candidate_count"] == 1
    assert payload["eligible_count"] == 0
    assert payload["apply_eligible_count"] == 0
    assert payload["managed_exit_recovery_plan"]["close_candidates"][0]["local_symbol"] == "MNQM6"
    blocked = payload["blocked_positions"][0]
    assert blocked["diagnostic_close_candidate_ready"] is True
    assert blocked["apply_eligible"] is False
    assert "BROKER_SESSION_CLOSE_AUTHORITY_BLOCKED_ORDER_STATUS_UNRELIABLE" in blocked["apply_blockers"]
    assert "BROKER_SESSION_MANAGED_RISK_REDUCING_CLOSE_NOT_ALLOWED" in blocked["apply_blockers"]


def test_order_status_unreliable_with_exact_degraded_close_authority_is_ready() -> None:
    inputs = _inputs()
    inputs["broker_session_authority"] = _broker_session_authority(
        classification="BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
        connection_mode="ORDER_STATUS_UNRELIABLE",
        close_allowed=True,
    )
    inputs["broker_session_authority"]["degraded_exact_risk_reducing_close_context"] = {
        "ready": True,
        "broker_position_exactly_one": True,
        "broker_open_orders_zero": True,
        "unknown_open_orders_zero": True,
        "broker_lifecycle_reconciled": True,
    }

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_READY
    assert payload["eligible_count"] == 1
    assert payload["apply_eligible_count"] == 1
    assert payload["eligible_positions"][0]["apply_blockers"] == []


def test_position_truth_only_allows_diagnosis_but_blocks_close_apply() -> None:
    inputs = _inputs()
    inputs["broker_session_authority"] = _broker_session_authority(
        classification="BROKER_SESSION_AUTHORITY_POSITION_TRUTH_ONLY",
        connection_mode="POSITION_TRUTH_ONLY",
        close_allowed=False,
    )

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_BLOCKED
    assert payload["diagnostic_close_candidate_count"] == 1
    assert payload["eligible_count"] == 0
    assert payload["blocked_positions"][0]["diagnostic_close_candidate_ready"] is True
    assert "BROKER_SESSION_CLOSE_AUTHORITY_BLOCKED_POSITION_TRUTH_ONLY" in payload["blocked_positions"][0]["apply_blockers"]


def test_missing_broker_session_authority_blocks_apply() -> None:
    inputs = _inputs()
    inputs["broker_session_authority"] = {}

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_BLOCKED
    assert payload["diagnostic_close_candidate_count"] == 1
    assert payload["eligible_count"] == 0
    assert payload["blocked_positions"][0]["diagnostic_blockers"] == []
    assert payload["blocked_positions"][0]["apply_blockers"] == ["BROKER_SESSION_AUTHORITY_MISSING"]


def test_broker_open_order_conflict_blocks() -> None:
    inputs = _inputs()
    inputs["open_order_truth"] = {
        "classification": "OPEN_ORDERS_PRESENT",
        "broker_open_orders": [{"account_id": "DUM882026", "local_symbol": "MNQM6", "con_id": 770561201, "action": "SELL"}],
    }

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_BLOCKED
    assert "BROKER_OPEN_ORDER_CONFLICT" in payload["blocked_positions"][0]["blockers"]
    assert payload["blocked_positions"][0]["diagnostic_close_candidate_ready"] is False


def test_ambiguous_owner_blocks() -> None:
    inputs = _inputs()
    duplicate = dict(inputs["managed_positions"]["managed_positions"][0])
    duplicate["trade_id"] = "trade-other"
    inputs["managed_positions"]["managed_positions"].append(duplicate)

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_BLOCKED
    assert "COMPETING_MANAGED_POSITION_CANDIDATE" in payload["blocked_positions"][0]["blockers"]


def test_missing_registry_lifecycle_identity_blocks() -> None:
    inputs = _inputs()
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"] = []

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_BLOCKED
    assert "REGISTRY_OPEN_MANAGED_RECORD_MISSING" in payload["blocked_positions"][0]["blockers"]


def test_incomplete_registry_review_row_superseded_by_current_scope_lifecycle_for_close() -> None:
    inputs = _inputs(runtime_down=False)
    inputs["reconciliation"]["broker_reconciled"] = True
    inputs["reconciliation"]["current_scope_lifecycle_positions"] = [
        {
            "account_id": "DUM882026",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "trade_id": "trade-mnq",
            "lifecycle_id": "life-mnq",
            "quantity": "1",
        }
    ]
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"] = [
        {
            "account_id": "DUM882026",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "trade_id": "trade-mnq",
            "lifecycle_id": None,
            "current_state": "REVIEW_REQUIRED",
        }
    ]

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_READY
    assert payload["eligible_count"] == 1
    assert payload["eligible_positions"][0]["registry_current_state"] == "OPEN_MANAGED"
    assert payload["eligible_positions"][0]["apply_blockers"] == []


def test_track_b_lifecycle_positions_supersede_incomplete_registry_review_for_close() -> None:
    inputs = _inputs(runtime_down=False)
    inputs["reconciliation"]["broker_reconciled"] = True
    inputs["reconciliation"]["track_b_lifecycle_positions"] = [
        {
            "account_id": "DUM882026",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "trade_id": "trade-mnq",
            "lifecycle_id": "life-mnq",
            "quantity": "1",
        }
    ]
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"] = [
        {
            "account_id": "DUM882026",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "trade_id": "trade-mnq",
            "lifecycle_id": None,
            "current_state": "REVIEW_REQUIRED",
        }
    ]

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_READY
    assert payload["eligible_count"] == 1
    assert payload["eligible_positions"][0]["registry_current_state"] == "OPEN_MANAGED"
    assert payload["eligible_positions"][0]["apply_blockers"] == []


def test_registry_current_scope_identity_conflict_blocks_close() -> None:
    inputs = _inputs(runtime_down=False)
    inputs["reconciliation"]["broker_reconciled"] = True
    inputs["reconciliation"]["current_scope_lifecycle_positions"] = [
        {
            "account_id": "DUM882026",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "trade_id": "trade-mnq",
            "lifecycle_id": "life-mnq",
            "quantity": "1",
        }
    ]
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"] = [
        {
            "account_id": "DUM882026",
            "local_symbol": "MNQM6",
            "con_id": 770561201,
            "trade_id": "trade-other",
            "lifecycle_id": "life-other",
            "current_state": "REVIEW_REQUIRED",
        }
    ]

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_BLOCKED
    assert "REGISTRY_OPEN_MANAGED_RECORD_MISSING" in payload["blocked_positions"][0]["blockers"]
    assert "COMPETING_REGISTRY_CANDIDATE" in payload["blocked_positions"][0]["blockers"]


def test_exact_mnq_mes_simultaneous_long_exits_are_ready() -> None:
    inputs = _inputs()
    inputs["managed_positions"]["managed_positions"].append(_position(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"].append(_registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["guardian"]["managed_close_authority"]["candidates"].append(_candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["safe_state"]["close_authority"]["guardian_close_candidates"].append(_candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_READY
    assert payload["eligible_count"] == 2
    assert payload["diagnostic_close_candidate_count"] == 2
    assert {row["close_candidate"]["local_symbol"] for row in payload["eligible_positions"]} == {"MNQM6", "MESM6"}


def test_order_status_unreliable_with_multiple_exact_degraded_close_positions_is_ready() -> None:
    inputs = _inputs(runtime_down=True)
    inputs["managed_positions"]["managed_positions"].append(_position(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"].append(_registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["guardian"]["managed_close_authority"]["candidates"].append(_candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["safe_state"]["close_authority"]["guardian_close_candidates"].append(_candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["broker_session_authority"] = _broker_session_authority(
        classification="BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
        connection_mode="ORDER_STATUS_UNRELIABLE",
        close_allowed=True,
    )
    inputs["broker_session_authority"]["risk_reducing_close_connection_mode"] = "RISK_REDUCING_CLOSE_CAPABLE_ORDER_STATUS_DEGRADED"
    inputs["broker_session_authority"]["degraded_exact_risk_reducing_close_context"] = {
        "ready": True,
        "broker_positions_present": True,
        "broker_position_exactly_one": False,
        "position_count": 2,
        "broker_open_orders_zero": True,
        "unknown_open_orders_zero": True,
        "broker_lifecycle_reconciled": True,
        "current_scope_lifecycle_positions_match_broker": True,
        "current_scope_lifecycle_position_exact": True,
        "no_lifecycle_open_order": True,
    }

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_READY
    assert payload["apply_eligible_count"] == 2
    assert {row["close_candidate"]["local_symbol"] for row in payload["eligible_positions"]} == {"MNQM6", "MESM6"}
    assert all(row["broker_session_connection_mode"] == "ORDER_STATUS_UNRELIABLE" for row in payload["eligible_positions"])


def test_no_exit_due_positions_is_noop() -> None:
    inputs = _inputs()
    inputs["managed_positions"]["managed_positions"][0]["exit_due"] = False
    inputs["managed_positions"]["managed_positions"][0]["classification"] = "OPEN_MANAGED_MATCHED"

    payload = _build(inputs)

    assert payload["classification"] == NO_EXIT_DUE_POSITIONS
    assert payload["eligible_count"] == 0
    assert payload["apply_eligible_count"] == 0
    assert payload["diagnostic_close_candidate_count"] == 0
    assert payload["blocked_count"] == 0
    assert payload["broker_state_mutated"] is False


def test_stale_persisted_recovery_plan_cannot_authorize_apply(tmp_path: Path) -> None:
    output_path = tmp_path / "latest_managed_exit_recovery_plan.json"
    output_path.write_text(
        json.dumps({"classification": EXIT_DUE_CLOSE_READY, "eligible_count": 2, "eligible_positions": [{"stale": True}]}),
        encoding="utf-8",
    )
    inputs = _inputs()
    inputs["managed_positions"] = {"classification": "NO_MANAGED_POSITIONS", "managed_positions": []}
    config = TrackBManagedExitRecoveryConfig(repo_root=tmp_path, output_path=output_path)

    payload = build_track_b_managed_exit_recovery_plan(config=config, now=NOW, input_overrides=inputs)

    assert payload["classification"] == NO_EXIT_DUE_POSITIONS
    assert payload["eligible_count"] == 0
    assert payload["apply_eligible_count"] == 0
    assert payload["diagnostic_close_candidate_count"] == 0
    assert payload["eligible_positions"] == []


def test_partial_ready_when_one_position_blocks() -> None:
    inputs = _inputs()
    blocked = _position(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    blocked["projection_authority_owner_confirmed"] = False
    inputs["managed_positions"]["managed_positions"].append(blocked)
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"].append(_registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["guardian"]["managed_close_authority"]["candidates"].append(_candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["safe_state"]["close_authority"]["guardian_close_candidates"].append(_candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_PARTIAL_READY
    assert payload["eligible_count"] == 1
    assert payload["blocked_count"] == 1
    assert "OWNER_PROJECTION_NOT_CONFIRMED" in payload["blocked_positions"][0]["blockers"]
    assert payload["blocked_positions"][0]["diagnostic_close_candidate_ready"] is False


def _build(inputs: dict):
    return build_track_b_managed_exit_recovery_plan(
        config=TrackBManagedExitRecoveryConfig(),
        now=NOW,
        input_overrides=inputs,
    )


def _inputs(*, runtime_down: bool = True) -> dict:
    position = _position()
    candidate = _candidate()
    return {
        "managed_positions": {"classification": "OPEN_MANAGED_EXIT_DUE", "managed_positions": [position]},
        "managed_orders": {
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [
                {
                    "classification": "POSITION_WITHOUT_CLOSE_ORDER",
                    "close_order_required_now": True,
                    "trade_id": "trade-mnq",
                    "lifecycle_id": "life-mnq",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                }
            ],
        },
        "open_order_truth": {"classification": "NO_OPEN_ORDERS", "broker_open_orders": []},
        "guardian": {
            "classification": "BROKER_POSITION_GUARDIAN_READY",
            "managed_close_authority": {"allowed": True, "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING", "candidates": [candidate]},
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
        },
        "safe_state": {
            "classification": "SAFE_STATE_NORMAL",
            "close_authority": {"allowed": True, "classification": "MANAGED_CLOSE_MUTATION_ALLOWED", "guardian_close_candidates": [candidate]},
            "paper_proof_invoked": False,
            "live_money_eligible": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
        },
        "reconciliation": {
            "classification": "BROKER_TRUTH_SETTLEMENT_TIMEOUT" if runtime_down else "TRACK_B_PAPER_BROKER_RECONCILED",
            "registry_reconciliation": {
                "classification": "REGISTRY_RECONCILIATION_MATCHED",
                "blocking": False,
                "mapped_records": [_registry_record()],
                "review_required_trade_ids": [],
            },
        },
        "broker_truth_lease": {"schema_version": "track_b_broker_truth_lease_v1", "lease_state": "ACTIVE"},
        "broker_session_authority": _broker_session_authority(),
    }


def _broker_session_authority(
    *,
    classification: str = "BROKER_SESSION_AUTHORITY_SUBMIT_CAPABLE",
    connection_mode: str = "SUBMIT_CAPABLE",
    close_allowed: bool = True,
) -> dict:
    return {
        "schema_version": "track_b_broker_session_authority_v1",
        "classification": classification,
        "connection_mode": connection_mode,
        "lease_state": "ACTIVE",
        "allowed_uses": {
            "new_entry": False,
            "managed_risk_reducing_close": close_allowed,
            "broker_observed_adoption_diagnosis": True,
            "fill_callback_adoption": False,
            "status_diagnostic": True,
        },
        "authority_blockers": []
        if close_allowed
        else [{"code": "session_not_close_capable", "detail": "Synthetic blocked authority."}],
        "callback_ownership_attribution": {"classification": "CALLBACK_OWNERSHIP_ALIGNED"},
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def _position(
    *,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    lifecycle_id: str = "life-mnq",
    trade_id: str = "trade-mnq",
) -> dict:
    return {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "exit_due": True,
        "account_id": "DUM882026",
        "symbol": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "strategy_id": f"{symbol.lower()}_globex_active_participation_long",
        "lane_id": f"{symbol.lower()}_globex_active_participation_long",
        "projection_authority_owner_confirmed": True,
        "projection_authority_source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
        "working_close_qty": "0",
        "broker_position": {
            "account_id": "DUM882026",
            "symbol": symbol,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "quantity": "1.0",
        },
        "lifecycle_position": {
            "account_id": "DUM882026",
            "symbol": symbol,
            "local_symbol": local_symbol,
            "con_id": con_id,
            "quantity": "1",
            "side": "LONG",
            "lifecycle_id": lifecycle_id,
            "trade_id": trade_id,
            "source": "CURRENT_EXPOSURE_OWNER_RESOLVER",
        },
    }


def _registry_record(
    *,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    lifecycle_id: str = "life-mnq",
    trade_id: str = "trade-mnq",
) -> dict:
    return {
        "current_state": "OPEN_MANAGED",
        "account_id": "DUM882026",
        "symbol": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": "1",
        "side": "LONG",
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
    }


def _candidate(
    *,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    lifecycle_id: str = "life-mnq",
    trade_id: str = "trade-mnq",
) -> dict:
    return {
        "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
        "account_id": "DUM882026",
        "symbol": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "quantity": "1",
        "action": "SELL",
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
    }
