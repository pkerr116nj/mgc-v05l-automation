from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

from mgc_v05l.execution_core.track_b_managed_exit_actuator import (
    MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
    MANAGED_EXIT_ACTUATOR_BLOCKED,
    MANAGED_EXIT_ACTUATOR_DRY_RUN_READY,
    TrackBManagedExitActuatorConfig,
    run_track_b_managed_exit_actuator,
)


NOW = datetime(2026, 6, 8, 15, 5, tzinfo=UTC)


def test_runtime_down_exact_exit_due_position_applies_via_guarded_attach(tmp_path: Path) -> None:
    calls = []

    def _attach(config, now):
        calls.append((config, now))
        return {
            "classification": "TRACK_B_STRATEGY_PAPER_CLOSE_SUBMITTED",
            "submit_attempted": True,
            "broker_state_mutated": True,
            "apply_result": {
                "close_submit_attempt": {"broker_order_id": "91", "perm_id": 123456},
            },
        }

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=_inputs(runtime_down=True),
        attach_runner=_attach,
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    assert payload["entry_allowed"] is False
    assert payload["submit_attempted"] is True
    assert payload["submitted_count"] == 1
    assert payload["attempted_closes"][0]["order_id"] == "91"
    assert calls[0][0].apply is True
    assert calls[0][0].operator_authorized_managed_exit is True
    assert calls[0][0].lifecycle_id == "life-mnq"
    assert calls[0][0].local_symbol == "MNQM6"


def test_dry_run_ready_does_not_call_attach(tmp_path: Path) -> None:
    calls = []

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path),
        now=NOW,
        input_overrides=_inputs(runtime_down=True),
        attach_runner=lambda config, now: calls.append(config) or {},
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_DRY_RUN_READY
    assert payload["eligible_count"] == 1
    assert payload["submit_attempted"] is False
    assert calls == []


def test_multiple_positions_are_processed_one_at_a_time_with_refresh_between(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["managed_positions"]["managed_positions"].append(
        _position(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    inputs["managed_orders"]["managed_orders"].append(
        _managed_order(local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"].append(
        _registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    inputs["guardian"]["managed_close_authority"]["candidates"].append(
        _candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    inputs["safe_state"]["close_authority"]["guardian_close_candidates"].append(
        _candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes")
    )
    attach_symbols = []
    refreshes = []

    def _attach(config, now):
        attach_symbols.append(config.local_symbol)
        return {"classification": "TRACK_B_STRATEGY_PAPER_CLOSE_SUBMITTED", "submit_attempted": True, "broker_state_mutated": True}

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=_attach,
        refresh_hook=lambda row: refreshes.append(row["identity"]["local_symbol"]),
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING
    assert attach_symbols == ["MNQM6", "MESM6"]
    assert refreshes == ["MNQM6", "MESM6"]
    assert payload["submitted_count"] == 2


def test_duplicate_close_blocks_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["managed_orders"]["classification"] = "WORKING_CLOSE_ORDER"
    inputs["managed_orders"]["managed_orders"][0]["working"] = True
    calls = []

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: calls.append(config) or {},
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["eligible_count"] == 0
    assert "BROKER_OPEN_ORDER_CONFLICT" in payload["blocked_positions"][0]["blockers"]
    assert calls == []


def test_open_orders_block_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["open_order_truth"]["classification"] = "OPEN_ORDERS_PRESENT"
    inputs["open_order_truth"]["broker_open_orders"] = [{"account_id": "DUM882026", "local_symbol": "MNQM6"}]

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: {"submit_attempted": True},
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["submit_attempted"] is False
    assert "BROKER_OPEN_ORDER_CONFLICT" in payload["blocked_positions"][0]["blockers"]


def test_unknown_open_orders_block_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["reconciliation"]["unknown_broker_open_order_count"] = 1

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: {"submit_attempted": True},
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["submit_attempted"] is False
    assert "RECONCILIATION_UNKNOWN_OPEN_ORDERS_PRESENT" in payload["blocked_positions"][0]["blockers"]


def test_dirty_reconciliation_blocks_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["reconciliation"]["classification"] = "BROKER_TRUTH_SETTLEMENT_TIMEOUT"
    inputs["reconciliation"]["broker_reconciled"] = False

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: {"submit_attempted": True},
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["submit_attempted"] is False
    assert "BROKER_LIFECYCLE_RECONCILIATION_NOT_CLEAN" in payload["blocked_positions"][0]["blockers"]


def test_stale_due_projection_blocks_apply_but_not_due_detection(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["managed_positions"]["source_freshness"] = {
        "stale": True,
        "stale_sources": ["position_truth"],
    }
    inputs["managed_positions"]["managed_positions"][0]["freshness_state"] = "STALE_DEPENDENCY"
    inputs["managed_positions"]["managed_positions"][0]["exit_due_evidence_stale"] = True
    inputs["managed_positions"]["managed_positions"][0]["apply_authority_degraded"] = True

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: {"submit_attempted": True},
        write=False,
    )

    blockers = payload["blocked_positions"][0]["blockers"]
    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["exit_due_count"] == 1
    assert payload["submit_attempted"] is False
    assert "MANAGED_POSITION_APPLY_AUTHORITY_DEGRADED" in blockers
    assert "MANAGED_POSITION_SOURCE_STALE" in blockers


def test_ambiguous_ownership_blocks_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    duplicate = _position(lifecycle_id="life-other", trade_id="trade-other")
    inputs["managed_positions"]["managed_positions"].append(duplicate)
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"].append(
        _registry_record(lifecycle_id="life-other", trade_id="trade-other")
    )

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: {"submit_attempted": True},
        write=False,
    )

    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert payload["submit_attempted"] is False
    assert "COMPETING_MANAGED_POSITION_CANDIDATE" in payload["blocked_positions"][0]["blockers"]


def test_guardian_safe_state_and_bsa_block_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["guardian"]["managed_close_authority"]["allowed"] = False
    inputs["safe_state"]["classification"] = "SAFE_STATE_HARD_HOLD"
    inputs["safe_state"]["close_authority"]["allowed"] = False
    inputs["broker_session_authority"]["allowed_uses"]["managed_risk_reducing_close"] = False

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: {"submit_attempted": True},
        write=False,
    )

    blockers = payload["blocked_positions"][0]["blockers"]
    assert payload["classification"] == MANAGED_EXIT_ACTUATOR_BLOCKED
    assert "GUARDIAN_CLOSE_AUTHORITY_NOT_ALLOWED" in blockers
    assert "SAFE_STATE_CLOSE_NOT_ALLOWED" in blockers
    assert "BROKER_SESSION_MANAGED_RISK_REDUCING_CLOSE_NOT_ALLOWED" in blockers
    assert payload["submit_attempted"] is False


def test_live_money_or_paper_proof_blocks_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["safe_state"]["live_money_eligible"] = True
    inputs["broker_session_authority"]["paper_proof_invoked"] = True

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: {"submit_attempted": True},
        write=False,
    )

    blockers = payload["blocked_positions"][0]["blockers"]
    assert "LIVE_MONEY_ELIGIBLE_TRUE" in blockers
    assert "PAPER_PROOF_INVOKED_TRUE" in blockers
    assert payload["submit_attempted"] is False


def test_close_candidate_mismatch_blocks_before_attach(tmp_path: Path) -> None:
    inputs = _inputs(runtime_down=True)
    inputs["guardian"]["managed_close_authority"]["candidates"][0]["action"] = "BUY"

    payload = run_track_b_managed_exit_actuator(
        config=TrackBManagedExitActuatorConfig(repo_root=tmp_path, apply=True, operator_authorized_managed_exit=True),
        now=NOW,
        input_overrides=inputs,
        attach_runner=lambda config, now: {"submit_attempted": True},
        write=False,
    )

    assert "GUARDIAN_EXACT_CLOSE_CANDIDATE_MISSING" in payload["blocked_positions"][0]["blockers"]
    assert payload["submit_attempted"] is False


def _inputs(*, runtime_down: bool) -> dict:
    candidate = _candidate()
    return {
        "managed_positions": {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [_position()],
        },
        "managed_orders": {
            "generated_at": NOW.isoformat(),
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [_managed_order()],
        },
        "open_order_truth": {"classification": "NO_OPEN_ORDERS", "broker_open_orders": [], "unknown_open_orders": []},
        "guardian": {
            "classification": "BROKER_POSITION_GUARDIAN_READY",
            "managed_close_authority": {
                "allowed": True,
                "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
                "candidates": [candidate],
            },
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
        },
        "safe_state": {
            "classification": "SAFE_STATE_NORMAL",
            "close_authority": {
                "allowed": True,
                "classification": "MANAGED_CLOSE_MUTATION_ALLOWED",
                "guardian_close_candidates": [candidate],
            },
            "paper_proof_invoked": False,
            "live_money_eligible": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
        },
        "reconciliation": {
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "registry_reconciliation": {
                "classification": "REGISTRY_RECONCILIATION_MATCHED",
                "blocking": False,
                "mapped_records": [_registry_record()],
                "review_required_trade_ids": [],
            },
        },
        "broker_truth_lease": {"schema_version": "track_b_broker_truth_lease_v1", "lease_state": "ACTIVE"},
        "broker_session_authority": {
            "schema_version": "track_b_broker_session_authority_v1",
            "classification": "BROKER_SESSION_AUTHORITY_ORDER_STATUS_UNRELIABLE",
            "connection_mode": "ORDER_STATUS_UNRELIABLE",
            "allowed_uses": {"new_entry": False, "managed_risk_reducing_close": True},
            "degraded_exact_risk_reducing_close_context": {"ready": True},
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
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
            "expiry": "20260618",
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


def _managed_order(
    *,
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
    lifecycle_id: str = "life-mnq",
    trade_id: str = "trade-mnq",
) -> dict:
    return {
        "classification": "POSITION_WITHOUT_CLOSE_ORDER",
        "close_order_required_now": True,
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "working": False,
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
