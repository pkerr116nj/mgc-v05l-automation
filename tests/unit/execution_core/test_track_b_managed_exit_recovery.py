from __future__ import annotations

from datetime import UTC, datetime

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


def test_broker_open_order_conflict_blocks() -> None:
    inputs = _inputs()
    inputs["open_order_truth"] = {
        "classification": "OPEN_ORDERS_PRESENT",
        "broker_open_orders": [{"account_id": "DUM882026", "local_symbol": "MNQM6", "con_id": 770561201, "action": "SELL"}],
    }

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_BLOCKED
    assert "BROKER_OPEN_ORDER_CONFLICT" in payload["blocked_positions"][0]["blockers"]


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


def test_exact_mnq_mes_simultaneous_long_exits_are_ready() -> None:
    inputs = _inputs()
    inputs["managed_positions"]["managed_positions"].append(_position(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["reconciliation"]["registry_reconciliation"]["mapped_records"].append(_registry_record(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["guardian"]["managed_close_authority"]["candidates"].append(_candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))
    inputs["safe_state"]["close_authority"]["guardian_close_candidates"].append(_candidate(symbol="MES", local_symbol="MESM6", con_id=770561194, lifecycle_id="life-mes", trade_id="trade-mes"))

    payload = _build(inputs)

    assert payload["classification"] == EXIT_DUE_CLOSE_READY
    assert payload["eligible_count"] == 2
    assert {row["close_candidate"]["local_symbol"] for row in payload["eligible_positions"]} == {"MNQM6", "MESM6"}


def test_no_exit_due_positions_is_noop() -> None:
    inputs = _inputs()
    inputs["managed_positions"]["managed_positions"][0]["exit_due"] = False
    inputs["managed_positions"]["managed_positions"][0]["classification"] = "OPEN_MANAGED_MATCHED"

    payload = _build(inputs)

    assert payload["classification"] == NO_EXIT_DUE_POSITIONS
    assert payload["eligible_count"] == 0
    assert payload["blocked_count"] == 0
    assert payload["broker_state_mutated"] is False


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
