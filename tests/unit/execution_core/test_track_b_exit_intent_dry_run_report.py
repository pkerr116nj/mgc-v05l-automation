from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import pytest

from mgc_v05l.execution_core.track_b_exit_intent_dry_run_report import (
    EXIT_INTENT_DRY_RUN_BLOCKED,
    EXIT_INTENT_DRY_RUN_DEGRADED,
    EXIT_INTENT_DRY_RUN_READY,
    NO_BROKER_POSITIONS,
    TrackBExitIntentDryRunReportConfig,
    build_track_b_exit_intent_dry_run_report,
    run_track_b_exit_intent_dry_run_report,
)


NOW = datetime(2026, 6, 9, 12, 0, tzinfo=UTC)


def test_attributed_broker_position_builds_allowed_exit_intent(tmp_path: Path) -> None:
    payload = _build(tmp_path, _inputs())

    assert payload["classification"] == EXIT_INTENT_DRY_RUN_READY
    assert payload["candidate_count"] == 1
    candidate = payload["candidate_exit_intents"][0]
    assert candidate["candidate_close_action"] == "BUY"
    assert candidate["candidate_close_qty"] == "1"
    assert candidate["attribution_status"] == "ATTRIBUTED"
    assert candidate["authority_decision"]["decision"] == "ALLOWED"
    assert candidate["exit_intent"]["execution_domain"] == "TRACK_B_PAPER"
    assert candidate["exit_intent"]["price_policy"]["type"] == "PLACEHOLDER_GUARDED_LIMIT"
    assert payload["submit_attempted"] is False
    assert payload["cancel_attempted"] is False
    assert payload["close_attempted"] is False
    assert payload["service_started"] is False
    assert payload["runtime_restarted"] is False


def test_unattributed_broker_scoped_risk_exit_is_degraded_allowed(tmp_path: Path) -> None:
    inputs = _inputs()
    inputs["managed_positions"]["managed_positions"] = []
    inputs["managed_orders"]["managed_orders"] = []

    payload = _build(tmp_path, inputs)

    assert payload["classification"] == EXIT_INTENT_DRY_RUN_DEGRADED
    candidate = payload["candidate_exit_intents"][0]
    assert candidate["attribution_status"] == "UNATTRIBUTED"
    assert candidate["authority_decision"]["decision"] == "DEGRADED_ALLOWED"
    assert candidate["degraded_reasons"] == ["attribution_incomplete"]
    assert candidate["block_reasons"] == []


def test_same_contract_working_close_blocks_over_close_risk(tmp_path: Path) -> None:
    inputs = _inputs()
    inputs["managed_orders"]["managed_orders"][0]["working"] = True
    inputs["managed_orders"]["managed_orders"][0]["remaining_quantity"] = "1"

    payload = _build(tmp_path, inputs)

    assert payload["classification"] == EXIT_INTENT_DRY_RUN_BLOCKED
    candidate = payload["candidate_exit_intents"][0]
    assert candidate["authority_decision"]["decision"] == "BLOCKED"
    assert "same_contract_working_close_over_close_risk" in candidate["block_reasons"]


def test_unrelated_unknown_order_is_diagnostic_only(tmp_path: Path) -> None:
    inputs = _inputs()
    inputs["reconciliation"]["unknown_broker_open_order_count"] = 1
    inputs["reconciliation"]["unknown_broker_open_orders"] = [{"local_symbol": "ESM6", "con_id": 123, "quantity": "1"}]
    inputs["open_order_truth"]["unknown_open_orders"] = [{"local_symbol": "ESM6", "con_id": 123, "quantity": "1"}]

    payload = _build(tmp_path, inputs)

    candidate = payload["candidate_exit_intents"][0]
    assert candidate["authority_decision"]["decision"] == "ALLOWED"
    assert candidate["authority_decision"]["diagnostic_checks"]["unrelated_unknown_orders"]["passed"] is False


def test_same_contract_unknown_order_blocks_when_over_close_not_ruled_out(tmp_path: Path) -> None:
    inputs = _inputs()
    unknown = {"account_id": "DUM882026", "local_symbol": "MESM6", "con_id": 770561194, "quantity": "1"}
    inputs["reconciliation"]["unknown_broker_open_order_count"] = 1
    inputs["reconciliation"]["unknown_broker_open_orders"] = [unknown]
    inputs["open_order_truth"]["unknown_open_orders"] = [unknown]

    payload = _build(tmp_path, inputs)

    candidate = payload["candidate_exit_intents"][0]
    assert candidate["authority_decision"]["decision"] == "BLOCKED"
    assert "same_contract_unknown_order_over_close_risk" in candidate["block_reasons"]


def test_safe_state_hard_halt_is_diagnostic_for_exact_paper_risk_reducing_close(tmp_path: Path) -> None:
    inputs = _inputs()
    inputs["safe_state"]["classification"] = "SAFE_STATE_HARD_HOLD"

    payload = _build(tmp_path, inputs)

    candidate = payload["candidate_exit_intents"][0]
    assert candidate["authority_decision"]["decision"] == "ALLOWED"
    assert candidate["authority_decision"]["hard_required_checks"]["safe_state_no_hard_halt"]["passed"] is True
    assert candidate["block_reasons"] == []


def test_no_broker_positions_reports_no_candidates(tmp_path: Path) -> None:
    inputs = _inputs()
    inputs["reconciliation"]["track_b_broker_positions"] = []
    inputs["reconciliation"]["track_b_broker_position_count"] = 0
    inputs["managed_positions"]["managed_positions"] = []

    payload = _build(tmp_path, inputs)

    assert payload["classification"] == NO_BROKER_POSITIONS
    assert payload["candidate_count"] == 0
    assert payload["candidate_exit_intents"] == []


def test_current_managed_positions_outrank_stale_reconciliation_positions(tmp_path: Path) -> None:
    inputs = _inputs()
    inputs["reconciliation"]["classification"] = "TRACK_B_PAPER_BROKER_RECONCILIATION_BLOCKED"
    inputs["reconciliation"]["broker_reconciled"] = False
    inputs["reconciliation"]["track_b_broker_positions"] = [
        {
            "account_id": "DUM882026",
            "local_symbol": "MGCQ6",
            "con_id": 732156883,
            "quantity": "-1",
            "symbol": "MGC",
            "track_b_root": "MGC",
        }
    ]
    inputs["managed_positions"]["managed_positions"] = [
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "account_id": "DUM882026",
            "local_symbol": "ESU6",
            "con_id": 649180671,
            "quantity": "1",
            "side": "LONG",
            "broker_position": {
                "account_id": "DUM882026",
                "local_symbol": "ESU6",
                "con_id": 649180671,
                "quantity": "1.0",
                "symbol": "ES",
                "track_b_root": "ES",
            },
            "projection_authority_owner_confirmed": True,
            "lifecycle_id": "life-es",
            "trade_id": "trade-es",
            "strategy_id": "es_strategy",
            "lane_id": "es_lane",
            "exit_due": True,
        }
    ]
    inputs["managed_orders"]["managed_orders"] = [
        {
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "account_id": "DUM882026",
            "local_symbol": "ESU6",
            "con_id": 649180671,
            "quantity": "1",
            "lifecycle_id": "life-es",
            "trade_id": "trade-es",
            "strategy_id": "es_strategy",
            "lane_id": "es_lane",
            "required_close_action": "SELL",
            "required_close_quantity": "1",
            "working": False,
        }
    ]

    payload = _build(tmp_path, inputs)

    assert payload["classification"] == EXIT_INTENT_DRY_RUN_READY
    assert payload["candidate_count"] == 1
    candidate = payload["candidate_exit_intents"][0]
    assert candidate["local_symbol"] == "ESU6"
    assert candidate["candidate_close_action"] == "SELL"
    assert candidate["exit_due"] is True


def test_complete_fresh_broker_snapshot_blocks_stale_managed_close_candidate(tmp_path: Path) -> None:
    inputs = _inputs()
    inputs["ibkr_positions_snapshot"] = {
        "generated_at": NOW.isoformat(),
        "ok": True,
        "positions_complete": True,
        "selected_account_id": "DUM882026",
        "positions": [
            {
                "account_id": "DUM882026",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "quantity": "0",
                "security_type": "FUT",
                "symbol": "MES",
                "track_b_root": "MES",
            }
        ],
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }

    payload = _build(tmp_path, inputs)

    assert payload["classification"] == NO_BROKER_POSITIONS
    assert payload["candidate_count"] == 0
    assert payload["candidate_exit_intents"] == []


@pytest.mark.parametrize(
    ("symbol", "local_symbol", "expiry", "con_id", "quantity", "side", "close_action"),
    (
        ("MGC", "MGCQ6", "20260827", 732156883, "-1", "SHORT", "BUY"),
        ("GC", "GCQ6", "20260827", 732156872, "-1", "SHORT", "BUY"),
        ("ES", "ESU6", "20260918", 649180671, "-1", "SHORT", "BUY"),
        ("NQ", "NQU6", "20260918", 770561204, "-1", "SHORT", "BUY"),
        ("MES", "MESU6", "20260918", 793356217, "-1", "SHORT", "BUY"),
        ("MNQ", "MNQU6", "20260918", 793356225, "1", "LONG", "SELL"),
    ),
)
def test_fresh_broker_snapshot_local_symbol_expiry_without_con_id_normalizes_through_shared_identity(
    tmp_path: Path,
    symbol: str,
    local_symbol: str,
    expiry: str,
    con_id: int,
    quantity: str,
    side: str,
    close_action: str,
) -> None:
    inputs = _inputs()
    lifecycle_id = f"life-{symbol.lower()}"
    trade_id = f"trade-{symbol.lower()}"
    inputs["ibkr_positions_snapshot"] = {
        "generated_at": NOW.isoformat(),
        "ok": True,
        "positions_complete": True,
        "selected_account_id": "DUM882026",
        "positions": [
            {
                "account_id": "DUM882026",
                "local_symbol": local_symbol,
                "expiry": expiry,
                "quantity": quantity,
                "security_type": "FUT",
                "symbol": symbol,
            }
        ],
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    inputs["managed_positions"]["managed_positions"] = [
        {
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "account_id": "DUM882026",
            "local_symbol": local_symbol,
            "con_id": con_id,
            "quantity": "1",
            "side": side,
            "lifecycle_id": lifecycle_id,
            "trade_id": trade_id,
            "strategy_id": f"{symbol.lower()}_strategy",
            "lane_id": f"{symbol.lower()}_lane",
            "exit_due": True,
            "projection_authority_owner_confirmed": True,
        }
    ]
    inputs["managed_orders"]["managed_orders"] = [
        {
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "account_id": "DUM882026",
            "local_symbol": local_symbol,
            "con_id": con_id,
            "quantity": "1",
            "lifecycle_id": lifecycle_id,
            "trade_id": trade_id,
            "strategy_id": f"{symbol.lower()}_strategy",
            "lane_id": f"{symbol.lower()}_lane",
            "required_close_action": close_action,
            "required_close_quantity": "1",
            "working": False,
        }
    ]

    payload = _build(tmp_path, inputs)

    assert payload["classification"] == EXIT_INTENT_DRY_RUN_READY
    assert payload["candidate_count"] == 1
    candidate = payload["candidate_exit_intents"][0]
    assert candidate["local_symbol"] == local_symbol
    assert candidate["con_id"] == con_id
    assert candidate["candidate_close_action"] == close_action
    assert candidate["authority_decision"]["decision"] == "ALLOWED"


def test_fresh_broker_snapshot_unresolved_identity_blocks_instead_of_disappearing(tmp_path: Path) -> None:
    inputs = _inputs()
    inputs["ibkr_positions_snapshot"] = {
        "generated_at": NOW.isoformat(),
        "ok": True,
        "positions_complete": True,
        "selected_account_id": "DUM882026",
        "positions": [
            {
                "account_id": "DUM882026",
                "local_symbol": "ZTU6",
                "expiry": "20260930",
                "quantity": "-1",
                "security_type": "FUT",
                "symbol": "ZT",
            }
        ],
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    inputs["managed_positions"]["managed_positions"] = []
    inputs["managed_orders"]["managed_orders"] = []

    payload = _build(tmp_path, inputs)

    assert payload["classification"] == EXIT_INTENT_DRY_RUN_BLOCKED
    assert payload["candidate_count"] == 1
    candidate = payload["candidate_exit_intents"][0]
    assert candidate["authority_decision"]["decision"] == "BLOCKED"
    assert candidate["block_reasons"] == ["contract_identity_not_in_validated_registry"]


def test_run_can_write_report_without_broker_or_service_side_effects(tmp_path: Path) -> None:
    config = TrackBExitIntentDryRunReportConfig(repo_root=tmp_path, output_path=Path("report.json"))

    payload = run_track_b_exit_intent_dry_run_report(config=config, now=NOW, write=True)

    assert payload["classification"] == NO_BROKER_POSITIONS
    written = tmp_path / "report.json"
    assert written.exists()
    assert payload["broker_state_mutated"] is False
    assert payload["service_started"] is False


def _build(tmp_path: Path, inputs: dict) -> dict:
    return build_track_b_exit_intent_dry_run_report(
        config=TrackBExitIntentDryRunReportConfig(repo_root=tmp_path),
        now=NOW,
        input_overrides=inputs,
    )


def _inputs() -> dict:
    broker_position = {
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "-1",
        "symbol": "MES",
        "track_b_root": "MES",
    }
    managed_position = {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "quantity": "1",
        "side": "SHORT",
        "lifecycle_id": "life-mes",
        "trade_id": "trade-mes",
        "strategy_id": "mes_strategy",
        "lane_id": "mes_lane",
        "exit_due": True,
    }
    managed_order = {
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
    return {
        "reconciliation": {
            "generated_at": NOW.isoformat(),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "broker_reconciled": True,
            "track_b_broker_positions": [broker_position],
            "track_b_broker_position_count": 1,
            "track_b_broker_open_order_count": 0,
            "unknown_broker_open_order_count": 0,
            "unknown_broker_open_orders": [],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "managed_positions": {
            "generated_at": NOW.isoformat(),
            "classification": "OPEN_MANAGED_EXIT_DUE",
            "managed_positions": [managed_position],
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
        "managed_orders": {
            "generated_at": NOW.isoformat(),
            "classification": "POSITION_WITHOUT_CLOSE_ORDER",
            "managed_orders": [managed_order],
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
