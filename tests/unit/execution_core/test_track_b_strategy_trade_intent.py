from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_strategy_trade_intent import (
    TrackBStrategyTradeIntentClassification,
    TrackBStrategyTradeIntentConfig,
    create_track_b_strategy_trade_intent,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 7, 14, 30, tzinfo=timezone.utc)


def config(tmp_path: Path, **overrides: object) -> TrackBStrategyTradeIntentConfig:
    payload: dict[str, object] = {
        "mode": "PAPER",
        "account_id": "DUM882026",
        "expected_account_id": "DUM882026",
        "strategy_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
        "instrument_family": "MNQ",
        "contract_key": "MNQ-202606",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "side": "BUY",
        "quantity": 1,
        "runtime_source": "DATABENTO_LIVE_ARTIFACT",
        "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
        "pricing_policy": "MARKETABLE_LIMIT_FROM_LIVE_CONTEXT",
        "managed_exit_policy_id": "DIAGNOSTIC_TIME_EXIT_IMMEDIATE",
        "output_root": tmp_path / "intents",
        "paper_trade_ledger_output_root": tmp_path / "ledger",
    }
    payload.update(overrides)
    return TrackBStrategyTradeIntentConfig(**payload)


def signal_report(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "signal_emitted": True,
        "real_strategy_signal": True,
        "signal_source": "MNQ_FIRST_BULL_SNAP_TURN_V1",
        "signal_direction": "LONG",
        "decision": "LONG",
        "strategy_registry_id": "MNQ_FIRST_BULL_SNAP_TURN_V1",
        "strategy_registry_paper_eligible": True,
        "strategy_registry_live_money_eligible": False,
        "strategy_registry_instrument_family": "MNQ",
        "latest_decision_bar_source": "DATABENTO_LIVE_ARTIFACT",
        "decision_bar_timestamp": "2026-05-07T14:25:00+00:00",
        "signal_timestamp": "2026-05-07T14:25:00+00:00",
        "candidate_reason": "first bull snap turn",
        "passed_predicates": ["SESSION_FILTER_PASSED", "SNAP_TURN_STRUCTURE_PASSED"],
        "report_json_path": "outputs/rule/report.json",
    }
    payload.update(overrides)
    return payload


def test_real_hard_signal_creates_persisted_strategy_trade_intent(tmp_path: Path) -> None:
    result = create_track_b_strategy_trade_intent(
        config=config(tmp_path),
        strategy_report=signal_report(),
        intent_id="intent-001",
        now=aware_now(),
    )

    assert result.classification == TrackBStrategyTradeIntentClassification.CREATED
    assert result.intent_created is True
    assert result.report["intent_id"] == "intent-001"
    assert result.report["strategy_id"] == "MNQ_FIRST_BULL_SNAP_TURN_V1"
    assert result.report["instrument"] == "MNQ"
    assert result.report["contract_key"] == "MNQ-202606"
    assert result.report["local_symbol"] == "MNQM6"
    assert result.report["order_action"] == "BUY"
    assert result.report["latest_decision_bar_source"] == "DATABENTO_LIVE_ARTIFACT"
    assert result.report["lifecycle_mode"] == "STRATEGY_MANAGED"
    assert result.report["paper_proof_invoked"] is False
    assert result.report["broker_state_mutated"] is False
    assert json.loads(result.latest_intent_json.read_text(encoding="utf-8"))["intent_created"] is True
    assert result.intent_jsonl.read_text(encoding="utf-8").count("intent-001") == 1


def test_no_signal_writes_blocked_status_but_no_intent(tmp_path: Path) -> None:
    result = create_track_b_strategy_trade_intent(
        config=config(tmp_path),
        strategy_report=signal_report(signal_emitted=False, real_strategy_signal=True),
        now=aware_now(),
    )

    assert result.classification == TrackBStrategyTradeIntentClassification.NOT_CREATED_NO_SIGNAL
    assert result.intent_created is False
    assert result.report["intent_id"] is None
    assert result.report["submit_attempted"] is False


def test_missing_exit_policy_blocks_before_lifecycle(tmp_path: Path) -> None:
    result = create_track_b_strategy_trade_intent(
        config=config(tmp_path, managed_exit_policy_id=None),
        strategy_report=signal_report(),
        now=aware_now(),
    )

    assert result.classification == TrackBStrategyTradeIntentClassification.BLOCKED_MISSING_EXIT_POLICY
    assert result.intent_created is False
    assert "paper_proof is not a strategy-management fallback" in str(result.report["primary_blocker"])


def test_review_required_state_blocks_intent(tmp_path: Path) -> None:
    status = tmp_path / "ledger" / "latest_track_b_live_position_status.json"
    status.parent.mkdir(parents=True, exist_ok=True)
    status.write_text(json.dumps({"review_required_count": 1, "positions": []}), encoding="utf-8")

    result = create_track_b_strategy_trade_intent(
        config=config(tmp_path),
        strategy_report=signal_report(),
        now=aware_now(),
    )

    assert result.classification == TrackBStrategyTradeIntentClassification.BLOCKED_REVIEW_REQUIRED
    assert result.intent_created is False


def test_non_live_decision_bar_blocks_intent(tmp_path: Path) -> None:
    result = create_track_b_strategy_trade_intent(
        config=config(tmp_path, latest_decision_bar_source="DATABENTO_HTTP_BACKFILL"),
        strategy_report=signal_report(latest_decision_bar_source="DATABENTO_HTTP_BACKFILL"),
        now=aware_now(),
    )

    assert result.classification == TrackBStrategyTradeIntentClassification.BLOCKED_NOT_LIVE_DECISION_BAR
    assert result.intent_created is False


def test_non_paper_eligible_strategy_blocks_intent(tmp_path: Path) -> None:
    result = create_track_b_strategy_trade_intent(
        config=config(tmp_path),
        strategy_report=signal_report(strategy_registry_paper_eligible=False),
        now=aware_now(),
    )

    assert result.classification == TrackBStrategyTradeIntentClassification.BLOCKED_NOT_PAPER_ELIGIBLE
    assert result.intent_created is False


def test_live_money_eligible_strategy_blocks_intent(tmp_path: Path) -> None:
    result = create_track_b_strategy_trade_intent(
        config=config(tmp_path),
        strategy_report=signal_report(strategy_registry_live_money_eligible=True),
        now=aware_now(),
    )

    assert result.classification == TrackBStrategyTradeIntentClassification.BLOCKED_LIVE_MONEY_DISABLED
    assert result.intent_created is False


def test_account_and_contract_guards_block_intent(tmp_path: Path) -> None:
    account_result = create_track_b_strategy_trade_intent(
        config=config(tmp_path, account_id="WRONG"),
        strategy_report=signal_report(),
        now=aware_now(),
    )
    contract_result = create_track_b_strategy_trade_intent(
        config=config(tmp_path, quantity=0),
        strategy_report=signal_report(),
        now=aware_now(),
    )

    assert account_result.classification == TrackBStrategyTradeIntentClassification.BLOCKED_ACCOUNT_GUARD
    assert contract_result.classification == TrackBStrategyTradeIntentClassification.BLOCKED_CONTRACT_GUARD
