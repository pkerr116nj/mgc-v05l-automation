from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.strategy_intent import (
    StrategyIntentValidationConfig,
    StrategyIntentValidationVerdict,
    validate_strategy_intent,
)
from mgc_v05l.execution_core.strategy_intent_cli import main as strategy_intent_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc)


def intent(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "mode": "PAPER",
        "account_id": "DUM882026",
        "instrument_family": "MGC",
        "local_execution_contract_key": "MGC-202606",
        "side": "BUY",
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": "4623.1",
        "time_in_force": "DAY",
        "signal_timestamp": aware_now().isoformat(),
        "decision_timestamp": aware_now().isoformat(),
        "reason": "synthetic strategy intent boundary test",
        "source": "unit_test",
        "submit_requested": False,
        "live_money_readiness": False,
        "databento_continuous_symbol": "MGC.v.0",
    }
    payload.update(overrides)
    return payload


def config(tmp_path: Path, **overrides: object) -> StrategyIntentValidationConfig:
    kwargs = {
        "expected_account_id": "DUM882026",
        "output_root": tmp_path / "strategy_intents",
    }
    kwargs.update(overrides)
    return StrategyIntentValidationConfig(**kwargs)


def test_valid_paper_lmt_day_intent_is_valid_for_paper_review(tmp_path: Path) -> None:
    result = validate_strategy_intent(
        payload=intent(),
        config=config(tmp_path),
        run_id="intent-valid",
        now=aware_now(),
    )
    payload = json.loads(result.report_json.read_text(encoding="utf-8"))

    assert result.verdict == StrategyIntentValidationVerdict.VALID_FOR_PAPER_REVIEW
    assert payload["intent_validation_verdict"] == "INTENT_VALID_FOR_PAPER_REVIEW"
    assert payload["intent_allowed_for_review"] is True
    assert payload["submit_allowed"] is False
    assert payload["submit_attempted"] is False
    assert payload["live_money_readiness"] is False
    assert payload["local_execution_contract_key"] == "MGC-202606"
    assert payload["local_execution_contract_key_is_execution_authority"] is True
    assert payload["databento_continuous_symbol"] == "MGC.v.0"
    assert payload["databento_symbol_is_execution_authority"] is False
    assert payload["required_next_action"]


@pytest.mark.parametrize(
    ("overrides", "verdict"),
    [
        ({"mode": "LIVE"}, StrategyIntentValidationVerdict.BLOCKED_LIVE_MODE),
        ({"order_type": "MKT"}, StrategyIntentValidationVerdict.BLOCKED_UNSUPPORTED_ORDER_TYPE),
        ({"time_in_force": "GTC"}, StrategyIntentValidationVerdict.BLOCKED_UNSUPPORTED_TIF),
        ({"limit_price": None}, StrategyIntentValidationVerdict.BLOCKED_MISSING_LIMIT_PRICE),
        ({"quantity": 0}, StrategyIntentValidationVerdict.BLOCKED_INVALID_QUANTITY),
        ({"quantity": -1}, StrategyIntentValidationVerdict.BLOCKED_INVALID_QUANTITY),
        ({"quantity": "1.5"}, StrategyIntentValidationVerdict.BLOCKED_INVALID_QUANTITY),
        ({"local_execution_contract_key": "MGC-209912"}, StrategyIntentValidationVerdict.BLOCKED_CONTRACT_NOT_ALLOWLISTED),
        ({"account_id": "OTHER"}, StrategyIntentValidationVerdict.BLOCKED_ACCOUNT_MISMATCH),
        ({"submit_requested": True}, StrategyIntentValidationVerdict.BLOCKED_SUBMIT_REQUESTED),
    ],
)
def test_invalid_intents_block_without_submit(tmp_path: Path, overrides: dict[str, object], verdict: StrategyIntentValidationVerdict) -> None:
    result = validate_strategy_intent(
        payload=intent(**overrides),
        config=config(tmp_path),
        run_id=f"intent-{verdict.value.lower()}",
        now=aware_now(),
    )

    assert result.verdict == verdict
    assert result.report["intent_allowed_for_review"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["primary_blocker"]
    assert result.report["required_next_action"]


def test_schema_error_blocks_without_submit(tmp_path: Path) -> None:
    result = validate_strategy_intent(
        payload={},
        config=config(tmp_path),
        run_id="intent-schema-error",
        now=aware_now(),
    )

    assert result.verdict == StrategyIntentValidationVerdict.BLOCKED_SCHEMA_ERROR
    assert result.intent is None
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False


def test_live_money_readiness_claim_is_neutralized(tmp_path: Path) -> None:
    result = validate_strategy_intent(
        payload=intent(live_money_readiness=True),
        config=config(tmp_path),
        run_id="intent-live-money-neutralized",
        now=aware_now(),
    )

    assert result.verdict == StrategyIntentValidationVerdict.VALID_FOR_PAPER_REVIEW
    assert result.report["live_money_readiness"] is False
    assert "Track B v1 report forces this to false" in result.report["secondary_blockers"][0]


def test_strategy_intent_cli_reads_json_and_writes_no_submit_report(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    intent_json = tmp_path / "intent.json"
    intent_json.write_text(json.dumps(intent(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = strategy_intent_cli_main(
        [
            "--intent-json",
            str(intent_json),
            "--expected-account-id",
            "DUM882026",
            "--output-root",
            str(tmp_path / "reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["intent_validation_verdict"] == "INTENT_VALID_FOR_PAPER_REVIEW"
    assert output["intent_allowed_for_review"] is True
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
