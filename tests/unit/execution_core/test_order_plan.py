from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.order_plan import (
    OrderPlanConfig,
    OrderPlanVerdict,
    create_order_plan_from_intent,
)
from mgc_v05l.execution_core.order_plan_cli import main as order_plan_cli_main


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


def config(tmp_path: Path, **overrides: object) -> OrderPlanConfig:
    kwargs = {
        "expected_account_id": "DUM882026",
        "output_root": tmp_path / "order_plans",
    }
    kwargs.update(overrides)
    return OrderPlanConfig(**kwargs)


def test_valid_paper_lmt_day_intent_creates_no_submit_order_plan(tmp_path: Path) -> None:
    result = create_order_plan_from_intent(
        payload=intent(),
        config=config(tmp_path),
        run_id="plan-valid",
        now=aware_now(),
    )
    payload = json.loads(result.report_json.read_text(encoding="utf-8"))

    assert result.verdict == OrderPlanVerdict.CREATED_FOR_PAPER_REVIEW
    assert payload["order_plan_verdict"] == "ORDER_PLAN_CREATED_FOR_PAPER_REVIEW"
    assert payload["order_plan_created"] is True
    assert payload["order_plan_id"].startswith("order_plan_")
    assert payload["source_intent_validation_verdict"] == "INTENT_VALID_FOR_PAPER_REVIEW"
    assert payload["strategy_id"] == "track_b_test_strategy"
    assert payload["lane_id"] == "paper_proof_lane"
    assert payload["account_id"] == "DUM882026"
    assert payload["local_execution_contract_key"] == "MGC-202606"
    assert payload["action"] == "BUY"
    assert payload["quantity"] == 1
    assert payload["order_type"] == "LMT"
    assert payload["limit_price"] == "4623.1"
    assert payload["time_in_force"] == "DAY"
    assert payload["submit_allowed"] is False
    assert payload["submit_attempted"] is False
    assert payload["live_money_readiness"] is False
    assert payload["paper_proof_cli_remains_only_submit_path"] is True
    assert payload["strategy_intent_boundary_is_no_submit"] is True
    assert payload["local_execution_contract_key_is_execution_authority"] is True
    assert payload["databento_symbol_is_execution_authority"] is False
    assert {
        "recovery_status_ready_clean",
        "read_only_preflight_ready",
        "proof_timing_active_session",
        "readiness_summary_ready_for_paper_proof",
    }.issubset(set(payload["required_gates_before_submit"]))


@pytest.mark.parametrize(
    ("overrides", "verdict"),
    [
        ({"mode": "LIVE"}, OrderPlanVerdict.BLOCKED_LIVE_MODE),
        ({"order_type": "MKT"}, OrderPlanVerdict.BLOCKED_UNSUPPORTED_ORDER_TYPE),
        ({"time_in_force": "GTC"}, OrderPlanVerdict.BLOCKED_UNSUPPORTED_TIF),
        ({"limit_price": None}, OrderPlanVerdict.BLOCKED_MISSING_LIMIT_PRICE),
        ({"quantity": 0}, OrderPlanVerdict.BLOCKED_INVALID_QUANTITY),
        ({"quantity": -1}, OrderPlanVerdict.BLOCKED_INVALID_QUANTITY),
        ({"quantity": "1.5"}, OrderPlanVerdict.BLOCKED_INVALID_QUANTITY),
        ({"local_execution_contract_key": "MGC-209912"}, OrderPlanVerdict.BLOCKED_CONTRACT_NOT_ALLOWLISTED),
        ({"account_id": "OTHER"}, OrderPlanVerdict.BLOCKED_ACCOUNT_MISMATCH),
    ],
)
def test_invalid_intent_does_not_create_order_plan(tmp_path: Path, overrides: dict[str, object], verdict: OrderPlanVerdict) -> None:
    result = create_order_plan_from_intent(
        payload=intent(**overrides),
        config=config(tmp_path),
        run_id=f"plan-{verdict.value.lower()}",
        now=aware_now(),
    )

    assert result.verdict == verdict
    assert result.report["order_plan_created"] is False
    assert result.report["order_plan_id"] is None
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["primary_blocker"]
    assert result.report["required_next_action"]


def test_schema_error_is_blocked_as_invalid_intent(tmp_path: Path) -> None:
    result = create_order_plan_from_intent(
        payload={},
        config=config(tmp_path),
        run_id="plan-schema-error",
        now=aware_now(),
    )

    assert result.verdict == OrderPlanVerdict.BLOCKED_INVALID_INTENT
    assert result.report["order_plan_created"] is False
    assert result.report["source_intent_validation_verdict"] == "INTENT_BLOCKED_SCHEMA_ERROR"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False


def test_order_plan_cli_reads_intent_json_and_writes_no_submit_report(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    intent_json = tmp_path / "intent.json"
    intent_json.write_text(json.dumps(intent(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = order_plan_cli_main(
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
    assert output["order_plan_verdict"] == "ORDER_PLAN_CREATED_FOR_PAPER_REVIEW"
    assert output["order_plan_created"] is True
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
