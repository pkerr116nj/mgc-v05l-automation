from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.order_plan import OrderPlanConfig, OrderPlanResult, OrderPlanVerdict
from mgc_v05l.execution_core.shadow_evaluation import (
    ShadowEvaluationConfig,
    ShadowEvaluationVerdict,
    run_shadow_evaluation,
)
from mgc_v05l.execution_core.shadow_evaluation_cli import main as shadow_evaluation_cli_main


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
        "reason": "synthetic shadow signal",
        "source": "unit_test",
        "submit_requested": False,
        "live_money_readiness": False,
        "databento_continuous_symbol": "MGC.v.0",
    }
    payload.update(overrides)
    return payload


def config(tmp_path: Path, **overrides: object) -> ShadowEvaluationConfig:
    kwargs = {
        "expected_account_id": "DUM882026",
        "output_root": tmp_path / "shadow_evaluations",
    }
    kwargs.update(overrides)
    return ShadowEvaluationConfig(**kwargs)


def readiness_summary(tmp_path: Path, **overrides: object) -> Path:
    payload: dict[str, object] = {
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "primary_blocker": None,
        "required_next_action": "All summary inputs are clean.",
        "report_json_path": str(tmp_path / "readiness_summary.json"),
    }
    payload.update(overrides)
    path = tmp_path / "readiness_summary.json"
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    return path


def test_valid_intent_and_order_plan_create_shadow_evaluation_for_review(tmp_path: Path) -> None:
    result = run_shadow_evaluation(
        payload=intent(),
        config=config(tmp_path),
        run_id="shadow-valid",
        now=aware_now(),
    )
    payload = json.loads(result.report_json.read_text(encoding="utf-8"))

    assert result.verdict == ShadowEvaluationVerdict.CREATED_FOR_REVIEW
    assert payload["shadow_evaluation_verdict"] == "SHADOW_EVALUATION_CREATED_FOR_REVIEW"
    assert payload["shadow_evaluation_id"].startswith("shadow_eval_")
    assert payload["strategy_id"] == "track_b_test_strategy"
    assert payload["lane_id"] == "paper_proof_lane"
    assert payload["mode"] == "PAPER"
    assert payload["account_id"] == "DUM882026"
    assert payload["local_execution_contract_key"] == "MGC-202606"
    assert payload["signal_direction"] == "BUY"
    assert payload["signal_reason"] == "synthetic shadow signal"
    assert payload["intent_validation_verdict"] == "INTENT_VALID_FOR_PAPER_REVIEW"
    assert payload["intent_allowed_for_review"] is True
    assert payload["order_plan_verdict"] == "ORDER_PLAN_CREATED_FOR_PAPER_REVIEW"
    assert payload["order_plan_created"] is True
    assert payload["readiness_verdict"] is None
    assert payload["submit_allowed"] is False
    assert payload["submit_attempted"] is False
    assert payload["live_money_readiness"] is False
    assert payload["local_execution_contract_key_is_execution_authority"] is True
    assert payload["databento_symbol_is_execution_authority"] is False
    assert payload["paper_proof_cli_remains_only_submit_path"] is True


def test_invalid_intent_blocks_shadow_evaluation(tmp_path: Path) -> None:
    result = run_shadow_evaluation(
        payload=intent(order_type="MKT"),
        config=config(tmp_path),
        run_id="shadow-invalid-intent",
        now=aware_now(),
    )

    assert result.verdict == ShadowEvaluationVerdict.BLOCKED_INVALID_INTENT
    assert result.report["shadow_evaluation_verdict"] == "SHADOW_EVALUATION_BLOCKED_INVALID_INTENT"
    assert result.report["intent_validation_verdict"] == "INTENT_BLOCKED_UNSUPPORTED_ORDER_TYPE"
    assert result.report["order_plan_created"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False


def test_order_plan_failure_blocks_shadow_evaluation_distinctly(tmp_path: Path) -> None:
    def failing_plan_factory(
        payload: Mapping[str, Any],  # noqa: ARG001
        config: OrderPlanConfig,  # noqa: ARG001
        run_id: str,
        now: datetime,  # noqa: ARG001
    ) -> OrderPlanResult:
        report_json = tmp_path / f"{run_id}.json"
        report = {
            "order_plan_verdict": "ORDER_PLAN_BLOCKED_INVALID_INTENT",
            "order_plan_created": False,
            "primary_blocker": "forced order plan failure",
            "required_next_action": "Fix forced order plan failure.",
            "secondary_blockers": [],
        }
        report_json.write_text(json.dumps(report), encoding="utf-8")
        return OrderPlanResult(verdict=OrderPlanVerdict.BLOCKED_INVALID_INTENT, report_json=report_json, report=report)

    result = run_shadow_evaluation(
        payload=intent(),
        config=config(tmp_path),
        run_id="shadow-plan-failure",
        now=aware_now(),
        order_plan_factory=failing_plan_factory,
    )

    assert result.verdict == ShadowEvaluationVerdict.BLOCKED_ORDER_PLAN
    assert result.report["shadow_evaluation_verdict"] == "SHADOW_EVALUATION_BLOCKED_ORDER_PLAN"
    assert result.report["primary_blocker"] == "forced order plan failure"
    assert result.report["submit_allowed"] is False


def test_readiness_summary_blocker_is_surfaced_distinctly(tmp_path: Path) -> None:
    result = run_shadow_evaluation(
        payload=intent(),
        config=config(
            tmp_path,
            readiness_summary_json=readiness_summary(
                tmp_path,
                final_readiness_verdict="BLOCKED_UNRESOLVED_BROKER_ORDER",
                primary_blocker="unresolved broker order blocks same account/contract submit",
                required_next_action="Wait for terminal broker order state.",
            ),
        ),
        run_id="shadow-readiness-blocked",
        now=aware_now(),
    )

    assert result.verdict == ShadowEvaluationVerdict.READINESS_BLOCKED
    assert result.report["shadow_evaluation_verdict"] == "SHADOW_EVALUATION_READINESS_BLOCKED"
    assert result.report["readiness_verdict"] == "BLOCKED_UNRESOLVED_BROKER_ORDER"
    assert result.report["primary_blocker"] == "unresolved broker order blocks same account/contract submit"
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False


def test_shadow_evaluation_cli_writes_no_submit_report(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    intent_json = tmp_path / "intent.json"
    intent_json.write_text(json.dumps(intent(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = shadow_evaluation_cli_main(
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
    assert output["shadow_evaluation_verdict"] == "SHADOW_EVALUATION_CREATED_FOR_REVIEW"
    assert output["intent_validation_verdict"] == "INTENT_VALID_FOR_PAPER_REVIEW"
    assert output["order_plan_verdict"] == "ORDER_PLAN_CREATED_FOR_PAPER_REVIEW"
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
