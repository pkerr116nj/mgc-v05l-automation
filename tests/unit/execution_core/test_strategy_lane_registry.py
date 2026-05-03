from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.strategy_lane_registry import LaneValidationVerdict, validate_strategy_lane
from mgc_v05l.execution_core.strategy_lane_registry_cli import main as lane_registry_cli_main


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
        "reason": "synthetic lane registry test",
        "source": "unit_test",
        "submit_requested": False,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def lane(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "lane_status": "PAPER_REVIEW",
        "mode_allowed": "PAPER",
        "expected_account_id": "DUM882026",
        "allowed_local_execution_contract_keys": ["MGC-202606"],
        "allowed_instrument_family": "MGC",
        "allowed_sides": ["BUY", "SELL"],
        "allowed_order_types": ["LMT"],
        "allowed_time_in_force": ["DAY"],
        "max_quantity": 1,
        "live_money_readiness": False,
    }
    payload.update(overrides)
    return payload


def registry(*lanes: dict[str, object], **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "registry_version": "track_b_lane_registry_test_v1",
        "generated_at": aware_now().isoformat(),
        "lanes": list(lanes or (lane(),)),
    }
    payload.update(overrides)
    return payload


def test_known_enabled_lane_authorizes_valid_paper_intent_for_review(tmp_path: Path) -> None:
    result = validate_strategy_lane(
        intent_payload=intent(),
        registry_payload=registry(),
        output_root=tmp_path / "lane_registry",
        run_id="lane-valid",
        now=aware_now(),
    )
    payload = json.loads(result.report_json.read_text(encoding="utf-8"))

    assert result.verdict == LaneValidationVerdict.AUTHORIZED_FOR_PAPER_REVIEW
    assert payload["lane_validation_verdict"] == "LANE_AUTHORIZED_FOR_PAPER_REVIEW"
    assert payload["lane_authorized_for_review"] is True
    assert payload["lane_status"] == "PAPER_REVIEW"
    assert payload["submit_allowed"] is False
    assert payload["submit_attempted"] is False
    assert payload["live_money_readiness"] is False
    assert payload["strategy_id"] == "track_b_test_strategy"
    assert payload["lane_id"] == "paper_proof_lane"
    assert payload["account_id"] == "DUM882026"
    assert payload["local_execution_contract_key"] == "MGC-202606"
    assert payload["side"] == "BUY"
    assert payload["quantity"] == 1
    assert payload["order_type"] == "LMT"
    assert payload["time_in_force"] == "DAY"
    assert payload["max_quantity"] == "1"
    assert payload["registry_version"] == "track_b_lane_registry_test_v1"
    assert "recovery_status_ready_clean" in payload["required_gates_before_submit"]
    assert payload["local_execution_contract_key_is_execution_authority"] is True
    assert payload["databento_symbol_is_execution_authority"] is False


@pytest.mark.parametrize(
    ("intent_overrides", "registry_payload", "verdict"),
    [
        ({}, registry(lane(lane_status="DISABLED")), LaneValidationVerdict.BLOCKED_DISABLED),
        ({"strategy_id": "unknown"}, registry(), LaneValidationVerdict.BLOCKED_UNKNOWN_STRATEGY),
        ({"lane_id": "unknown"}, registry(), LaneValidationVerdict.BLOCKED_UNKNOWN_LANE),
        ({"account_id": "OTHER"}, registry(), LaneValidationVerdict.BLOCKED_ACCOUNT_MISMATCH),
        ({"local_execution_contract_key": "MGC-209912"}, registry(), LaneValidationVerdict.BLOCKED_CONTRACT_NOT_ALLOWED),
        ({"side": "SELL"}, registry(lane(allowed_sides=["BUY"])), LaneValidationVerdict.BLOCKED_SIDE_NOT_ALLOWED),
        ({"order_type": "MKT"}, registry(), LaneValidationVerdict.BLOCKED_ORDER_TYPE_NOT_ALLOWED),
        ({"time_in_force": "GTC"}, registry(), LaneValidationVerdict.BLOCKED_TIF_NOT_ALLOWED),
        ({"quantity": 2}, registry(), LaneValidationVerdict.BLOCKED_QUANTITY_EXCEEDS_MAX),
        ({"mode": "LIVE"}, registry(), LaneValidationVerdict.BLOCKED_LIVE_MODE),
    ],
)
def test_lane_registry_blockers(
    tmp_path: Path,
    intent_overrides: dict[str, object],
    registry_payload: dict[str, object],
    verdict: LaneValidationVerdict,
) -> None:
    result = validate_strategy_lane(
        intent_payload=intent(**intent_overrides),
        registry_payload=registry_payload,
        output_root=tmp_path / "lane_registry",
        run_id=f"lane-{verdict.value.lower()}",
        now=aware_now(),
    )

    assert result.verdict == verdict
    assert result.report["lane_authorized_for_review"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["primary_blocker"]
    assert result.report["required_next_action"]


def test_shadow_only_lane_authorizes_shadow_review_only(tmp_path: Path) -> None:
    result = validate_strategy_lane(
        intent_payload=intent(),
        registry_payload=registry(lane(lane_status="SHADOW_ONLY")),
        output_root=tmp_path / "lane_registry",
        run_id="lane-shadow",
        now=aware_now(),
    )

    assert result.verdict == LaneValidationVerdict.AUTHORIZED_FOR_SHADOW_REVIEW
    assert result.report["lane_authorized_for_review"] is True
    assert result.report["lane_status"] == "SHADOW_ONLY"
    assert result.report["submit_allowed"] is False


def test_lane_schema_error_blocks_without_submit(tmp_path: Path) -> None:
    result = validate_strategy_lane(
        intent_payload={},
        registry_payload={},
        output_root=tmp_path / "lane_registry",
        run_id="lane-schema-error",
        now=aware_now(),
    )

    assert result.verdict == LaneValidationVerdict.BLOCKED_SCHEMA_ERROR
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_lane_registry_cli_reads_json_and_writes_no_submit_report(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    intent_json = tmp_path / "intent.json"
    registry_json = tmp_path / "registry.json"
    intent_json.write_text(json.dumps(intent(), indent=2, sort_keys=True), encoding="utf-8")
    registry_json.write_text(json.dumps(registry(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = lane_registry_cli_main(
        [
            "--intent-json",
            str(intent_json),
            "--registry-json",
            str(registry_json),
            "--output-root",
            str(tmp_path / "reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["lane_validation_verdict"] == "LANE_AUTHORIZED_FOR_PAPER_REVIEW"
    assert output["lane_authorized_for_review"] is True
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
