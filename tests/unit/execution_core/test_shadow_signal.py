from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.shadow_signal import ShadowSignalValidationConfig, ShadowSignalValidationVerdict, validate_shadow_signal
from mgc_v05l.execution_core.shadow_signal_cli import main as shadow_signal_cli_main


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc)


def signal(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "signal_id": "example-signal-001",
        "strategy_id": "track_b_test_strategy",
        "lane_id": "paper_proof_lane",
        "signal_type": "breakout_retest",
        "signal_direction": "LONG",
        "decision_style": "BINARY",
        "mode": "PAPER",
        "account_id": "DUM882026",
        "instrument_family": "MGC",
        "local_execution_contract_key": "MGC-202606",
        "signal_timestamp": aware_now().isoformat(),
        "observed_at": aware_now().isoformat(),
        "source": "unit_test",
        "reason": "synthetic shadow signal boundary test",
        "submit_requested": False,
        "live_money_readiness": False,
        "metadata": {
            "note": "metadata stays non-authoritative",
        },
    }
    payload.update(overrides)
    return payload


def scoring(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "signal_score": "0.91",
        "score_scale": "ZERO_TO_ONE",
        "probability_win": "0.62",
        "expected_value_r": "0.34",
        "expected_drawdown_r": "0.18",
        "confidence_level": "HIGH",
        "model_version": "static_research_v1",
        "calibration_window": "2025Q1-2026Q1",
        "sample_size": 250,
        "regime_score": "0.72",
        "risk_quality": "medium",
        "scoring_authority": "INFORMATIONAL_ONLY",
    }
    payload.update(overrides)
    return payload


def config(tmp_path: Path) -> ShadowSignalValidationConfig:
    return ShadowSignalValidationConfig(expected_account_id="DUM882026", output_root=tmp_path / "shadow_signals")


def test_valid_binary_paper_signal_validates_without_scoring(tmp_path: Path) -> None:
    result = validate_shadow_signal(
        payload=signal(),
        config=config(tmp_path),
        run_id="signal-binary",
        now=aware_now(),
    )
    payload = json.loads(result.report_json.read_text(encoding="utf-8"))

    assert result.verdict == ShadowSignalValidationVerdict.VALID_FOR_REVIEW
    assert payload["shadow_signal_validation_verdict"] == "SHADOW_SIGNAL_VALID_FOR_REVIEW"
    assert payload["signal_allowed_for_review"] is True
    assert payload["decision_style"] == "BINARY"
    assert payload["scoring_present"] is False
    assert payload["submit_allowed"] is False
    assert payload["submit_attempted"] is False
    assert payload["live_money_readiness"] is False
    assert payload["signal_is_intent"] is False
    assert payload["signal_authorizes_lane"] is False
    assert payload["signal_creates_order_plan"] is False
    assert payload["order_plan_created"] is False
    assert payload["local_execution_contract_key_is_execution_authority"] is True


def test_valid_scored_static_signal_validates_with_informational_scoring(tmp_path: Path) -> None:
    result = validate_shadow_signal(
        payload=signal(decision_style="SCORED_STATIC", scoring=scoring()),
        config=config(tmp_path),
        run_id="signal-scored-static",
        now=aware_now(),
    )

    assert result.verdict == ShadowSignalValidationVerdict.VALID_FOR_REVIEW
    assert result.report["decision_style"] == "SCORED_STATIC"
    assert result.report["scoring_present"] is True
    assert result.report["scoring"]["signal_score"] == "0.91"
    assert result.report["scoring_authority"] == "INFORMATIONAL_ONLY"
    assert result.report["scoring_is_execution_authority"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["live_money_readiness"] is False


def test_scored_dynamic_schema_is_accepted_without_claiming_engine_implementation(tmp_path: Path) -> None:
    result = validate_shadow_signal(
        payload=signal(decision_style="SCORED_DYNAMIC", scoring=scoring(model_version="dynamic_shape_future")),
        config=config(tmp_path),
        run_id="signal-scored-dynamic",
        now=aware_now(),
    )

    assert result.verdict == ShadowSignalValidationVerdict.VALID_FOR_REVIEW
    assert result.report["decision_style"] == "SCORED_DYNAMIC"
    assert result.report["dynamic_scoring_implemented"] is False
    assert "dynamic scoring logic is not implemented" in result.report["secondary_blockers"][0]
    assert result.report["submit_allowed"] is False


def test_human_review_signal_does_not_require_scoring(tmp_path: Path) -> None:
    result = validate_shadow_signal(
        payload=signal(decision_style="HUMAN_REVIEW", signal_direction="NONE"),
        config=config(tmp_path),
        run_id="signal-human-review",
        now=aware_now(),
    )

    assert result.verdict == ShadowSignalValidationVerdict.VALID_FOR_REVIEW
    assert result.report["scoring_present"] is False
    assert result.report["submit_allowed"] is False


@pytest.mark.parametrize(
    ("overrides", "verdict"),
    [
        ({"mode": "LIVE"}, ShadowSignalValidationVerdict.BLOCKED_LIVE_MODE),
        ({"strategy_id": ""}, ShadowSignalValidationVerdict.BLOCKED_MISSING_STRATEGY),
        ({"lane_id": ""}, ShadowSignalValidationVerdict.BLOCKED_MISSING_LANE),
        ({"signal_direction": ""}, ShadowSignalValidationVerdict.BLOCKED_MISSING_DIRECTION),
        ({"local_execution_contract_key": ""}, ShadowSignalValidationVerdict.BLOCKED_MISSING_CONTRACT),
        ({"signal_timestamp": None}, ShadowSignalValidationVerdict.BLOCKED_MISSING_TIMESTAMP),
        ({"decision_style": "SCORED_STATIC"}, ShadowSignalValidationVerdict.BLOCKED_SCORING_REQUIRED),
        ({"decision_style": "SCORED_DYNAMIC"}, ShadowSignalValidationVerdict.BLOCKED_SCORING_REQUIRED),
        ({"decision_style": "SCORED_STATIC", "scoring": scoring(signal_score="1.2")}, ShadowSignalValidationVerdict.BLOCKED_INVALID_SCORE_RANGE),
        ({"decision_style": "SCORED_STATIC", "scoring": scoring(probability_win="-0.01")}, ShadowSignalValidationVerdict.BLOCKED_INVALID_SCORE_RANGE),
    ],
)
def test_shadow_signal_blockers(tmp_path: Path, overrides: dict[str, object], verdict: ShadowSignalValidationVerdict) -> None:
    result = validate_shadow_signal(
        payload=signal(**overrides),
        config=config(tmp_path),
        run_id=f"signal-{verdict.value.lower()}",
        now=aware_now(),
    )

    assert result.verdict == verdict
    assert result.report["signal_allowed_for_review"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["signal_authorizes_lane"] is False
    assert result.report["signal_creates_order_plan"] is False
    assert result.report["primary_blocker"]
    assert result.report["required_next_action"]


def test_high_score_cannot_authorize_or_submit_or_create_plan(tmp_path: Path) -> None:
    result = validate_shadow_signal(
        payload=signal(
            decision_style="SCORED_STATIC",
            scoring=scoring(signal_score="1.0", probability_win="1.0"),
            submit_requested=True,
            live_money_readiness=True,
        ),
        config=config(tmp_path),
        run_id="signal-high-score-no-authority",
        now=aware_now(),
    )

    assert result.verdict == ShadowSignalValidationVerdict.VALID_FOR_REVIEW
    assert result.report["signal_allowed_for_review"] is True
    assert result.report["signal_authorizes_lane"] is False
    assert result.report["signal_creates_order_plan"] is False
    assert result.report["order_plan_created"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["manifest_registry_readiness_gates_bypassed"] is False


def test_metadata_cannot_override_safety_fields(tmp_path: Path) -> None:
    result = validate_shadow_signal(
        payload=signal(
            metadata={
                "submit_allowed": True,
                "submit_attempted": True,
                "live_money_readiness": True,
                "local_execution_contract_key": "OTHER",
            }
        ),
        config=config(tmp_path),
        run_id="signal-metadata-no-authority",
        now=aware_now(),
    )

    assert result.verdict == ShadowSignalValidationVerdict.VALID_FOR_REVIEW
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["local_execution_contract_key"] == "MGC-202606"
    assert result.report["metadata_is_authoritative"] is False


def test_schema_error_blocks_without_submit(tmp_path: Path) -> None:
    result = validate_shadow_signal(
        payload=signal(decision_style="NOT_A_STYLE"),
        config=config(tmp_path),
        run_id="signal-schema-error",
        now=aware_now(),
    )

    assert result.verdict == ShadowSignalValidationVerdict.BLOCKED_SCHEMA_ERROR
    assert result.signal is None
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False


def test_shadow_signal_cli_reads_json_and_writes_no_submit_report(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    signal_json = tmp_path / "signal.json"
    signal_json.write_text(json.dumps(signal(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = shadow_signal_cli_main(
        [
            "--signal-json",
            str(signal_json),
            "--expected-account-id",
            "DUM882026",
            "--output-root",
            str(tmp_path / "reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["shadow_signal_validation_verdict"] == "SHADOW_SIGNAL_VALID_FOR_REVIEW"
    assert output["signal_allowed_for_review"] is True
    assert output["decision_style"] == "BINARY"
    assert output["scoring_present"] is False
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
