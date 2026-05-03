from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.signal_intent_proposal import IntentProposalVerdict, SignalIntentProposalConfig, propose_intent_from_signal
from mgc_v05l.execution_core.signal_intent_proposal_cli import main as proposal_cli_main


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
        "reason": "synthetic signal-to-intent proposal test",
        "submit_requested": False,
        "live_money_readiness": False,
        "metadata": {
            "submit_allowed": True,
            "live_money_readiness": True,
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
        "confidence_level": "HIGH",
        "model_version": "static_research_v1",
        "scoring_authority": "INFORMATIONAL_ONLY",
    }
    payload.update(overrides)
    return payload


def policy(**overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "quantity": 1,
        "order_type": "LMT",
        "limit_price": "4623.1",
        "limit_price_source": "fixture",
        "time_in_force": "DAY",
        "source": "unit_test_signal_intent_proposal",
        "min_signal_score": "0.70",
        "min_expected_value_r": "0.10",
        "allowed_confidence_levels": ["MEDIUM", "HIGH"],
    }
    payload.update(overrides)
    return payload


def config(tmp_path: Path) -> SignalIntentProposalConfig:
    return SignalIntentProposalConfig(expected_account_id="DUM882026", output_root=tmp_path / "signal_intent_proposals")


def test_valid_binary_signal_with_order_defaults_creates_proposed_intent(tmp_path: Path) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(),
        policy_payload=policy(),
        run_id="proposal-binary",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.CREATED_FOR_REVIEW
    assert result.report["intent_proposal_created"] is True
    assert result.proposed_intent is not None
    assert result.proposed_intent["side"] == "BUY"
    assert result.proposed_intent["quantity"] == 1
    assert result.proposed_intent["order_type"] == "LMT"
    assert result.proposed_intent["time_in_force"] == "DAY"
    assert result.proposed_intent["submit_requested"] is False
    assert result.proposed_intent["live_money_readiness"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["lane_authorized"] is False
    assert result.report["order_plan_created"] is False


@pytest.mark.parametrize(
    "policy_overrides",
    [
        {"quantity": None},
        {"order_type": None},
        {"time_in_force": None},
        {"limit_price": None},
        {"order_type": "MKT"},
    ],
)
def test_binary_signal_missing_required_order_defaults_blocks(tmp_path: Path, policy_overrides: dict[str, object]) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(),
        policy_payload=policy(**policy_overrides),
        run_id="proposal-binary-missing-order-fields",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.BLOCKED_BINARY_POLICY_MISSING_ORDER_FIELDS
    assert result.proposed_intent is None
    assert result.report["intent_proposal_created"] is False
    assert result.report["submit_allowed"] is False


def test_scored_static_above_threshold_creates_proposed_intent(tmp_path: Path) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(decision_style="SCORED_STATIC", scoring=scoring()),
        policy_payload=policy(),
        run_id="proposal-static-pass",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.CREATED_FOR_REVIEW
    assert result.report["scoring_present"] is True
    assert result.report["scoring_passed_policy"] is True
    assert result.proposed_intent is not None
    assert result.report["submit_allowed"] is False


@pytest.mark.parametrize(
    "score_overrides",
    [
        {"signal_score": "0.69"},
        {"expected_value_r": "0.09"},
        {"confidence_level": "LOW"},
    ],
)
def test_scored_static_below_threshold_blocks(tmp_path: Path, score_overrides: dict[str, object]) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(decision_style="SCORED_STATIC", scoring=scoring(**score_overrides)),
        policy_payload=policy(),
        run_id="proposal-static-below",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.BLOCKED_STATIC_SCORE_BELOW_THRESHOLD
    assert result.proposed_intent is None
    assert result.report["submit_allowed"] is False


def test_scored_static_missing_scoring_blocks(tmp_path: Path) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(decision_style="SCORED_STATIC"),
        policy_payload=policy(),
        run_id="proposal-static-missing-scoring",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.BLOCKED_SCORING_REQUIRED
    assert result.report["signal_validation_verdict"] == "SHADOW_SIGNAL_BLOCKED_SCORING_REQUIRED"
    assert result.proposed_intent is None
    assert result.report["submit_allowed"] is False


def test_scored_static_missing_policy_thresholds_blocks_as_scoring_required(tmp_path: Path) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(decision_style="SCORED_STATIC", scoring=scoring()),
        policy_payload=policy(min_signal_score=None, min_expected_value_r=None, allowed_confidence_levels=[]),
        run_id="proposal-static-policy-missing-thresholds",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.BLOCKED_SCORING_REQUIRED
    assert result.proposed_intent is None


def test_scored_dynamic_does_not_create_automatic_intent(tmp_path: Path) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(decision_style="SCORED_DYNAMIC", scoring=scoring(model_version="future_dynamic")),
        policy_payload=policy(allow_dynamic_scoring_for_review_only=True),
        run_id="proposal-dynamic",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.BLOCKED_DYNAMIC_SCORING_NOT_IMPLEMENTED
    assert result.proposed_intent is None
    assert result.report["order_plan_created"] is False
    assert result.report["submit_allowed"] is False


def test_human_review_does_not_create_automatic_intent(tmp_path: Path) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(decision_style="HUMAN_REVIEW", signal_direction="NONE"),
        policy_payload=policy(),
        run_id="proposal-human-review",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.BLOCKED_HUMAN_REVIEW_ONLY
    assert result.proposed_intent is None
    assert result.report["submit_allowed"] is False


@pytest.mark.parametrize(
    ("signal_overrides", "verdict"),
    [
        ({"mode": "LIVE"}, IntentProposalVerdict.BLOCKED_LIVE_MODE),
        ({"lane_id": ""}, IntentProposalVerdict.BLOCKED_INVALID_SIGNAL),
        ({"account_id": ""}, IntentProposalVerdict.BLOCKED_SCHEMA_ERROR),
        ({"local_execution_contract_key": ""}, IntentProposalVerdict.BLOCKED_INVALID_SIGNAL),
    ],
)
def test_invalid_signal_blocks_proposal(tmp_path: Path, signal_overrides: dict[str, object], verdict: IntentProposalVerdict) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(decision_style="SCORED_STATIC", scoring=scoring(signal_score="1.0"), **signal_overrides),
        policy_payload=policy(),
        run_id="proposal-invalid-signal",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == verdict
    assert result.proposed_intent is None
    assert result.report["intent_proposal_created"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["lane_authorized"] is False
    assert result.report["order_plan_created"] is False


def test_high_score_cannot_bypass_registry_order_plan_or_readiness(tmp_path: Path) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(decision_style="SCORED_STATIC", scoring=scoring(signal_score="1.0", probability_win="1.0")),
        policy_payload=policy(min_signal_score="0.01"),
        run_id="proposal-high-score-no-bypass",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.CREATED_FOR_REVIEW
    assert result.report["lane_authorized"] is False
    assert result.report["proposal_is_lane_authorization"] is False
    assert result.report["proposal_is_order_plan"] is False
    assert result.report["order_plan_created"] is False
    assert result.report["proposal_is_readiness"] is False
    assert result.report["manifest_registry_readiness_gates_bypassed"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["live_money_readiness"] is False


def test_metadata_cannot_override_proposal_safety_fields(tmp_path: Path) -> None:
    result = propose_intent_from_signal(
        signal_payload=signal(metadata={"limit_price": "1", "submit_allowed": True, "live_money_readiness": True}),
        policy_payload=policy(limit_price="4623.1"),
        run_id="proposal-metadata-no-authority",
        now=aware_now(),
        config=config(tmp_path),
    )

    assert result.verdict == IntentProposalVerdict.CREATED_FOR_REVIEW
    assert result.proposed_intent is not None
    assert result.proposed_intent["limit_price"] == "4623.1"
    assert result.report["metadata_is_authoritative"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["live_money_readiness"] is False


def test_signal_intent_proposal_cli_reads_json_and_writes_report(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    signal_json = tmp_path / "signal.json"
    policy_json = tmp_path / "policy.json"
    signal_json.write_text(json.dumps(signal(), indent=2, sort_keys=True), encoding="utf-8")
    policy_json.write_text(json.dumps(policy(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = proposal_cli_main(
        [
            "--signal-json",
            str(signal_json),
            "--policy-json",
            str(policy_json),
            "--expected-account-id",
            "DUM882026",
            "--output-root",
            str(tmp_path / "reports"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["intent_proposal_verdict"] == "INTENT_PROPOSAL_CREATED_FOR_REVIEW"
    assert output["intent_proposal_created"] is True
    assert output["signal_validation_verdict"] == "SHADOW_SIGNAL_VALID_FOR_REVIEW"
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
