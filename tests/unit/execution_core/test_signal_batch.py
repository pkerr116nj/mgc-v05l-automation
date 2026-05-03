from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core.signal_batch import SignalBatchVerdict, process_signal_batch
from mgc_v05l.execution_core.signal_batch_cli import main as signal_batch_cli_main


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
        "reason": "synthetic signal batch test",
        "submit_requested": False,
        "live_money_readiness": False,
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


def batch(*signals: dict[str, object], **overrides: object) -> dict[str, object]:
    payload: dict[str, object] = {
        "batch_id": "signal_batch_test_001",
        "shadow_run_id": "shadow_run_test_001",
        "mode": "PAPER",
        "expected_account_id": "DUM882026",
        "default_policy_id": "policy_test_001",
        "proposal_policy_path": "examples/track_b_shadow_run/proposal_policy_scored_static.json",
        "signal_items": [{"signal": row} for row in signals],
        "submit_enabled": False,
        "live_money_readiness": False,
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
        "source": "unit_test_signal_batch",
        "min_signal_score": "0.70",
        "min_expected_value_r": "0.10",
        "allowed_confidence_levels": ["MEDIUM", "HIGH"],
    }
    payload.update(overrides)
    return payload


def test_valid_batch_processes_binary_and_static_signals_for_review(tmp_path: Path) -> None:
    result = process_signal_batch(
        batch_payload=batch(
            signal(signal_id="binary"),
            signal(signal_id="static-pass", decision_style="SCORED_STATIC", scoring=scoring()),
            signal(signal_id="static-block", decision_style="SCORED_STATIC", scoring=scoring(signal_score="0.41", expected_value_r="0.03", confidence_level="LOW")),
        ),
        policy_payload=policy(),
        expected_account_id="DUM882026",
        output_root=tmp_path / "signal_batches",
        run_id="batch-valid",
        now=aware_now(),
    )

    assert result.verdict == SignalBatchVerdict.COMPLETED_WITH_BLOCKERS
    assert result.report["total_signals"] == 3
    assert result.report["signals_validated"] == 3
    assert result.report["proposal_attempts"] == 3
    assert result.report["proposed_intents_created"] == 2
    assert result.report["blocked_proposals"] == 1
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["broker_connection_attempted"] is False
    assert result.report["market_data_connection_attempted"] is False
    assert len(result.report["proposed_intent_output_paths"]) == 2

    intents = [json.loads(Path(path).read_text(encoding="utf-8")) for path in result.report["proposed_intent_output_paths"]]
    assert all(intent["submit_requested"] is False for intent in intents)
    assert all(intent["live_money_readiness"] is False for intent in intents)


@pytest.mark.parametrize(
    ("batch_overrides", "policy_payload", "verdict"),
    [
        ({"signal_items": []}, policy(), SignalBatchVerdict.BLOCKED_EMPTY_SIGNALS),
        ({"mode": "LIVE"}, policy(), SignalBatchVerdict.BLOCKED_LIVE_MODE),
        ({"submit_enabled": True}, policy(), SignalBatchVerdict.BLOCKED_SUBMIT_ENABLED),
        ({}, None, SignalBatchVerdict.BLOCKED_MISSING_POLICY),
    ],
)
def test_signal_batch_blockers(
    tmp_path: Path,
    batch_overrides: dict[str, object],
    policy_payload: dict[str, object] | None,
    verdict: SignalBatchVerdict,
) -> None:
    result = process_signal_batch(
        batch_payload=batch(signal(), **batch_overrides),
        policy_payload=policy_payload,
        expected_account_id="DUM882026",
        output_root=tmp_path / "signal_batches",
        run_id=f"batch-{verdict.value.lower()}",
        now=aware_now(),
    )

    assert result.verdict == verdict
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False
    assert result.report["primary_blocker"]


def test_dynamic_scored_signal_is_blocked_for_automatic_proposal(tmp_path: Path) -> None:
    result = process_signal_batch(
        batch_payload=batch(signal(decision_style="SCORED_DYNAMIC", scoring=scoring(model_version="future_dynamic"))),
        policy_payload=policy(allow_dynamic_scoring_for_review_only=True),
        expected_account_id="DUM882026",
        output_root=tmp_path / "signal_batches",
        run_id="batch-dynamic",
        now=aware_now(),
    )

    assert result.verdict == SignalBatchVerdict.COMPLETED_WITH_BLOCKERS
    assert result.report["proposed_intents_created"] == 0
    assert result.report["blocked_proposals"] == 1
    assert result.report["blockers_count_by_type"]["INTENT_PROPOSAL_BLOCKED_DYNAMIC_SCORING_NOT_IMPLEMENTED"] == 1
    assert result.report["dynamic_scoring_implemented"] is False
    assert result.report["submit_allowed"] is False


def test_signal_batch_cli_reads_json_and_writes_summary(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    batch_json = tmp_path / "batch.json"
    policy_json = tmp_path / "policy.json"
    batch_json.write_text(json.dumps(batch(signal()), indent=2, sort_keys=True), encoding="utf-8")
    policy_json.write_text(json.dumps(policy(), indent=2, sort_keys=True), encoding="utf-8")

    exit_code = signal_batch_cli_main(
        [
            "--batch-json",
            str(batch_json),
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
    assert output["batch_validation_verdict"] == "SIGNAL_BATCH_PROCESSED_FOR_REVIEW"
    assert output["total_signals"] == 1
    assert output["proposed_intents_created"] == 1
    assert output["blocked_proposals"] == 0
    assert output["submit_allowed"] is False
    assert output["submit_attempted"] is False
