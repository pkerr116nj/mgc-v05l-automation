from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.models import TerminalClassification
from mgc_v05l.execution_core.paper_proof import PaperProofConfig, PaperProofResult
from mgc_v05l.execution_core.track_b_readiness_check_runner import (
    TrackBReadinessCheckRunnerResult,
    TrackBReadinessCheckRunnerVerdict,
)
from mgc_v05l.execution_core.track_b_strategy_paper_runner import (
    TrackBStrategyPaperRunnerConfig,
    TrackBStrategyPaperRunnerStages,
    TrackBStrategyPaperRunnerVerdict,
    run_track_b_strategy_paper,
)
from mgc_v05l.execution_core.track_b_strategy_paper_runner_cli import main as strategy_paper_runner_cli_main
from mgc_v05l.execution_core.track_b_strategy_rule_runner import (
    TrackBStrategyRuleRunnerResult,
    TrackBStrategyRuleRunnerVerdict,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 14, 30, tzinfo=timezone.utc)


class Calls:
    def __init__(self) -> None:
        self.strategy = 0
        self.readiness = 0
        self.proof = 0
        self.operator_status = 0


def base_config(tmp_path: Path, **overrides: object) -> TrackBStrategyPaperRunnerConfig:
    payload = {
        "mode": "PAPER",
        "input_event_payload": {
            "account_id": "DUM882026",
            "contract_key": "MGC-202606",
            "strategy_id": "track_b_example_gold_shadow_v1",
            "lane_id": "mgc_example_long_lmt_day",
            "candle_timestamp": aware_now().isoformat(),
            "observed_at": aware_now().isoformat(),
            "close": "4575.3",
            "metadata": {"fixture": True},
        },
        "inbox_dir": tmp_path / "inbox",
        "allow_fixture_input": True,
        "output_root": tmp_path / "paper_runner",
        "strategy_rule_output_root": tmp_path / "rule_runner",
        "strategy_adapter_output_root": tmp_path / "adapter",
        "candle_producer_output_root": tmp_path / "candle",
        "writer_output_root": tmp_path / "writer",
        "readiness_output_root": tmp_path / "readiness",
        "paper_proof_output_root": tmp_path / "proof",
        "operator_status_output_root": tmp_path / "operator_status",
    }
    payload.update(overrides)
    return TrackBStrategyPaperRunnerConfig(**payload)


def strategy_result(tmp_path: Path, *, verdict: str = "TRACK_B_STRATEGY_RULE_RUNNER_EMITTED_SIGNAL", decision: str = "LONG", emitted: bool = True) -> TrackBStrategyRuleRunnerResult:
    report_json = tmp_path / "strategy_rule_report.json"
    report = {
        "strategy_rule_runner_verdict": verdict,
        "decision": decision,
        "signal_emitted": emitted,
        "signal_direction": "LONG" if emitted else None,
        "output_batch_path": str(tmp_path / "inbox" / "signal_batch.json") if emitted else None,
        "primary_blocker": None,
        "required_next_action": "strategy next",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    return TrackBStrategyRuleRunnerResult(
        verdict=TrackBStrategyRuleRunnerVerdict.EMITTED_SIGNAL if emitted else TrackBStrategyRuleRunnerVerdict.NO_SIGNAL,
        report_json=report_json,
        report=report,
        downstream_strategy_adapter_report_json=None,
        downstream_candle_producer_report_json=None,
        downstream_signal_batch_writer_report_json=None,
        output_batch_json=Path(str(report["output_batch_path"])) if emitted else None,
    )


def readiness_result(tmp_path: Path, *, ready: bool = True) -> TrackBReadinessCheckRunnerResult:
    report_json = tmp_path / "readiness_report.json"
    verdict = (
        TrackBReadinessCheckRunnerVerdict.READY_FOR_PAPER_PROOF_REVIEW
        if ready
        else TrackBReadinessCheckRunnerVerdict.BLOCKED_CURRENT_QUOTE
    )
    report = {
        "runner_verdict": verdict.value,
        "readiness_verdict": "READY_FOR_PAPER_PROOF" if ready else "BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE",
        "realtime_quote_received": ready,
        "current_quote_available": ready,
        "quote_provider_mode": "REALTIME",
        "primary_blocker": None if ready else "Current quote unavailable.",
        "required_next_action": "readiness next",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    return TrackBReadinessCheckRunnerResult(verdict=verdict, report_json=report_json, report=report)


def proof_result(tmp_path: Path, classification: TerminalClassification) -> PaperProofResult:
    report_json = tmp_path / "paper_proof_report.json"
    report_md = tmp_path / "paper_proof_report.md"
    lifecycle_status = "AMBIGUOUS_MANUAL_REVIEW_REQUIRED"
    final_reconciliation = None
    failure_or_ambiguity = "callback gap"
    required_manual_action = "Manual review required."
    if classification == TerminalClassification.PASSED:
        lifecycle_status = "PROOF_COMPLETE_FLAT"
        final_reconciliation = {"status": "CLEAN"}
        failure_or_ambiguity = None
        required_manual_action = None
    elif classification == TerminalClassification.FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE:
        lifecycle_status = "PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE"
        failure_or_ambiguity = "flat but close provenance incomplete"
        required_manual_action = "Verify broker activity."
    proof_payload = {
        "classification": classification.value,
        "proof_lifecycle_status": lifecycle_status,
        "final_reconciliation": final_reconciliation,
        "failure_or_ambiguity": failure_or_ambiguity,
        "required_manual_action": required_manual_action,
    }
    report = {
        "classification": classification.value,
        "proof_payload": proof_payload,
        "failure_or_ambiguity": proof_payload["failure_or_ambiguity"],
        "required_manual_action": proof_payload["required_manual_action"],
        "final_readiness_verdict": "READY_FOR_PAPER_PROOF",
        "submit_allowed": classification == TerminalClassification.PASSED,
        "submit_attempted": True,
        "live_money_readiness": False,
    }
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(json.dumps(report), encoding="utf-8")
    report_md.write_text("# proof", encoding="utf-8")
    return PaperProofResult(
        run_id="proof-test",
        classification=classification,
        report_json=report_json,
        report_md=report_md,
        report=report,
        proof_result=None,
    )


def stages(
    *,
    calls: Calls,
    strategy: TrackBStrategyRuleRunnerResult,
    readiness: TrackBReadinessCheckRunnerResult | None = None,
    proof: PaperProofResult | None = None,
) -> TrackBStrategyPaperRunnerStages:
    def strategy_stage(config: TrackBStrategyPaperRunnerConfig) -> TrackBStrategyRuleRunnerResult:
        calls.strategy += 1
        return strategy

    def readiness_stage(config: TrackBStrategyPaperRunnerConfig) -> TrackBReadinessCheckRunnerResult:
        calls.readiness += 1
        assert readiness is not None
        return readiness

    def proof_stage(config: TrackBStrategyPaperRunnerConfig) -> PaperProofResult:
        calls.proof += 1
        assert proof is not None
        return proof

    def operator_status_stage(config: TrackBStrategyPaperRunnerConfig, runner_report_json: Path) -> None:
        calls.operator_status += 1

    return TrackBStrategyPaperRunnerStages(
        strategy_rule=strategy_stage,
        readiness=readiness_stage,
        paper_proof=proof_stage,
        operator_status=operator_status_stage,
    )


def test_no_signal_stops_without_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path, decision="NO_SIGNAL", emitted=False)),
        runner_id="paper-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.NO_SIGNAL
    assert calls.strategy == 1
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_human_review_stops_without_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path, decision="HUMAN_REVIEW", emitted=False)),
        runner_id="paper-human-review",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.HUMAN_REVIEW_NO_SIGNAL
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["paper_proof_invoked"] is False


def test_signal_with_blocked_readiness_does_not_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path, emit_signal=True),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path), readiness=readiness_result(tmp_path, ready=False)),
        runner_id="paper-readiness-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_READINESS
    assert calls.strategy == 1
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["readiness_verdict"] == "BLOCKED_MARKET_DATA_MODE_OR_QUOTE_UNAVAILABLE"
    assert result.report["submit_attempted"] is False


def test_blocked_strategy_rule_does_not_run_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    blocked_strategy = strategy_result(
        tmp_path,
        verdict="TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED_NON_REALTIME_INPUT",
        decision="NO_SIGNAL",
        emitted=False,
    )
    blocked_strategy.report["primary_blocker"] = "Input is not explicitly REALTIME."

    result = run_track_b_strategy_paper(
        config=base_config(tmp_path, emit_signal=True),
        stages=stages(calls=calls, strategy=blocked_strategy),
        runner_id="paper-strategy-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_STRATEGY_RULE
    assert calls.strategy == 1
    assert calls.readiness == 0
    assert calls.proof == 0
    assert result.report["primary_blocker"] == "Input is not explicitly REALTIME."
    assert result.report["submit_attempted"] is False


def test_signal_and_green_readiness_without_submit_flags_stops_ready_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path, emit_signal=True),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path), readiness=readiness_result(tmp_path)),
        runner_id="paper-ready-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED
    assert calls.readiness == 1
    assert calls.proof == 0
    assert result.report["paper_submit_requested"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_allowed"] is False
    assert result.report["submit_attempted"] is False


def test_signal_readiness_green_and_explicit_submit_invokes_paper_proof(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.PASSED),
        ),
        runner_id="paper-proof-passed",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_PASSED
    assert calls.proof == 1
    assert result.report["paper_submit_requested"] is True
    assert result.report["paper_proof_invoked"] is True
    assert result.report["paper_proof_classification"] == "TRACK_B_PAPER_PROOF_PASSED"
    assert result.report["final_flat"] is True
    assert result.report["final_position_status"] == "CLEAN"
    assert result.report["submit_allowed"] is True
    assert result.report["submit_attempted"] is True
    assert result.report["live_money_readiness"] is False


def test_ambiguous_paper_proof_requires_manual_review(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.AMBIGUOUS_MANUAL_REVIEW_REQUIRED),
        ),
        runner_id="paper-proof-ambiguous",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED
    assert result.report["paper_proof_classification"] == "TRACK_B_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED"
    assert result.report["final_flat"] is False
    assert result.report["primary_blocker"] == "callback gap"
    assert result.report["submit_attempted"] is True


def test_flat_but_close_provenance_incomplete_is_not_paper_proof_passed(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            emit_signal=True,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(
            calls=calls,
            strategy=strategy_result(tmp_path),
            readiness=readiness_result(tmp_path),
            proof=proof_result(tmp_path, TerminalClassification.FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE),
        ),
        runner_id="paper-proof-flat-provenance",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE
    assert result.report["paper_proof_classification"] == "TRACK_B_PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE"
    assert result.report["final_flat"] is True
    assert result.report["primary_blocker"] == "flat but close provenance incomplete"
    assert result.report["submit_attempted"] is True
    assert result.report["live_money_readiness"] is False


def test_non_paper_mode_refuses_before_strategy(tmp_path: Path) -> None:
    calls = Calls()
    result = run_track_b_strategy_paper(
        config=base_config(tmp_path, mode="LIVE"),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path)),
        runner_id="paper-non-paper-mode",
        now=aware_now(),
    )

    assert result.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_NON_PAPER_MODE
    assert calls.strategy == 0
    assert calls.proof == 0
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_submit_requested_missing_manual_prices_or_quantity_refuses_before_strategy(tmp_path: Path) -> None:
    calls = Calls()
    missing_quantity = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            submit_paper=True,
            confirm_paper_submit=True,
            manual_open_limit_price="4575.3",
            manual_close_limit_price="4575.0",
        ),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path)),
        runner_id="paper-missing-quantity",
        now=aware_now(),
    )
    missing_price = run_track_b_strategy_paper(
        config=base_config(
            tmp_path,
            submit_paper=True,
            confirm_paper_submit=True,
            quantity=1,
            manual_open_limit_price="4575.3",
        ),
        stages=stages(calls=calls, strategy=strategy_result(tmp_path)),
        runner_id="paper-missing-price",
        now=aware_now(),
    )

    assert missing_quantity.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "--quantity" in str(missing_quantity.report["primary_blocker"])
    assert missing_price.verdict == TrackBStrategyPaperRunnerVerdict.BLOCKED_INVALID_SUBMIT_REQUEST
    assert "--manual-close-limit-price" in str(missing_price.report["primary_blocker"])
    assert calls.strategy == 0
    assert calls.proof == 0
    assert missing_price.report["submit_attempted"] is False


def test_cli_dry_run_no_signal_does_not_submit(tmp_path: Path, capsys) -> None:  # type: ignore[no-untyped-def]
    event_json = tmp_path / "event.json"
    event_json.write_text(
        json.dumps(
            {
                "account_id": "DUM882026",
                "contract_key": "MGC-202606",
                "strategy_id": "track_b_example_gold_shadow_v1",
                "lane_id": "mgc_example_long_lmt_day",
                "candle_timestamp": aware_now().isoformat(),
                "observed_at": aware_now().isoformat(),
                "close": "4575.3",
                "metadata": {"fixture": True},
            }
        ),
        encoding="utf-8",
    )

    exit_code = strategy_paper_runner_cli_main(
        [
            "--mode",
            "PAPER",
            "--input-event-json",
            str(event_json),
            "--inbox-dir",
            str(tmp_path / "inbox"),
            "--allow-fixture-input",
            "--output-root",
            str(tmp_path / "paper_runner"),
            "--strategy-rule-output-root",
            str(tmp_path / "rule_runner"),
            "--strategy-adapter-output-root",
            str(tmp_path / "adapter"),
            "--candle-producer-output-root",
            str(tmp_path / "candle"),
            "--writer-output-root",
            str(tmp_path / "writer"),
            "--operator-status-output-root",
            str(tmp_path / "operator_status"),
        ]
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["strategy_paper_runner_verdict"] in {
        "TRACK_B_STRATEGY_PAPER_RUNNER_NO_SIGNAL",
        "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_READY_NO_SUBMIT_REQUESTED",
    }
    assert output["paper_proof_invoked"] is False
    assert output["submit_attempted"] is False
    assert output["live_money_readiness"] is False
