from __future__ import annotations

import json
from dataclasses import replace
from datetime import datetime, timezone
from pathlib import Path

from mgc_v05l.execution_core.track_b_real_rule_wait_runner import (
    TrackBRealRuleWaitRunnerConfig,
    TrackBRealRuleWaitRunnerStages,
    TrackBRealRuleWaitRunnerVerdict,
    run_track_b_real_rule_wait,
)
from mgc_v05l.execution_core.track_b_runtime_candle_capture import (
    TrackBRuntimeCandleCaptureResult,
    TrackBRuntimeCandleCaptureVerdict,
)
from mgc_v05l.execution_core.track_b_strategy_paper_runner import (
    TrackBStrategyPaperRunnerConfig,
    TrackBStrategyPaperRunnerResult,
    TrackBStrategyPaperRunnerVerdict,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 4, 14, 30, tzinfo=timezone.utc)


class Calls:
    def __init__(self) -> None:
        self.capture = 0
        self.strategy = 0
        self.sleep = 0
        self.strategy_configs: list[TrackBStrategyPaperRunnerConfig] = []


def context_json(tmp_path: Path) -> Path:
    path = tmp_path / "latest_runtime_mgc_1m_candles.json"
    path.write_text(
        json.dumps(
            {
                "schema_version": "track_b_runtime_mgc_1m_candles_v1",
                "account_id": "DUM882026",
                "contract_key": "MGC-202606",
                "runtime_candle_context_ready": True,
                "quote_provider_mode": "REALTIME",
                "realtime_quote_received": True,
                "current_quote_available": True,
                "candles": [
                    {"candle_timestamp": "2026-05-04T14:28:00+00:00", "open": "4574.6", "high": "4574.7", "low": "4574.5", "close": "4574.6"},
                    {"candle_timestamp": "2026-05-04T14:29:00+00:00", "open": "4574.6", "high": "4574.9", "low": "4574.6", "close": "4574.8"},
                    {"candle_timestamp": "2026-05-04T14:30:00+00:00", "open": "4574.8", "high": "4575.4", "low": "4574.8", "close": "4575.3"},
                ],
            }
        ),
        encoding="utf-8",
    )
    return path


def base_config(tmp_path: Path, **overrides: object) -> TrackBRealRuleWaitRunnerConfig:
    paper = TrackBStrategyPaperRunnerConfig(
        mode="PAPER",
        runtime_candle_context_json=context_json(tmp_path),
        runtime_candle_context_required=True,
        inbox_dir=tmp_path / "inbox",
        source_id="track_b_real_rule_wait_test",
        output_root=tmp_path / "strategy_paper",
        strategy_rule_output_root=tmp_path / "rule",
        strategy_adapter_output_root=tmp_path / "adapter",
        candle_producer_output_root=tmp_path / "candle",
        writer_output_root=tmp_path / "writer",
        readiness_output_root=tmp_path / "readiness",
        paper_proof_output_root=tmp_path / "proof",
        operator_status_output_root=tmp_path / "operator_status",
    )
    payload: dict[str, object] = {
        "strategy_paper_config": paper,
        "runtime_candle_context_json": paper.runtime_candle_context_json,
        "max_cycles": 3,
        "poll_seconds": 0.0,
        "output_root": tmp_path / "wait",
    }
    payload.update(overrides)
    return TrackBRealRuleWaitRunnerConfig(**payload)


def strategy_result(
    tmp_path: Path,
    *,
    verdict: TrackBStrategyPaperRunnerVerdict,
    decision: str,
    emitted: bool,
    real_strategy_signal: bool = True,
    submit_requested: bool = False,
    proof_invoked: bool = False,
    broker_mutated: bool = False,
    final_flat: bool | None = None,
    primary_blocker: str | None = None,
) -> TrackBStrategyPaperRunnerResult:
    report_json = tmp_path / f"strategy_paper_{len(list(tmp_path.glob('strategy_paper_*.json')))}.json"
    report = {
        "strategy_paper_runner_verdict": verdict.value,
        "runtime_candle_context_ready": True,
        "feature_builder_invoked": True,
        "strategy_rule_evaluated": True,
        "signal_source": "REAL_STRATEGY_RULE" if real_strategy_signal else "DEMO_WIRING_PROOF",
        "real_strategy_signal": real_strategy_signal,
        "rule_decision": decision,
        "signal_emitted": emitted,
        "readiness_invoked": emitted,
        "paper_submit_requested": submit_requested,
        "paper_proof_invoked": proof_invoked,
        "submit_attempted": proof_invoked,
        "broker_state_mutated": broker_mutated,
        "paper_proof_classification": "TRACK_B_PAPER_PROOF_PASSED" if proof_invoked and final_flat else None,
        "final_flat": final_flat,
        "primary_blocker": primary_blocker,
        "required_next_action": "next action",
    }
    report_json.write_text(json.dumps(report), encoding="utf-8")
    return TrackBStrategyPaperRunnerResult(verdict=verdict, report_json=report_json, report=report)


def capture_result(tmp_path: Path, *, ready: bool = True) -> TrackBRuntimeCandleCaptureResult:
    report_json = tmp_path / "capture_report.json"
    event_json = tmp_path / "runtime_mgc_1m_candles.json"
    report = {
        "runtime_candle_capture_verdict": (
            TrackBRuntimeCandleCaptureVerdict.WROTE_RUNTIME_CANDLES.value
            if ready
            else TrackBRuntimeCandleCaptureVerdict.BLOCKED_INSUFFICIENT_RUNTIME_CANDLES.value
        ),
        "runtime_candle_context_ready": ready,
        "primary_blocker": None if ready else "At least 3 runtime candles are required; received 1.",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }
    report_json.write_text(json.dumps(report), encoding="utf-8")
    if ready:
        event_json.write_text(json.dumps({"runtime_candle_context_ready": True, "candles": []}), encoding="utf-8")
    return TrackBRuntimeCandleCaptureResult(
        verdict=TrackBRuntimeCandleCaptureVerdict.WROTE_RUNTIME_CANDLES
        if ready
        else TrackBRuntimeCandleCaptureVerdict.BLOCKED_INSUFFICIENT_RUNTIME_CANDLES,
        report_json=report_json,
        report=report,
        runtime_candles_json=event_json if ready else None,
        runtime_candles_event={"runtime_candle_context_ready": True} if ready else None,
    )


def stages(calls: Calls, results: list[TrackBStrategyPaperRunnerResult], capture: TrackBRuntimeCandleCaptureResult | None = None) -> TrackBRealRuleWaitRunnerStages:
    def capture_stage(config: TrackBRealRuleWaitRunnerConfig, cycle_index: int, now: datetime):
        if config.runtime_candle_source_json is None and config.runtime_candle_source_payload is None:
            return None
        calls.capture += 1
        return capture

    def strategy_stage(config: TrackBStrategyPaperRunnerConfig, runner_id: str, now: datetime) -> TrackBStrategyPaperRunnerResult:
        calls.strategy += 1
        calls.strategy_configs.append(config)
        return results[min(calls.strategy - 1, len(results) - 1)]

    def sleep_stage(seconds: float) -> None:
        calls.sleep += 1

    return TrackBRealRuleWaitRunnerStages(
        runtime_candle_capture=capture_stage,
        strategy_paper_runner=strategy_stage,
        sleep=sleep_stage,
    )


def test_bounded_polling_stops_after_repeated_no_signal_cycles_without_readiness_or_proof(tmp_path: Path) -> None:
    calls = Calls()
    no_signal = strategy_result(
        tmp_path,
        verdict=TrackBStrategyPaperRunnerVerdict.NO_SIGNAL,
        decision="NO_SIGNAL",
        emitted=False,
    )

    result = run_track_b_real_rule_wait(
        config=base_config(tmp_path, max_cycles=3, poll_seconds=1.0),
        stages=stages(calls, [no_signal]),
        runner_id="wait-no-signal",
        now=aware_now(),
    )

    assert result.verdict == TrackBRealRuleWaitRunnerVerdict.NO_SIGNAL_NO_MUTATION
    assert calls.strategy == 3
    assert calls.sleep == 2
    assert result.report["cycles_attempted"] == 3
    assert result.report["final_rule_decision"] == "NO_SIGNAL"
    assert result.report["ever_signal_emitted"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["broker_state_mutated"] is False
    assert result.report["live_money_readiness"] is False


def test_real_signal_without_submit_flags_reports_signal_ready_no_submit(tmp_path: Path) -> None:
    calls = Calls()
    signal = strategy_result(
        tmp_path,
        verdict=TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED,
        decision="LONG",
        emitted=True,
    )

    result = run_track_b_real_rule_wait(
        config=base_config(tmp_path, max_cycles=5),
        stages=stages(calls, [signal]),
        runner_id="wait-signal-no-submit",
        now=aware_now(),
    )

    assert result.verdict == TrackBRealRuleWaitRunnerVerdict.SIGNAL_READY_NO_SUBMIT
    assert calls.strategy == 1
    assert result.report["ever_signal_emitted"] is True
    assert result.report["paper_proof_invoked"] is False
    assert result.report["mutation_attempted"] is False
    assert result.report["final_rule_decision"] == "LONG"


def test_real_signal_with_paper_flags_follows_guarded_proof_path(tmp_path: Path) -> None:
    calls = Calls()
    proof_passed = strategy_result(
        tmp_path,
        verdict=TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_PASSED,
        decision="LONG",
        emitted=True,
        submit_requested=True,
        proof_invoked=True,
        broker_mutated=True,
        final_flat=True,
    )

    result = run_track_b_real_rule_wait(
        config=base_config(tmp_path, max_cycles=5),
        stages=stages(calls, [proof_passed]),
        runner_id="wait-proof-passed",
        now=aware_now(),
    )

    assert result.verdict == TrackBRealRuleWaitRunnerVerdict.PAPER_PROOF_PASSED
    assert calls.strategy == 1
    assert result.report["paper_proof_invoked"] is True
    assert result.report["submit_attempted"] is True
    assert result.report["broker_state_mutated"] is True
    assert result.report["mutation_attempted"] is True
    assert result.report["final_flat"] is True
    assert result.report["live_money_readiness"] is False


def test_demo_proof_signal_is_blocked_and_not_mistaken_for_real_rule(tmp_path: Path) -> None:
    calls = Calls()
    demo_signal = strategy_result(
        tmp_path,
        verdict=TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED,
        decision="LONG",
        emitted=True,
        real_strategy_signal=False,
    )

    result = run_track_b_real_rule_wait(
        config=base_config(tmp_path),
        stages=stages(calls, [demo_signal]),
        runner_id="wait-demo-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBRealRuleWaitRunnerVerdict.BLOCKED_NON_REAL_SIGNAL
    assert result.report["signal_source"] == "DEMO_WIRING_PROOF"
    assert result.report["real_strategy_signal"] is False
    assert result.report["paper_proof_invoked"] is False
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_live_money_path_is_prohibited(tmp_path: Path) -> None:
    paper = base_config(tmp_path).strategy_paper_config
    result = run_track_b_real_rule_wait(
        config=base_config(tmp_path, strategy_paper_config=replace(paper, mode="LIVE")),
        stages=stages(Calls(), []),
        runner_id="wait-live-blocked",
        now=aware_now(),
    )

    assert result.verdict == TrackBRealRuleWaitRunnerVerdict.BLOCKED_NON_PAPER_MODE
    assert result.report["cycles_attempted"] == 0
    assert result.report["submit_attempted"] is False
    assert result.report["live_money_readiness"] is False


def test_runtime_capture_invoked_each_cycle_when_source_supplied_and_bounded(tmp_path: Path) -> None:
    calls = Calls()
    source_json = tmp_path / "runtime_source.json"
    source_json.write_text(json.dumps({"account_id": "DUM882026", "contract_key": "MGC-202606", "candles": []}), encoding="utf-8")
    no_signal = strategy_result(
        tmp_path,
        verdict=TrackBStrategyPaperRunnerVerdict.NO_SIGNAL,
        decision="NO_SIGNAL",
        emitted=False,
    )

    result = run_track_b_real_rule_wait(
        config=base_config(tmp_path, runtime_candle_source_json=source_json, max_cycles=2),
        stages=stages(calls, [no_signal], capture=capture_result(tmp_path)),
        runner_id="wait-capture-bounded",
        now=aware_now(),
    )

    assert result.verdict == TrackBRealRuleWaitRunnerVerdict.NO_SIGNAL_NO_MUTATION
    assert calls.capture == 2
    assert calls.strategy == 2
    assert len(result.report["cycles"]) == 2
    assert calls.strategy_configs[0].rule_mode == "MGC_EMA_MOMENTUM_RECLAIM_LONG"
    assert calls.strategy_configs[0].emit_signal is True
    assert result.report["paper_proof_invoked"] is False
    assert result.report["live_money_readiness"] is False
