"""Bounded Track B real-rule wait runner.

This runner repeatedly evaluates the real MGC strategy-rule PAPER handoff path
without forcing a signal. It may refresh bounded runtime candle context from a
supplied payload, then delegates one cycle to ``track_b_strategy_paper_runner``.
Broker mutation remains possible only inside that delegated Track B PAPER proof
lifecycle, and only when the operator supplied the explicit PAPER submit flags.
"""

from __future__ import annotations

import json
import time
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_runtime_candle_capture import (
    DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT,
    TrackBRuntimeCandleCaptureResult,
    TrackBRuntimeCandleCaptureVerdict,
    capture_track_b_runtime_mgc_1m_candles,
)
from .track_b_strategy_paper_runner import (
    DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT,
    TrackBStrategyPaperRunnerConfig,
    TrackBStrategyPaperRunnerResult,
    TrackBStrategyPaperRunnerVerdict,
    run_track_b_strategy_paper,
)
from .track_b_strategy_rule_runner import DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID, TrackBStrategyRuleMode


DEFAULT_TRACK_B_REAL_RULE_WAIT_RUNNER_OUTPUT_ROOT = Path(
    "outputs/track_b_execution_core/track_b_real_rule_wait_runner"
)


class TrackBRealRuleWaitRunnerVerdict(str, Enum):
    NO_SIGNAL_NO_MUTATION = "TRACK_B_REAL_RULE_WAIT_NO_SIGNAL_NO_MUTATION"
    SIGNAL_READY_NO_SUBMIT = "TRACK_B_REAL_RULE_WAIT_SIGNAL_READY_NO_SUBMIT"
    SIGNAL_READY_FOR_EXPLICIT_PAPER_SUBMIT = "TRACK_B_REAL_RULE_WAIT_SIGNAL_READY_FOR_EXPLICIT_PAPER_SUBMIT"
    PAPER_PROOF_PASSED = "TRACK_B_REAL_RULE_WAIT_PAPER_PROOF_PASSED"
    REVIEW_REQUIRED = "TRACK_B_REAL_RULE_WAIT_REVIEW_REQUIRED"
    BLOCKED_NON_PAPER_MODE = "TRACK_B_REAL_RULE_WAIT_BLOCKED_NON_PAPER_MODE"
    BLOCKED_RUNTIME_CONTEXT = "TRACK_B_REAL_RULE_WAIT_BLOCKED_RUNTIME_CONTEXT"
    BLOCKED_NON_REAL_SIGNAL = "TRACK_B_REAL_RULE_WAIT_BLOCKED_NON_REAL_SIGNAL"
    BLOCKED_STRATEGY_OR_READINESS = "TRACK_B_REAL_RULE_WAIT_BLOCKED_STRATEGY_OR_READINESS"
    BLOCKED_INVALID_CONFIG = "TRACK_B_REAL_RULE_WAIT_BLOCKED_INVALID_CONFIG"


@dataclass(frozen=True)
class TrackBRealRuleWaitRunnerConfig:
    strategy_paper_config: TrackBStrategyPaperRunnerConfig
    runtime_candle_source_json: Path | None = None
    runtime_candle_source_payload: Mapping[str, Any] | None = None
    runtime_candle_context_json: Path | None = None
    max_cycles: int = 1
    poll_seconds: float = 0.0
    max_runtime_seconds: float | None = None
    runtime_candle_capture_output_root: Path = DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT
    runtime_candle_max_bars: int = 250
    runtime_candle_min_bars: int = 3
    runtime_candle_retention_runs: int = 5
    output_root: Path = DEFAULT_TRACK_B_REAL_RULE_WAIT_RUNNER_OUTPUT_ROOT
    source_id: str = "track_b_real_rule_wait_runner"


@dataclass(frozen=True)
class TrackBRealRuleWaitRunnerStages:
    runtime_candle_capture: Callable[
        [TrackBRealRuleWaitRunnerConfig, int, datetime],
        TrackBRuntimeCandleCaptureResult | None,
    ]
    strategy_paper_runner: Callable[
        [TrackBStrategyPaperRunnerConfig, str, datetime],
        TrackBStrategyPaperRunnerResult,
    ]
    sleep: Callable[[float], None]


@dataclass(frozen=True)
class TrackBRealRuleWaitRunnerResult:
    verdict: TrackBRealRuleWaitRunnerVerdict
    report_json: Path
    report: dict[str, Any]


def default_stages() -> TrackBRealRuleWaitRunnerStages:
    return TrackBRealRuleWaitRunnerStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        strategy_paper_runner=lambda config, runner_id, now: run_track_b_strategy_paper(
            config=config,
            runner_id=runner_id,
            now=now,
        ),
        sleep=time.sleep,
    )


def run_track_b_real_rule_wait(
    *,
    config: TrackBRealRuleWaitRunnerConfig,
    stages: TrackBRealRuleWaitRunnerStages | None = None,
    runner_id: str | None = None,
    now: datetime | None = None,
) -> TrackBRealRuleWaitRunnerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_runner_id = runner_id or f"track_b_real_rule_wait_runner_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / actual_runner_id / "track_b_real_rule_wait_runner_report.json"
    actual_stages = stages or default_stages()

    validation_error = _validation_error(config)
    if validation_error is not None:
        return _finalize(
            config=config,
            report_json=report_json,
            runner_id=actual_runner_id,
            now=actual_now,
            verdict=TrackBRealRuleWaitRunnerVerdict.BLOCKED_INVALID_CONFIG,
            cycles=[],
            primary_blocker=validation_error,
            required_next_action="Fix bounded wait runner configuration before retrying.",
        )
    if str(config.strategy_paper_config.mode).upper() != "PAPER":
        return _finalize(
            config=config,
            report_json=report_json,
            runner_id=actual_runner_id,
            now=actual_now,
            verdict=TrackBRealRuleWaitRunnerVerdict.BLOCKED_NON_PAPER_MODE,
            cycles=[],
            primary_blocker="Track B real-rule wait runner is PAPER-only. Live-money execution is prohibited.",
            required_next_action="Set --mode PAPER. No live-money path exists in this runner.",
        )

    cycles: list[dict[str, Any]] = []
    started_at = actual_now
    final_verdict = TrackBRealRuleWaitRunnerVerdict.NO_SIGNAL_NO_MUTATION
    primary_blocker: str | None = None
    required_next_action = "No real strategy signal was emitted during the bounded wait. No broker mutation was attempted."

    for cycle_index in range(1, config.max_cycles + 1):
        cycle_now = started_at + timedelta(seconds=(cycle_index - 1) * max(config.poll_seconds, 0.0))
        if config.max_runtime_seconds is not None and (cycle_now - started_at).total_seconds() > config.max_runtime_seconds:
            break

        capture = actual_stages.runtime_candle_capture(config, cycle_index, cycle_now)
        if capture is not None and capture.verdict != TrackBRuntimeCandleCaptureVerdict.WROTE_RUNTIME_CANDLES:
            cycle = _cycle_report(
                cycle_index=cycle_index,
                now=cycle_now,
                capture=capture,
                strategy_result=None,
                primary_blocker=str(capture.report.get("primary_blocker") or "Runtime candle context was not ready."),
            )
            cycles.append(cycle)
            final_verdict = TrackBRealRuleWaitRunnerVerdict.BLOCKED_RUNTIME_CONTEXT
            primary_blocker = str(cycle["primary_blocker"])
            required_next_action = "Resolve runtime candle capture/context blocker before continuing the real-rule wait."
            break

        runtime_context_json = _runtime_context_json(config, capture)
        paper_config = _cycle_strategy_config(config, runtime_context_json=runtime_context_json)
        strategy_result = actual_stages.strategy_paper_runner(
            paper_config,
            f"{actual_runner_id}_cycle_{cycle_index}",
            cycle_now,
        )
        cycle = _cycle_report(
            cycle_index=cycle_index,
            now=cycle_now,
            capture=capture,
            strategy_result=strategy_result,
            primary_blocker=strategy_result.report.get("primary_blocker"),
        )
        cycles.append(cycle)

        signal_emitted = strategy_result.report.get("signal_emitted") is True
        real_strategy_signal = strategy_result.report.get("real_strategy_signal") is True
        if signal_emitted and not real_strategy_signal:
            final_verdict = TrackBRealRuleWaitRunnerVerdict.BLOCKED_NON_REAL_SIGNAL
            primary_blocker = "A signal was emitted, but it was not labeled as a real strategy signal."
            required_next_action = "Use track_b_strategy_paper_runner directly for DEMO_WIRING_PROOF; this wait runner is real-rule only."
            break

        if signal_emitted:
            final_verdict, primary_blocker, required_next_action = _signal_verdict(strategy_result)
            break

        if strategy_result.verdict not in {
            TrackBStrategyPaperRunnerVerdict.NO_SIGNAL,
            TrackBStrategyPaperRunnerVerdict.HUMAN_REVIEW_NO_SIGNAL,
        }:
            final_verdict = TrackBRealRuleWaitRunnerVerdict.BLOCKED_STRATEGY_OR_READINESS
            primary_blocker = str(strategy_result.report.get("primary_blocker") or "Strategy paper runner blocked before a clean no-signal cycle.")
            required_next_action = str(strategy_result.report.get("required_next_action") or "Resolve strategy/readiness blocker before retrying.")
            break

        if cycle_index < config.max_cycles and config.poll_seconds > 0:
            actual_stages.sleep(config.poll_seconds)

    if not cycles:
        final_verdict = TrackBRealRuleWaitRunnerVerdict.NO_SIGNAL_NO_MUTATION
        required_next_action = "No cycles were attempted because max runtime elapsed before the first cycle. No broker mutation was attempted."

    return _finalize(
        config=config,
        report_json=report_json,
        runner_id=actual_runner_id,
        now=actual_now,
        verdict=final_verdict,
        cycles=cycles,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
    )


def _validation_error(config: TrackBRealRuleWaitRunnerConfig) -> str | None:
    if config.max_cycles <= 0:
        return "max_cycles must be positive."
    if config.poll_seconds < 0:
        return "poll_seconds must be non-negative."
    if config.max_runtime_seconds is not None and config.max_runtime_seconds < 0:
        return "max_runtime_seconds must be non-negative when supplied."
    if config.runtime_candle_source_json is None and config.runtime_candle_source_payload is None and config.runtime_candle_context_json is None:
        return "Provide --runtime-candle-context-json or --runtime-candle-source-json for bounded real-rule evaluation."
    return None


def _run_runtime_candle_capture(
    config: TrackBRealRuleWaitRunnerConfig,
    cycle_index: int,
    now: datetime,
) -> TrackBRuntimeCandleCaptureResult | None:
    if config.runtime_candle_source_json is None and config.runtime_candle_source_payload is None:
        return None
    payload = _runtime_candle_source_payload(config)
    return capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=payload,
        source_payload_path=config.runtime_candle_source_json,
        expected_account_id=config.strategy_paper_config.expected_account_id,
        account_id=config.strategy_paper_config.account_id,
        contract_key=config.strategy_paper_config.contract_key,
        local_symbol=config.strategy_paper_config.allowlisted_local_symbol,
        databento_continuous_symbol=config.strategy_paper_config.databento_continuous_symbol,
        dataset=config.strategy_paper_config.dataset,
        timeframe="1m",
        max_bars=config.runtime_candle_max_bars,
        min_bars=config.runtime_candle_min_bars,
        source_id=f"{config.source_id}_cycle_{cycle_index}",
        strategy_id=config.strategy_paper_config.strategy_id,
        lane_id=config.strategy_paper_config.lane_id,
        output_root=config.runtime_candle_capture_output_root,
        capture_id=f"{config.source_id}_runtime_capture_cycle_{cycle_index}_{uuid.uuid4().hex}",
        retention_runs=config.runtime_candle_retention_runs,
        now=now,
    )


def _runtime_candle_source_payload(config: TrackBRealRuleWaitRunnerConfig) -> Mapping[str, Any]:
    if config.runtime_candle_source_payload is not None:
        return config.runtime_candle_source_payload
    if config.runtime_candle_source_json is None:
        raise ValueError("runtime_candle_source_json or runtime_candle_source_payload is required.")
    payload = json.loads(Path(config.runtime_candle_source_json).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError("runtime candle source JSON must contain an object.")
    return payload


def _runtime_context_json(
    config: TrackBRealRuleWaitRunnerConfig,
    capture: TrackBRuntimeCandleCaptureResult | None,
) -> Path:
    if capture is not None and capture.runtime_candles_json is not None:
        return capture.runtime_candles_json
    if config.runtime_candle_context_json is not None:
        return config.runtime_candle_context_json
    raise ValueError("Runtime candle capture did not produce context JSON.")


def _cycle_strategy_config(
    config: TrackBRealRuleWaitRunnerConfig,
    *,
    runtime_context_json: Path,
) -> TrackBStrategyPaperRunnerConfig:
    return replace(
        config.strategy_paper_config,
        input_event_json=None,
        input_event_payload=None,
        runtime_candle_context_json=runtime_context_json,
        runtime_candle_context_payload=None,
        runtime_candle_context_required=True,
        rule_id=DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID,
        rule_mode=TrackBStrategyRuleMode.MGC_EMA_MOMENTUM_RECLAIM_LONG.value,
        emit_signal=True,
        allow_fixture_input=config.strategy_paper_config.allow_fixture_input,
    )


def _signal_verdict(
    strategy_result: TrackBStrategyPaperRunnerResult,
) -> tuple[TrackBRealRuleWaitRunnerVerdict, str | None, str]:
    report = strategy_result.report
    if strategy_result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_READY_NO_SUBMIT_REQUESTED:
        return (
            TrackBRealRuleWaitRunnerVerdict.SIGNAL_READY_NO_SUBMIT,
            None,
            "Real strategy signal was emitted and readiness was green, but explicit PAPER submit flags were not supplied. No broker mutation was attempted.",
        )
    if strategy_result.verdict == TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_PASSED:
        return (
            TrackBRealRuleWaitRunnerVerdict.PAPER_PROOF_PASSED,
            None,
            "Real strategy signal triggered explicit PAPER proof lifecycle, which passed with final flat broker state.",
        )
    if strategy_result.report.get("paper_submit_requested") is True and strategy_result.report.get("paper_proof_invoked") is False:
        return (
            TrackBRealRuleWaitRunnerVerdict.SIGNAL_READY_FOR_EXPLICIT_PAPER_SUBMIT,
            str(report.get("primary_blocker") or "Real signal occurred, but PAPER proof was not invoked."),
            str(report.get("required_next_action") or "Review explicit PAPER submit gates before retrying."),
        )
    if strategy_result.verdict in {
        TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE,
        TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED,
        TrackBStrategyPaperRunnerVerdict.PAPER_PROOF_BLOCKED,
    }:
        return (
            TrackBRealRuleWaitRunnerVerdict.REVIEW_REQUIRED,
            str(report.get("primary_blocker") or "Paper proof lifecycle requires review."),
            str(report.get("required_next_action") or "Review broker-state artifacts before any further PAPER submit."),
        )
    return (
        TrackBRealRuleWaitRunnerVerdict.BLOCKED_STRATEGY_OR_READINESS,
        str(report.get("primary_blocker") or "Real signal occurred, but strategy/readiness handoff did not complete."),
        str(report.get("required_next_action") or "Resolve strategy/readiness blocker before retrying."),
    )


def _cycle_report(
    *,
    cycle_index: int,
    now: datetime,
    capture: TrackBRuntimeCandleCaptureResult | None,
    strategy_result: TrackBStrategyPaperRunnerResult | None,
    primary_blocker: str | None,
) -> dict[str, Any]:
    strategy_report = strategy_result.report if strategy_result is not None else {}
    capture_report = capture.report if capture is not None else {}
    return {
        "cycle_index": cycle_index,
        "generated_at": now.isoformat(),
        "runtime_candle_capture_invoked": capture is not None,
        "runtime_candle_capture_verdict": capture_report.get("runtime_candle_capture_verdict"),
        "runtime_candle_capture_report_path": str(capture.report_json) if capture is not None else None,
        "runtime_candle_context_path": (
            str(capture.runtime_candles_json)
            if capture is not None and capture.runtime_candles_json is not None
            else strategy_report.get("runtime_candle_context_path")
        ),
        "runtime_candle_context_ready": (
            capture_report.get("runtime_candle_context_ready")
            if capture is not None
            else strategy_report.get("runtime_candle_context_ready")
        ),
        "feature_builder_invoked": strategy_report.get("feature_builder_invoked", False),
        "strategy_rule_evaluated": strategy_report.get("strategy_rule_evaluated", False),
        "signal_source": strategy_report.get("signal_source"),
        "real_strategy_signal": strategy_report.get("real_strategy_signal"),
        "rule_decision": strategy_report.get("rule_decision", "NOT_EVALUATED"),
        "signal_emitted": strategy_report.get("signal_emitted", False),
        "readiness_invoked": strategy_report.get("readiness_invoked", False),
        "paper_proof_invoked": strategy_report.get("paper_proof_invoked", False),
        "submit_attempted": strategy_report.get("submit_attempted", False),
        "broker_state_mutated": strategy_report.get("broker_state_mutated", False),
        "paper_submit_requested": strategy_report.get("paper_submit_requested", False),
        "paper_proof_classification": strategy_report.get("paper_proof_classification"),
        "final_flat": strategy_report.get("final_flat"),
        "primary_blocker": primary_blocker,
        "strategy_paper_runner_verdict": strategy_report.get("strategy_paper_runner_verdict"),
        "strategy_paper_runner_report_path": str(strategy_result.report_json) if strategy_result is not None else None,
    }


def _finalize(
    *,
    config: TrackBRealRuleWaitRunnerConfig,
    report_json: Path,
    runner_id: str,
    now: datetime,
    verdict: TrackBRealRuleWaitRunnerVerdict,
    cycles: list[dict[str, Any]],
    primary_blocker: str | None,
    required_next_action: str,
) -> TrackBRealRuleWaitRunnerResult:
    final_cycle = cycles[-1] if cycles else {}
    report = {
        "schema_version": "track_b_real_rule_wait_runner_v1",
        "generated_at": now.isoformat(),
        "track_b_real_rule_wait_runner_id": runner_id,
        "real_rule_wait_runner_verdict": verdict.value,
        "mode": config.strategy_paper_config.mode,
        "source_id": config.source_id,
        "strategy_id": config.strategy_paper_config.strategy_id,
        "lane_id": config.strategy_paper_config.lane_id,
        "rule_id": DEFAULT_MGC_EMA_MOMENTUM_RECLAIM_LONG_RULE_ID,
        "rule_mode": TrackBStrategyRuleMode.MGC_EMA_MOMENTUM_RECLAIM_LONG.value,
        "signal_source": final_cycle.get("signal_source") or "REAL_STRATEGY_RULE",
        "real_strategy_signal": final_cycle.get("real_strategy_signal") if final_cycle else True,
        "max_cycles": config.max_cycles,
        "poll_seconds": config.poll_seconds,
        "max_runtime_seconds": config.max_runtime_seconds,
        "cycles_attempted": len(cycles),
        "cycles": cycles,
        "final_rule_decision": final_cycle.get("rule_decision", "NOT_EVALUATED"),
        "ever_signal_emitted": any(cycle.get("signal_emitted") is True for cycle in cycles),
        "mutation_attempted": any(cycle.get("broker_state_mutated") is True or cycle.get("submit_attempted") is True for cycle in cycles),
        "paper_proof_invoked": any(cycle.get("paper_proof_invoked") is True for cycle in cycles),
        "submit_attempted": any(cycle.get("submit_attempted") is True for cycle in cycles),
        "broker_state_mutated": any(cycle.get("broker_state_mutated") is True for cycle in cycles),
        "final_paper_proof_classification": final_cycle.get("paper_proof_classification"),
        "final_flat": final_cycle.get("final_flat"),
        "primary_blocker": primary_blocker,
        "required_next_action": required_next_action,
        "submit_allowed": False,
        "live_money_readiness": False,
        "live_money_submit_allowed": False,
        "ui_authority": False,
        "hidden_submit": False,
        "paper_proof_cli_called": any(cycle.get("paper_proof_invoked") is True for cycle in cycles),
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_real_rule_wait_runner_report.json"),
    }
    _write_report(report_json, report)
    return TrackBRealRuleWaitRunnerResult(verdict=verdict, report_json=report_json, report=report)


def _write_report(report_json: Path, report: Mapping[str, Any]) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(dict(report)), indent=2, sort_keys=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
