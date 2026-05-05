"""Bounded Track B multi-strategy runtime cycle.

This boundary evaluates the registered Asia strategy adapters together and
arbitrates at most one PAPER candidate. It does not create a broker path: any
PAPER mutation is delegated to ``track_b_strategy_paper_runner`` and therefore
to the existing guarded Track B paper-proof lifecycle.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Callable, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_strategy_paper_runner import (
    DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT,
    TrackBStrategyPaperRunnerConfig,
    TrackBStrategyPaperRunnerResult,
    run_track_b_strategy_paper,
)
from .track_b_strategy_registry import (
    TrackBStrategyRegistryEntry,
    arbitrate_track_b_strategy_candidates,
    resolve_track_b_strategy_registry_entry,
)
from .track_b_strategy_rule_runner import (
    DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT,
    TrackBStrategyRuleRunnerResult,
    run_track_b_strategy_rule,
)


DEFAULT_TRACK_B_MULTI_STRATEGY_RUNTIME_CYCLE_OUTPUT_ROOT = Path(
    "outputs/track_b_execution_core/track_b_multi_strategy_runtime_cycle"
)


class TrackBMultiStrategyRuntimeCycleVerdict(str, Enum):
    NO_SIGNAL_NO_MUTATION = "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION"
    SIGNAL_READY_NO_SUBMIT = "TRACK_B_MULTI_STRATEGY_RUNTIME_SIGNAL_READY_NO_SUBMIT"
    ARBITRATION_BLOCKED = "TRACK_B_MULTI_STRATEGY_RUNTIME_ARBITRATION_BLOCKED"
    PAPER_PROOF_PASSED = "TRACK_B_MULTI_STRATEGY_RUNTIME_PAPER_PROOF_PASSED"
    PAPER_PROOF_REVIEW_REQUIRED = "TRACK_B_MULTI_STRATEGY_RUNTIME_PAPER_PROOF_REVIEW_REQUIRED"
    BLOCKED_STAGE_ERROR = "TRACK_B_MULTI_STRATEGY_RUNTIME_BLOCKED_STAGE_ERROR"


@dataclass(frozen=True)
class TrackBMultiStrategyInput:
    strategy_id: str
    rule_id: str
    rule_mode: str
    lane_id: str
    event_json: Path | None = None
    event_payload: Mapping[str, object] | None = None


@dataclass(frozen=True)
class TrackBMultiStrategyRuntimeCycleConfig:
    asian_drift_event_json: Path | None = None
    asian_drift_event_payload: Mapping[str, object] | None = None
    pause_resume_short_event_json: Path | None = None
    pause_resume_short_event_payload: Mapping[str, object] | None = None
    breakout_retest_hold_long_event_json: Path | None = None
    breakout_retest_hold_long_event_payload: Mapping[str, object] | None = None
    inbox_dir: Path = Path("examples/track_b_shadow_listener/inbox")
    expected_account_id: str = "DUM882026"
    source_id: str = "track_b_multi_strategy_runtime_cycle"
    allow_fixture_input: bool = False
    mode: str = "PAPER"
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 17086
    account_id: str = "DUM882026"
    contract_key: str = "MGC-202606"
    side: str = "BUY"
    quantity: int | None = None
    submit_paper: bool = False
    confirm_paper_submit: bool = False
    manual_open_limit_price: str | None = None
    manual_close_limit_price: str | None = None
    allowlisted_local_symbol: str = "MGCM6"
    con_id: int | None = 712565978
    proof_timing_status: str = "ACTIVE_SESSION"
    proof_timing_source: str = "track_b_multi_strategy_runtime_cycle"
    output_root: Path = DEFAULT_TRACK_B_MULTI_STRATEGY_RUNTIME_CYCLE_OUTPUT_ROOT
    strategy_rule_output_root: Path = DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT
    strategy_paper_runner_output_root: Path = DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT


@dataclass(frozen=True)
class TrackBMultiStrategyRuntimeCycleStages:
    strategy_rule: Callable[[TrackBMultiStrategyInput, TrackBMultiStrategyRuntimeCycleConfig], TrackBStrategyRuleRunnerResult]
    paper_runner: Callable[
        [TrackBMultiStrategyRuntimeCycleConfig, TrackBMultiStrategyInput, Mapping[str, object]],
        TrackBStrategyPaperRunnerResult,
    ]


@dataclass(frozen=True)
class TrackBMultiStrategyRuntimeCycleResult:
    verdict: TrackBMultiStrategyRuntimeCycleVerdict
    report_json: Path
    report: dict[str, object]
    strategy_results: tuple[TrackBStrategyRuleRunnerResult, ...]
    paper_runner_result: TrackBStrategyPaperRunnerResult | None = None


def default_stages() -> TrackBMultiStrategyRuntimeCycleStages:
    return TrackBMultiStrategyRuntimeCycleStages(
        strategy_rule=_run_strategy_rule,
        paper_runner=_run_strategy_paper_runner,
    )


def run_track_b_multi_strategy_runtime_cycle(
    *,
    config: TrackBMultiStrategyRuntimeCycleConfig,
    stages: TrackBMultiStrategyRuntimeCycleStages | None = None,
    cycle_id: str | None = None,
    now: datetime | None = None,
) -> TrackBMultiStrategyRuntimeCycleResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_cycle_id = cycle_id or f"track_b_multi_strategy_runtime_cycle_{uuid.uuid4().hex}"
    report_json = Path(config.output_root) / actual_cycle_id / "track_b_multi_strategy_runtime_cycle_report.json"
    actual_stages = stages or default_stages()
    strategy_results: list[TrackBStrategyRuleRunnerResult] = []
    paper_result: TrackBStrategyPaperRunnerResult | None = None

    try:
        strategy_reports: list[dict[str, object]] = []
        candidate_signals: list[dict[str, object]] = []
        for strategy_input in _strategy_inputs(config):
            if strategy_input.event_payload is None and strategy_input.event_json is None:
                strategy_reports.append(_missing_strategy_report(strategy_input))
                continue
            result = actual_stages.strategy_rule(strategy_input, config)
            strategy_results.append(result)
            summary = _strategy_summary(result.report)
            strategy_reports.append(summary)
            if _is_real_signal_candidate(result.report):
                candidate_signals.append(_candidate_from_report(result.report))

        arbitration = arbitrate_track_b_strategy_candidates(candidate_signals)
        chosen_signal = arbitration.get("chosen_candidate") if isinstance(arbitration.get("chosen_candidate"), Mapping) else None
        paper_submit_requested = _paper_submit_requested(config)
        primary_blocker = arbitration.get("primary_blocker")
        required_next_action = str(arbitration.get("required_next_action") or "Continue bounded no-submit watch.")

        if not candidate_signals:
            verdict = TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION
            required_next_action = "No registered Asia strategy emitted a real signal; no readiness or broker mutation was attempted."
        elif arbitration.get("strategy_arbitration_verdict") != "TRACK_B_STRATEGY_REGISTRY_READY":
            verdict = TrackBMultiStrategyRuntimeCycleVerdict.ARBITRATION_BLOCKED
            primary_blocker = primary_blocker or "Track B strategy arbitration did not choose exactly one PAPER candidate."
        elif not paper_submit_requested:
            verdict = TrackBMultiStrategyRuntimeCycleVerdict.SIGNAL_READY_NO_SUBMIT
            required_next_action = "Exactly one real strategy signal is ready, but explicit PAPER submit flags were not supplied."
        else:
            chosen_input = _input_for_candidate(config, chosen_signal)
            if chosen_input is None:
                verdict = TrackBMultiStrategyRuntimeCycleVerdict.ARBITRATION_BLOCKED
                primary_blocker = "Chosen strategy candidate did not map back to a cycle input envelope."
                required_next_action = "Review multi-strategy cycle artifacts before any PAPER retry."
            else:
                paper_result = actual_stages.paper_runner(config, chosen_input, chosen_signal or {})
                verdict = (
                    TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_PASSED
                    if paper_result.report.get("strategy_paper_runner_verdict") == "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_PASSED"
                    else TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_REVIEW_REQUIRED
                )
                primary_blocker = paper_result.report.get("primary_blocker")
                required_next_action = str(paper_result.report.get("required_next_action") or "Review strategy PAPER runner report.")

        report = _build_report(
            config=config,
            now=actual_now,
            cycle_id=actual_cycle_id,
            report_json=report_json,
            verdict=verdict,
            strategy_reports=strategy_reports,
            candidate_signals=candidate_signals,
            arbitration=arbitration,
            paper_result=paper_result,
            primary_blocker=primary_blocker,
            required_next_action=required_next_action,
        )
        _write_report(report_json, report)
        return TrackBMultiStrategyRuntimeCycleResult(
            verdict=verdict,
            report_json=report_json,
            report=report,
            strategy_results=tuple(strategy_results),
            paper_runner_result=paper_result,
        )
    except Exception as exc:  # noqa: BLE001 - runtime cycle errors must become artifacts.
        report = _build_report(
            config=config,
            now=actual_now,
            cycle_id=actual_cycle_id,
            report_json=report_json,
            verdict=TrackBMultiStrategyRuntimeCycleVerdict.BLOCKED_STAGE_ERROR,
            strategy_reports=[],
            candidate_signals=[],
            arbitration={},
            paper_result=paper_result,
            primary_blocker=f"Track B multi-strategy runtime cycle stage error: {exc}",
            required_next_action="Review multi-strategy cycle diagnostics before retrying.",
        )
        _write_report(report_json, report)
        return TrackBMultiStrategyRuntimeCycleResult(
            verdict=TrackBMultiStrategyRuntimeCycleVerdict.BLOCKED_STAGE_ERROR,
            report_json=report_json,
            report=report,
            strategy_results=tuple(strategy_results),
            paper_runner_result=paper_result,
        )


def _run_strategy_rule(
    strategy_input: TrackBMultiStrategyInput,
    config: TrackBMultiStrategyRuntimeCycleConfig,
) -> TrackBStrategyRuleRunnerResult:
    payload = _payload_for_strategy_input(strategy_input)
    return run_track_b_strategy_rule(
        input_event_payload=payload,
        input_event_path=strategy_input.event_json,
        inbox_dir=config.inbox_dir,
        expected_account_id=config.expected_account_id,
        source_id=f"{config.source_id}_{strategy_input.rule_mode.lower()}",
        strategy_id=strategy_input.strategy_id,
        lane_id=strategy_input.lane_id,
        rule_id=strategy_input.rule_id,
        rule_mode=strategy_input.rule_mode,
        emit_signal=True,
        allow_fixture_input=config.allow_fixture_input,
        output_root=config.strategy_rule_output_root,
    )


def _run_strategy_paper_runner(
    config: TrackBMultiStrategyRuntimeCycleConfig,
    strategy_input: TrackBMultiStrategyInput,
    chosen_signal: Mapping[str, object],
) -> TrackBStrategyPaperRunnerResult:
    return run_track_b_strategy_paper(
        config=TrackBStrategyPaperRunnerConfig(
            mode=config.mode,
            input_event_json=strategy_input.event_json,
            input_event_payload=strategy_input.event_payload,
            inbox_dir=config.inbox_dir,
            source_id=f"{config.source_id}_paper_handoff",
            strategy_id=strategy_input.strategy_id,
            lane_id=strategy_input.lane_id,
            rule_id=strategy_input.rule_id,
            rule_mode=strategy_input.rule_mode,
            emit_signal=True,
            allow_fixture_input=config.allow_fixture_input,
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            account_id=config.account_id,
            expected_account_id=config.expected_account_id,
            contract_key=config.contract_key,
            side=config.side,
            quantity=config.quantity,
            submit_paper=config.submit_paper,
            confirm_paper_submit=config.confirm_paper_submit,
            manual_open_limit_price=config.manual_open_limit_price,
            manual_close_limit_price=config.manual_close_limit_price,
            allowlisted_local_symbol=config.allowlisted_local_symbol,
            con_id=config.con_id,
            proof_timing_status=config.proof_timing_status,
            proof_timing_source=config.proof_timing_source,
            output_root=config.strategy_paper_runner_output_root,
        )
    )


def _strategy_inputs(config: TrackBMultiStrategyRuntimeCycleConfig) -> tuple[TrackBMultiStrategyInput, ...]:
    return (
        TrackBMultiStrategyInput(
            strategy_id="asian_drift_v1",
            rule_id="asian_drift_v1",
            rule_mode="ASIAN_DRIFT_V1",
            lane_id="mgc_example_long_lmt_day",
            event_json=config.asian_drift_event_json,
            event_payload=config.asian_drift_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_id="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            rule_mode="ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
            lane_id="mgc_asia_early_pause_resume_short",
            event_json=config.pause_resume_short_event_json,
            event_payload=config.pause_resume_short_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_id="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            rule_mode="ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
            lane_id="mgc_asia_early_normal_breakout_retest_hold_long",
            event_json=config.breakout_retest_hold_long_event_json,
            event_payload=config.breakout_retest_hold_long_event_payload,
        ),
    )


def _payload_for_strategy_input(strategy_input: TrackBMultiStrategyInput) -> Mapping[str, object]:
    if strategy_input.event_payload is not None:
        return strategy_input.event_payload
    if strategy_input.event_json is None:
        raise ValueError(f"{strategy_input.strategy_id} input envelope was not supplied.")
    payload = json.loads(Path(strategy_input.event_json).read_text(encoding="utf-8"))
    if not isinstance(payload, Mapping):
        raise ValueError(f"{strategy_input.strategy_id} input JSON must contain an object.")
    return payload


def _missing_strategy_report(strategy_input: TrackBMultiStrategyInput) -> dict[str, object]:
    entry = resolve_track_b_strategy_registry_entry(
        rule_mode=strategy_input.rule_mode,
        rule_id=strategy_input.rule_id,
        strategy_id=strategy_input.strategy_id,
    )
    metadata = entry.report_metadata() if isinstance(entry, TrackBStrategyRegistryEntry) else {}
    return {
        **metadata,
        "strategy_id": strategy_input.strategy_id,
        "rule_id": strategy_input.rule_id,
        "rule_mode": strategy_input.rule_mode,
        "strategy_rule_runner_verdict": "NOT_INVOKED",
        "strategy_runtime_verdict": "NOT_READY",
        "signal_source": _signal_source_for_strategy(strategy_input.strategy_id),
        "real_strategy_signal": False,
        "signal_emitted": False,
        "signal_direction": None,
        "paper_eligible": metadata.get("strategy_registry_paper_eligible", False),
        "live_money_eligible": metadata.get("strategy_registry_live_money_eligible", False),
        "primary_blocker": f"{strategy_input.strategy_id} input envelope was not supplied.",
        "required_next_action": "Provide the explicit strategy feature/state envelope before runtime arbitration.",
    }


def _strategy_summary(report: Mapping[str, object]) -> dict[str, object]:
    return {
        "strategy_id": report.get("strategy_registry_id") or report.get("strategy_id"),
        "rule_id": report.get("strategy_registry_rule_id") or report.get("strategy_rule_id"),
        "rule_mode": report.get("strategy_registry_rule_mode") or report.get("rule_mode"),
        "strategy_rule_runner_verdict": report.get("strategy_rule_runner_verdict"),
        "strategy_runtime_verdict": _strategy_runtime_verdict(report),
        "registry_metadata": {
            "strategy_registry_id": report.get("strategy_registry_id"),
            "strategy_registry_rule_mode": report.get("strategy_registry_rule_mode"),
            "strategy_registry_instrument_family": report.get("strategy_registry_instrument_family"),
            "strategy_registry_timeframe": report.get("strategy_registry_timeframe"),
            "strategy_registry_feature_version": report.get("strategy_registry_feature_version"),
            "strategy_registry_calibration_profile": report.get("strategy_registry_calibration_profile"),
            "strategy_registry_paper_eligible": report.get("strategy_registry_paper_eligible"),
            "strategy_registry_live_money_eligible": report.get("strategy_registry_live_money_eligible"),
        },
        "signal_source": report.get("signal_source"),
        "real_strategy_signal": report.get("real_strategy_signal"),
        "decision": report.get("decision"),
        "signal_emitted": report.get("signal_emitted"),
        "signal_direction": report.get("signal_direction"),
        "paper_eligible": report.get("strategy_registry_paper_eligible"),
        "live_money_eligible": report.get("strategy_registry_live_money_eligible"),
        "primary_blocker": report.get("primary_blocker"),
        "required_next_action": report.get("required_next_action"),
        "report_json_path": report.get("report_json_path"),
    }


def _strategy_runtime_verdict(report: Mapping[str, object]) -> str:
    for key in (
        "asian_drift_watch_verdict",
        "asia_early_pause_resume_short_watch_verdict",
        "asia_early_normal_breakout_retest_hold_long_watch_verdict",
    ):
        value = report.get(key)
        if value:
            return str(value)
    if report.get("signal_emitted") is True:
        return "SIGNAL_READY_NO_SUBMIT"
    if str(report.get("strategy_rule_runner_verdict") or "").startswith("TRACK_B_STRATEGY_RULE_RUNNER_BLOCKED"):
        return "NOT_READY"
    return "NO_SIGNAL_NO_MUTATION"


def _is_real_signal_candidate(report: Mapping[str, object]) -> bool:
    if report.get("signal_emitted") is not True:
        return False
    if report.get("real_strategy_signal") is not True:
        return False
    if report.get("strategy_registry_live_money_eligible") is not False:
        return False
    return report.get("signal_source") == _signal_source_for_strategy(str(report.get("strategy_registry_id") or report.get("strategy_id") or ""))


def _candidate_from_report(report: Mapping[str, object]) -> dict[str, object]:
    return {
        "strategy_id": report.get("strategy_registry_id") or report.get("strategy_id"),
        "rule_mode": report.get("strategy_registry_rule_mode") or report.get("rule_mode"),
        "signal_source": report.get("signal_source"),
        "real_strategy_signal": report.get("real_strategy_signal"),
        "signal_emitted": report.get("signal_emitted"),
        "signal_direction": report.get("signal_direction"),
        "decision": report.get("decision"),
        "paper_eligible": report.get("strategy_registry_paper_eligible"),
        "live_money_eligible": report.get("strategy_registry_live_money_eligible"),
        "strategy_rule_report_path": report.get("report_json_path"),
    }


def _input_for_candidate(
    config: TrackBMultiStrategyRuntimeCycleConfig,
    candidate: Mapping[str, object] | None,
) -> TrackBMultiStrategyInput | None:
    if candidate is None:
        return None
    strategy_id = str(candidate.get("strategy_id") or "")
    for strategy_input in _strategy_inputs(config):
        if strategy_input.strategy_id == strategy_id:
            return strategy_input
    return None


def _signal_source_for_strategy(strategy_id: str) -> str:
    if strategy_id == "asian_drift_v1":
        return "ASIAN_DRIFT_V1"
    if strategy_id == "ASIA_EARLY_PAUSE_RESUME_SHORT_V1":
        return "ASIA_EARLY_PAUSE_RESUME_SHORT_V1"
    if strategy_id == "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1":
        return "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1"
    return "UNKNOWN"


def _paper_submit_requested(config: TrackBMultiStrategyRuntimeCycleConfig) -> bool:
    return bool(config.submit_paper and config.confirm_paper_submit)


def _build_report(
    *,
    config: TrackBMultiStrategyRuntimeCycleConfig,
    now: datetime,
    cycle_id: str,
    report_json: Path,
    verdict: TrackBMultiStrategyRuntimeCycleVerdict,
    strategy_reports: list[dict[str, object]],
    candidate_signals: list[dict[str, object]],
    arbitration: Mapping[str, object],
    paper_result: TrackBStrategyPaperRunnerResult | None,
    primary_blocker: object | None,
    required_next_action: str,
) -> dict[str, object]:
    paper_report = paper_result.report if paper_result is not None else {}
    chosen_signal = arbitration.get("chosen_candidate") if isinstance(arbitration.get("chosen_candidate"), Mapping) else None
    return {
        "schema_version": "track_b_multi_strategy_runtime_cycle_v1",
        "generated_at": now.isoformat(),
        "track_b_multi_strategy_runtime_cycle_id": cycle_id,
        "multi_strategy_runtime_cycle_verdict": verdict.value,
        "mode": config.mode,
        "source_id": config.source_id,
        "evaluated_strategies": strategy_reports,
        "candidate_signals": candidate_signals,
        "suppressed_signals": arbitration.get("suppressed_candidates") or [],
        "arbitration_result": arbitration,
        "chosen_signal": chosen_signal,
        "chosen_strategy_id": chosen_signal.get("strategy_id") if isinstance(chosen_signal, Mapping) else None,
        "reason_no_signal_chosen": _reason_no_signal_chosen(verdict, arbitration, primary_blocker),
        "paper_submit_requested": _paper_submit_requested(config),
        "readiness_invoked": bool(paper_report.get("readiness_invoked")) if paper_report else False,
        "paper_proof_invoked": bool(paper_report.get("paper_proof_invoked")) if paper_report else False,
        "submit_attempted": bool(paper_report.get("submit_attempted")) if paper_report else False,
        "broker_state_mutated": bool(paper_report.get("broker_state_mutated")) if paper_report else False,
        "paper_runner_report_path": str(paper_result.report_json) if paper_result else None,
        "paper_runner_verdict": paper_report.get("strategy_paper_runner_verdict"),
        "paper_proof_classification": paper_report.get("paper_proof_classification"),
        "final_broker_state_classification": paper_report.get("paper_proof_lifecycle_status"),
        "final_flat": paper_report.get("final_flat"),
        "primary_blocker": None if primary_blocker is None else str(primary_blocker),
        "required_next_action": required_next_action,
        "live_money_readiness": False,
        "live_money_submit_allowed": False,
        "ui_authority": False,
        "hidden_submit": False,
        "direct_broker_path": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_multi_strategy_runtime_cycle_report.json"),
    }


def _reason_no_signal_chosen(
    verdict: TrackBMultiStrategyRuntimeCycleVerdict,
    arbitration: Mapping[str, object],
    primary_blocker: object | None,
) -> str | None:
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION:
        return "No registered Asia strategy emitted a real signal."
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.SIGNAL_READY_NO_SUBMIT:
        return "One signal was chosen, but explicit PAPER submit flags were not supplied."
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.ARBITRATION_BLOCKED:
        return str(primary_blocker or arbitration.get("primary_blocker") or "Strategy arbitration did not choose a signal.")
    return None


def _write_report(report_json: Path, report: dict[str, object]) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
