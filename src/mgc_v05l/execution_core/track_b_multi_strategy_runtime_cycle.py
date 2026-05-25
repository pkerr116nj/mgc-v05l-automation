"""Bounded Track B multi-strategy runtime cycle.

This boundary evaluates the registered strategy adapters together and
arbitrates at most one PAPER candidate. It does not create a direct broker
path: any PAPER mutation is delegated to ``track_b_strategy_paper_runner`` and
therefore to the guarded Track B strategy-managed PAPER lifecycle.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping

from .models import require_aware_datetime, to_jsonable
from .operator_status import DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT, OperatorStatusInputs, create_operator_status_summary
from .track_b_decision_journal import record_track_b_decision_journal_cycle
from .track_b_paper_autonomous_recovery_planner import DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
from .track_b_pre_action_snapshot_validator import DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
from .track_b_runtime_safe_state_envelope import DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
from .track_b_runtime_supervisor_authority import DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
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
REPO_ROOT = Path(__file__).resolve().parents[3]

MULTI_STRATEGY_CYCLE_AUTHORIZED = "MULTI_STRATEGY_CYCLE_AUTHORIZED"
MULTI_STRATEGY_CYCLE_OBSERVE_ONLY = "MULTI_STRATEGY_CYCLE_OBSERVE_ONLY"
MULTI_STRATEGY_CYCLE_BLOCKED_NO_SNAPSHOT = "MULTI_STRATEGY_CYCLE_BLOCKED_NO_SNAPSHOT"
MULTI_STRATEGY_CYCLE_BLOCKED_SAFE_STATE = "MULTI_STRATEGY_CYCLE_BLOCKED_SAFE_STATE"
MULTI_STRATEGY_CYCLE_BLOCKED_RUNTIME_GENERATION = "MULTI_STRATEGY_CYCLE_BLOCKED_RUNTIME_GENERATION"
MULTI_STRATEGY_CYCLE_BLOCKED_LIVE_MONEY = "MULTI_STRATEGY_CYCLE_BLOCKED_LIVE_MONEY"
MULTI_STRATEGY_CYCLE_BLOCKED_PAPER_PROOF = "MULTI_STRATEGY_CYCLE_BLOCKED_PAPER_PROOF"


class TrackBMultiStrategyRuntimeCycleVerdict(str, Enum):
    NO_SIGNAL_NO_MUTATION = "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION"
    SIGNAL_READY_NO_SUBMIT = "TRACK_B_MULTI_STRATEGY_RUNTIME_SIGNAL_READY_NO_SUBMIT"
    ARBITRATION_BLOCKED = "TRACK_B_MULTI_STRATEGY_RUNTIME_ARBITRATION_BLOCKED"
    PAPER_PROOF_PASSED = "TRACK_B_MULTI_STRATEGY_RUNTIME_PAPER_PROOF_PASSED"
    PAPER_PROOF_REVIEW_REQUIRED = "TRACK_B_MULTI_STRATEGY_RUNTIME_PAPER_PROOF_REVIEW_REQUIRED"
    STRATEGY_MANAGED_OPEN_MANAGED = "TRACK_B_MULTI_STRATEGY_RUNTIME_STRATEGY_MANAGED_OPEN_MANAGED"
    STRATEGY_MANAGED_CLOSED_FLAT = "TRACK_B_MULTI_STRATEGY_RUNTIME_STRATEGY_MANAGED_CLOSED_FLAT"
    STRATEGY_MANAGED_REVIEW_REQUIRED = "TRACK_B_MULTI_STRATEGY_RUNTIME_STRATEGY_MANAGED_REVIEW_REQUIRED"
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
    enabled_strategy_ids: tuple[str, ...] = ()
    asian_drift_event_json: Path | None = None
    asian_drift_event_payload: Mapping[str, object] | None = None
    pause_resume_short_event_json: Path | None = None
    pause_resume_short_event_payload: Mapping[str, object] | None = None
    breakout_retest_hold_long_event_json: Path | None = None
    breakout_retest_hold_long_event_payload: Mapping[str, object] | None = None
    first_bull_snap_turn_event_json: Path | None = None
    first_bull_snap_turn_event_payload: Mapping[str, object] | None = None
    first_bear_snap_turn_event_json: Path | None = None
    first_bear_snap_turn_event_payload: Mapping[str, object] | None = None
    london_late_pause_resume_short_event_json: Path | None = None
    london_late_pause_resume_short_event_payload: Mapping[str, object] | None = None
    asia_late_flat_pullback_pause_resume_long_event_json: Path | None = None
    asia_late_flat_pullback_pause_resume_long_event_payload: Mapping[str, object] | None = None
    us_derivative_bear_turn_event_json: Path | None = None
    us_derivative_bear_turn_event_payload: Mapping[str, object] | None = None
    mnq_us_derivative_bear_turn_event_json: Path | None = None
    mnq_us_derivative_bear_turn_event_payload: Mapping[str, object] | None = None
    mnq_first_bear_snap_turn_event_json: Path | None = None
    mnq_first_bear_snap_turn_event_payload: Mapping[str, object] | None = None
    mnq_first_bull_snap_turn_event_json: Path | None = None
    mnq_first_bull_snap_turn_event_payload: Mapping[str, object] | None = None
    us_late_pause_resume_long_event_json: Path | None = None
    us_late_pause_resume_long_event_payload: Mapping[str, object] | None = None
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
    paper_execution_path: str = "STRATEGY_MANAGED"
    managed_exit_policy_id: str | None = None
    paper_order_pricing_policy: str = "MANUAL_LIMIT_PRICES"
    paper_order_price_offset_ticks: int = 2
    paper_exit_price_offset_ticks: int = 2
    pricing_context_json: Path | None = None
    pricing_context_payload: Mapping[str, object] | None = None
    live_quote_report_json: Path | None = None
    live_quote_report_payload: Mapping[str, object] | None = None
    max_pricing_context_age_seconds: int = 120
    tick_size: str = "0.1"
    allowlisted_local_symbol: str = "MGCM6"
    con_id: int | None = 712565978
    proof_timing_status: str = "ACTIVE_SESSION"
    proof_timing_source: str = "track_b_multi_strategy_runtime_cycle"
    output_root: Path = DEFAULT_TRACK_B_MULTI_STRATEGY_RUNTIME_CYCLE_OUTPUT_ROOT
    strategy_rule_output_root: Path = DEFAULT_TRACK_B_STRATEGY_RULE_RUNNER_OUTPUT_ROOT
    strategy_paper_runner_output_root: Path = DEFAULT_TRACK_B_STRATEGY_PAPER_RUNNER_OUTPUT_ROOT
    decision_journal_enabled: bool = True
    decision_journal_output_root: Path | None = None
    update_operator_status: bool = False
    operator_status_output_root: Path = DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT
    backend_health_json: Path | None = Path("outputs/operator_dashboard/runtime/operator_dashboard_readiness.json")
    repo_root: Path = REPO_ROOT
    runtime_generation_id: str | None = None
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    autonomous_recovery_plan_path: Path = DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    runtime_safe_state_envelope_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    control_plane_snapshot_max_age_seconds: int = 300
    expected_control_plane_snapshot_id: str | None = None
    expected_shared_truth_generation_id: str | None = None


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
    paper_order_parameters: dict[str, object] = _empty_paper_order_parameters(config)
    cycle_authority: dict[str, object] = build_track_b_multi_strategy_cycle_authority(
        config=config,
        submit_requested=False,
        now=actual_now,
    )

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
        cycle_authority = build_track_b_multi_strategy_cycle_authority(
            config=config,
            submit_requested=paper_submit_requested,
            now=actual_now,
        )

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
                paper_order_parameters = _resolve_paper_order_parameters(config, chosen_signal or {}, actual_now)
                if paper_order_parameters.get("paper_order_parameter_blocker"):
                    verdict = TrackBMultiStrategyRuntimeCycleVerdict.ARBITRATION_BLOCKED
                    primary_blocker = paper_order_parameters.get("paper_order_parameter_blocker")
                    required_next_action = (
                        "Resolve Track B PAPER order pricing context before any guarded PAPER lifecycle handoff."
                    )
                elif cycle_authority.get("authorization_classification") != MULTI_STRATEGY_CYCLE_AUTHORIZED:
                    verdict = TrackBMultiStrategyRuntimeCycleVerdict.SIGNAL_READY_NO_SUBMIT
                    primary_blocker = cycle_authority.get("reason") or "Multi-strategy cycle submit authority blocked handoff."
                    required_next_action = (
                        "Cycle-level Control Plane Snapshot / Safe-State authority blocked PAPER submit delegation; "
                        "continue observe/evaluate mode and refresh shared control-plane evidence before retry."
                    )
                else:
                    paper_config = replace(
                        config,
                        manual_open_limit_price=str(paper_order_parameters["open_limit_price"]),
                        manual_close_limit_price=str(paper_order_parameters["close_limit_price"]),
                        runtime_generation_id=str(cycle_authority.get("runtime_generation_id") or ""),
                        control_plane_snapshot_path=Path(str(cycle_authority["source_artifact_paths"]["control_plane_snapshot"])),
                        autonomous_recovery_plan_path=Path(str(cycle_authority["source_artifact_paths"]["autonomous_recovery_plan"])),
                        runtime_supervisor_authority_path=Path(str(cycle_authority["source_artifact_paths"]["runtime_supervisor_authority"])),
                        runtime_safe_state_envelope_path=Path(str(cycle_authority["source_artifact_paths"]["runtime_safe_state_envelope"])),
                        expected_control_plane_snapshot_id=str(cycle_authority.get("control_plane_snapshot_id") or ""),
                        expected_shared_truth_generation_id=str(cycle_authority.get("shared_truth_generation_id") or ""),
                    )
                    paper_result = actual_stages.paper_runner(paper_config, chosen_input, chosen_signal or {})
                    paper_order_parameters["paper_runner_config_open_limit_price"] = paper_config.manual_open_limit_price
                    paper_order_parameters["paper_runner_config_close_limit_price"] = paper_config.manual_close_limit_price
                    verdict = _paper_result_cycle_verdict(paper_result)
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
            paper_order_parameters=paper_order_parameters,
            cycle_authority=cycle_authority,
            primary_blocker=primary_blocker,
            required_next_action=required_next_action,
        )
        _write_report(report_json, report)
        report.update(_maybe_record_decision_journal(config, report, report_json, actual_now))
        _write_report(report_json, report)
        report.update(_maybe_update_operator_status(config, report))
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
            paper_order_parameters=paper_order_parameters,
            cycle_authority=cycle_authority,
            primary_blocker=f"Track B multi-strategy runtime cycle stage error: {exc}",
            required_next_action="Review multi-strategy cycle diagnostics before retrying.",
        )
        _write_report(report_json, report)
        report.update(_maybe_record_decision_journal(config, report, report_json, actual_now))
        _write_report(report_json, report)
        report.update(_maybe_update_operator_status(config, report))
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


def _paper_result_cycle_verdict(
    paper_result: TrackBStrategyPaperRunnerResult,
) -> TrackBMultiStrategyRuntimeCycleVerdict:
    runner_verdict = str(paper_result.report.get("strategy_paper_runner_verdict") or "")
    if runner_verdict == "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_PASSED":
        return TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_PASSED
    if runner_verdict == "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_OPEN_MANAGED":
        return TrackBMultiStrategyRuntimeCycleVerdict.STRATEGY_MANAGED_OPEN_MANAGED
    if runner_verdict == "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_CLOSED_FLAT":
        return TrackBMultiStrategyRuntimeCycleVerdict.STRATEGY_MANAGED_CLOSED_FLAT
    if runner_verdict in {
        "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_EXIT_PENDING",
        "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_REVIEW_REQUIRED",
        "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_LIFECYCLE_NOT_AVAILABLE",
        "TRACK_B_STRATEGY_PAPER_RUNNER_STRATEGY_MANAGED_EXIT_POLICY_MISSING",
    }:
        return TrackBMultiStrategyRuntimeCycleVerdict.STRATEGY_MANAGED_REVIEW_REQUIRED
    return TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_REVIEW_REQUIRED


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
            side=_paper_side_for_chosen_signal(config.side, chosen_signal),
            quantity=config.quantity,
            submit_paper=config.submit_paper,
            confirm_paper_submit=config.confirm_paper_submit,
            manual_open_limit_price=config.manual_open_limit_price,
            manual_close_limit_price=config.manual_close_limit_price,
            paper_execution_path=config.paper_execution_path,
            managed_exit_policy_id=config.managed_exit_policy_id,
            runtime_decision_source="DATABENTO_LIVE_ARTIFACT",
            paper_order_pricing_policy=config.paper_order_pricing_policy,
            allowlisted_local_symbol=config.allowlisted_local_symbol,
            con_id=config.con_id,
            tick_size=config.tick_size,
            exchange="CME" if str(config.contract_key).startswith(("MNQ-", "NQ-", "MES-", "ES-")) else "COMEX",
            databento_continuous_symbol="MNQ.v.0" if str(config.contract_key).startswith("MNQ-") else "MGC.v.0",
            dataset="GLBX.MDP3",
            proof_timing_status=config.proof_timing_status,
            proof_timing_source=config.proof_timing_source,
            output_root=config.strategy_paper_runner_output_root,
            repo_root=config.repo_root,
            runtime_generation_id=config.runtime_generation_id,
            control_plane_snapshot_path=config.control_plane_snapshot_path,
            autonomous_recovery_plan_path=config.autonomous_recovery_plan_path,
            runtime_supervisor_authority_path=config.runtime_supervisor_authority_path,
            runtime_safe_state_envelope_path=config.runtime_safe_state_envelope_path,
            expected_control_plane_snapshot_id=config.expected_control_plane_snapshot_id,
            expected_shared_truth_generation_id=config.expected_shared_truth_generation_id,
        )
    )


def _strategy_inputs(config: TrackBMultiStrategyRuntimeCycleConfig) -> tuple[TrackBMultiStrategyInput, ...]:
    inputs = (
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
        TrackBMultiStrategyInput(
            strategy_id="FIRST_BULL_SNAP_TURN_V1",
            rule_id="FIRST_BULL_SNAP_TURN_V1",
            rule_mode="FIRST_BULL_SNAP_TURN_V1",
            lane_id="mgc_first_bull_snap_turn",
            event_json=config.first_bull_snap_turn_event_json,
            event_payload=config.first_bull_snap_turn_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="FIRST_BEAR_SNAP_TURN_V1",
            rule_id="FIRST_BEAR_SNAP_TURN_V1",
            rule_mode="FIRST_BEAR_SNAP_TURN_V1",
            lane_id="mgc_first_bear_snap_turn",
            event_json=config.first_bear_snap_turn_event_json,
            event_payload=config.first_bear_snap_turn_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
            rule_id="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
            rule_mode="LONDON_LATE_PAUSE_RESUME_SHORT_V1",
            lane_id="mgc_london_late_pause_resume_short",
            event_json=config.london_late_pause_resume_short_event_json,
            event_payload=config.london_late_pause_resume_short_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
            rule_id="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
            rule_mode="ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
            lane_id="mgc_asia_late_flat_pullback_pause_resume_long",
            event_json=config.asia_late_flat_pullback_pause_resume_long_event_json,
            event_payload=config.asia_late_flat_pullback_pause_resume_long_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="US_DERIVATIVE_BEAR_TURN_V1",
            rule_id="US_DERIVATIVE_BEAR_TURN_V1",
            rule_mode="US_DERIVATIVE_BEAR_TURN_V1",
            lane_id="mgc_us_derivative_bear_turn",
            event_json=config.us_derivative_bear_turn_event_json,
            event_payload=config.us_derivative_bear_turn_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
            rule_id="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
            rule_mode="MNQ_US_DERIVATIVE_BEAR_TURN_V1",
            lane_id="mnq_us_derivative_bear_turn",
            event_json=config.mnq_us_derivative_bear_turn_event_json,
            event_payload=config.mnq_us_derivative_bear_turn_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
            rule_id="MNQ_FIRST_BEAR_SNAP_TURN_V1",
            rule_mode="MNQ_FIRST_BEAR_SNAP_TURN_V1",
            lane_id="mnq_first_bear_snap_turn",
            event_json=config.mnq_first_bear_snap_turn_event_json,
            event_payload=config.mnq_first_bear_snap_turn_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
            rule_id="MNQ_FIRST_BULL_SNAP_TURN_V1",
            rule_mode="MNQ_FIRST_BULL_SNAP_TURN_V1",
            lane_id="mnq_first_bull_snap_turn",
            event_json=config.mnq_first_bull_snap_turn_event_json,
            event_payload=config.mnq_first_bull_snap_turn_event_payload,
        ),
        TrackBMultiStrategyInput(
            strategy_id="US_LATE_PAUSE_RESUME_LONG_V1",
            rule_id="US_LATE_PAUSE_RESUME_LONG_V1",
            rule_mode="US_LATE_PAUSE_RESUME_LONG_V1",
            lane_id="mgc_us_late_pause_resume_long",
            event_json=config.us_late_pause_resume_long_event_json,
            event_payload=config.us_late_pause_resume_long_event_payload,
        ),
    )
    enabled = {str(item) for item in config.enabled_strategy_ids if str(item)}
    if not enabled:
        return inputs
    return tuple(item for item in inputs if _strategy_input_enabled(item, enabled))


def _strategy_input_enabled(strategy_input: TrackBMultiStrategyInput, enabled_strategy_ids: set[str]) -> bool:
    return bool(
        {
            strategy_input.strategy_id,
            strategy_input.rule_id,
            strategy_input.rule_mode,
            str(strategy_input.strategy_id).upper(),
            str(strategy_input.rule_id).upper(),
            str(strategy_input.rule_mode).upper(),
        }
        & enabled_strategy_ids
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
            "strategy_registry_evaluation_mode": report.get("strategy_registry_evaluation_mode"),
            "strategy_registry_required_1m_context_bars": report.get("strategy_registry_required_1m_context_bars"),
            "strategy_registry_required_5m_context_bars": report.get("strategy_registry_required_5m_context_bars"),
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
        "input_event_path": report.get("input_event_path"),
        "input_quote_provider_mode": report.get("input_quote_provider_mode"),
        "quote_age_seconds": report.get("quote_age_seconds"),
        "decision_reason": report.get("decision_reason"),
        "rule_conditions": report.get("rule_conditions") or {},
        "rule_blockers": report.get("rule_blockers") or [],
        "rule_inputs": report.get("rule_inputs") or {},
        "asian_drift_diagnostic_classification": report.get("asian_drift_diagnostic_classification"),
        "late_join_classification": report.get("late_join_classification"),
        "late_join_diagnostic": report.get("late_join_diagnostic", False),
        "anchor_required": report.get("anchor_required"),
        "anchor_observed": report.get("anchor_observed"),
        "missing_anchor_reason": report.get("missing_anchor_reason"),
        "late_join_policy": report.get("late_join_policy"),
        "operator_explanation": report.get("operator_explanation"),
    }


def _strategy_runtime_verdict(report: Mapping[str, object]) -> str:
    for key in (
        "asian_drift_watch_verdict",
        "asia_early_pause_resume_short_watch_verdict",
        "asia_early_normal_breakout_retest_hold_long_watch_verdict",
        "first_bull_snap_turn_watch_verdict",
        "first_bear_snap_turn_watch_verdict",
        "london_late_pause_resume_short_watch_verdict",
        "asia_late_flat_pullback_pause_resume_long_watch_verdict",
        "us_derivative_bear_turn_watch_verdict",
        "mnq_us_derivative_bear_turn_watch_verdict",
        "mnq_first_bear_snap_turn_watch_verdict",
        "mnq_first_bull_snap_turn_watch_verdict",
        "us_late_pause_resume_long_watch_verdict",
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
    if strategy_id == "FIRST_BULL_SNAP_TURN_V1":
        return "FIRST_BULL_SNAP_TURN_V1"
    if strategy_id == "FIRST_BEAR_SNAP_TURN_V1":
        return "FIRST_BEAR_SNAP_TURN_V1"
    if strategy_id == "LONDON_LATE_PAUSE_RESUME_SHORT_V1":
        return "LONDON_LATE_PAUSE_RESUME_SHORT_V1"
    if strategy_id == "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1":
        return "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1"
    if strategy_id == "US_DERIVATIVE_BEAR_TURN_V1":
        return "US_DERIVATIVE_BEAR_TURN_V1"
    if strategy_id == "MNQ_US_DERIVATIVE_BEAR_TURN_V1":
        return "MNQ_US_DERIVATIVE_BEAR_TURN_V1"
    if strategy_id == "MNQ_FIRST_BEAR_SNAP_TURN_V1":
        return "MNQ_FIRST_BEAR_SNAP_TURN_V1"
    if strategy_id == "MNQ_FIRST_BULL_SNAP_TURN_V1":
        return "MNQ_FIRST_BULL_SNAP_TURN_V1"
    if strategy_id == "US_LATE_PAUSE_RESUME_LONG_V1":
        return "US_LATE_PAUSE_RESUME_LONG_V1"
    return "UNKNOWN"


def _paper_submit_requested(config: TrackBMultiStrategyRuntimeCycleConfig) -> bool:
    return bool(config.submit_paper and config.confirm_paper_submit)


def build_track_b_multi_strategy_cycle_authority(
    *,
    config: TrackBMultiStrategyRuntimeCycleConfig,
    submit_requested: bool,
    now: datetime | None = None,
) -> dict[str, object]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    snapshot_path = _resolve_path(config.repo_root, config.control_plane_snapshot_path)
    safe_state_path = _resolve_path(config.repo_root, config.runtime_safe_state_envelope_path)
    supervisor_path = _resolve_path(config.repo_root, config.runtime_supervisor_authority_path)
    plan_path = _resolve_path(config.repo_root, config.autonomous_recovery_plan_path)
    snapshot = _read_json_object(snapshot_path)
    safe_state = _read_json_object(safe_state_path)
    supervisor = _read_json_object(supervisor_path)
    runtime_generation_id = str(
        config.runtime_generation_id
        or safe_state.get("runtime_generation_id")
        or snapshot.get("runtime_resume_proposed_next_runtime_generation_id")
        or snapshot.get("runtime_generation_id")
        or ""
    )
    base: dict[str, object] = {
        "schema_version": "track_b_multi_strategy_cycle_authority_v1",
        "authorized_at": actual_now.isoformat(),
        "mode": "PAPER",
        "submit_requested": submit_requested,
        "read_only_validation": True,
        "dashboard_projection_consumed": False,
        "not_routing_authority": True,
        "control_plane_snapshot_id": snapshot.get("control_plane_snapshot_id"),
        "shared_truth_generation_id": snapshot.get("shared_truth_refresh_generation_id"),
        "runtime_generation_id": runtime_generation_id or None,
        "safe_state_classification": safe_state.get("safe_state_classification") or safe_state.get("classification"),
        "submit_allowed": False,
        "broker_mutation_allowed": False,
        "runtime_supervisor_classification": snapshot.get("runtime_supervisor_classification")
        or supervisor.get("classification"),
        "runtime_resume_action_policy": snapshot.get("runtime_resume_action_policy"),
        "paper_recovery_policy": snapshot.get("paper_action_policy") or snapshot.get("paper_recovery_policy"),
        "live_money_eligible": _any_true(snapshot, safe_state, supervisor, key="live_money_eligible"),
        "paper_proof_invoked": _any_true(snapshot, safe_state, key="paper_proof_invoked"),
        "source_artifact_paths": {
            "control_plane_snapshot": str(snapshot_path),
            "runtime_safe_state_envelope": str(safe_state_path),
            "runtime_supervisor_authority": str(supervisor_path),
            "autonomous_recovery_plan": str(plan_path),
        },
    }
    classification, reason = _multi_strategy_cycle_authority_blocker(
        config=config,
        snapshot=snapshot,
        safe_state=safe_state,
        supervisor=supervisor,
        runtime_generation_id=runtime_generation_id,
        submit_requested=submit_requested,
        now=actual_now,
    )
    authorized = classification == MULTI_STRATEGY_CYCLE_AUTHORIZED
    return {
        **base,
        "authorization_classification": classification,
        "classification": classification,
        "authorized": authorized,
        "submit_allowed": authorized,
        "broker_mutation_allowed": authorized,
        "reason": reason,
    }


def _multi_strategy_cycle_authority_blocker(
    *,
    config: TrackBMultiStrategyRuntimeCycleConfig,
    snapshot: Mapping[str, Any],
    safe_state: Mapping[str, Any],
    supervisor: Mapping[str, Any],
    runtime_generation_id: str,
    submit_requested: bool,
    now: datetime,
) -> tuple[str, str]:
    if not submit_requested:
        return MULTI_STRATEGY_CYCLE_OBSERVE_ONLY, "No submit delegation requested; cycle remains observe/evaluate only."
    if str(config.paper_execution_path or "").strip().upper() in {"PAPER_PROOF_DEBUG", "PAPER_PROOF_CANARY"}:
        return MULTI_STRATEGY_CYCLE_BLOCKED_PAPER_PROOF, "paper_proof execution paths are not allowed from runtime cycle."
    if not snapshot:
        return MULTI_STRATEGY_CYCLE_BLOCKED_NO_SNAPSHOT, "Control Plane Snapshot authority artifact is missing."
    generated_at = _parse_datetime(snapshot.get("generated_at"))
    if generated_at is None:
        return MULTI_STRATEGY_CYCLE_BLOCKED_NO_SNAPSHOT, "Control Plane Snapshot generated_at is missing or invalid."
    age_seconds = max(0.0, (now.astimezone(UTC) - generated_at.astimezone(UTC)).total_seconds())
    if age_seconds > int(config.control_plane_snapshot_max_age_seconds):
        return MULTI_STRATEGY_CYCLE_BLOCKED_NO_SNAPSHOT, "Control Plane Snapshot is stale for submit delegation."
    if snapshot.get("shared_truth_coherence_status") != "COHERENT":
        return MULTI_STRATEGY_CYCLE_BLOCKED_NO_SNAPSHOT, "Control Plane Snapshot is not coherent."
    if _any_true(snapshot, safe_state, supervisor, key="live_money_eligible"):
        return MULTI_STRATEGY_CYCLE_BLOCKED_LIVE_MONEY, "live_money_eligible=true blocks multi-strategy PAPER submit."
    if _any_true(snapshot, safe_state, key="paper_proof_invoked"):
        return MULTI_STRATEGY_CYCLE_BLOCKED_PAPER_PROOF, "paper_proof_invoked=true blocks strategy submit delegation."
    if not safe_state:
        return MULTI_STRATEGY_CYCLE_BLOCKED_SAFE_STATE, "Runtime Safe-State Envelope authority artifact is missing."
    safe_classification = str(safe_state.get("safe_state_classification") or safe_state.get("classification") or "")
    if safe_state.get("submit_allowed") is not True:
        return MULTI_STRATEGY_CYCLE_BLOCKED_SAFE_STATE, "Runtime Safe-State Envelope does not permit submit."
    if safe_state.get("broker_mutation_allowed") is not True:
        return MULTI_STRATEGY_CYCLE_BLOCKED_SAFE_STATE, "Runtime Safe-State Envelope does not permit broker mutation."
    if safe_state.get("observe_only") is True or "HARD_HOLD" in safe_classification:
        return MULTI_STRATEGY_CYCLE_BLOCKED_SAFE_STATE, f"Runtime Safe-State Envelope blocks submit: {safe_classification}."
    if list(safe_state.get("tripped_limits") or []):
        return MULTI_STRATEGY_CYCLE_BLOCKED_SAFE_STATE, "Runtime Safe-State Envelope has tripped limits."
    if not runtime_generation_id:
        return MULTI_STRATEGY_CYCLE_BLOCKED_RUNTIME_GENERATION, "runtime_generation_id is required before submit delegation."
    proposed_generation = str(snapshot.get("runtime_resume_proposed_next_runtime_generation_id") or "")
    safe_generation = str(safe_state.get("runtime_generation_id") or "")
    if proposed_generation and proposed_generation != runtime_generation_id:
        return MULTI_STRATEGY_CYCLE_BLOCKED_RUNTIME_GENERATION, "Runtime Resume v2 proposed generation does not match cycle."
    if safe_generation and safe_generation != runtime_generation_id:
        return MULTI_STRATEGY_CYCLE_BLOCKED_RUNTIME_GENERATION, "Safe-State runtime generation does not match cycle."
    supervisor_classification = str(
        snapshot.get("runtime_supervisor_classification") or supervisor.get("classification") or ""
    )
    supervisor_submit_allowed = supervisor_classification in {
        "SUPERVISOR_RUNTIME_START_ALLOWED",
        "SUPERVISOR_RUNTIME_ALREADY_HEALTHY",
    } and (
        snapshot.get("safe_to_start_runtime") is True
        or snapshot.get("safe_to_leave_runtime_running") is True
        or safe_state.get("submit_allowed") is True
    )
    if not supervisor_submit_allowed:
        return (
            MULTI_STRATEGY_CYCLE_BLOCKED_SAFE_STATE,
            f"Runtime Supervisor does not permit submit/start posture: {supervisor_classification or 'UNKNOWN'}.",
        )
    return MULTI_STRATEGY_CYCLE_AUTHORIZED, "Multi-strategy cycle authority permits submit-capable handoff."


def _paper_side_for_chosen_signal(config_side: str, chosen_signal: Mapping[str, object]) -> str:
    if str(config_side or "").strip().upper() != "AUTO":
        return config_side
    direction = str(chosen_signal.get("signal_direction") or chosen_signal.get("decision") or "").upper()
    return {"LONG": "BUY", "SHORT": "SELL"}.get(direction, config_side)


def _empty_paper_order_parameters(config: TrackBMultiStrategyRuntimeCycleConfig) -> dict[str, object]:
    return {
        "pricing_policy": str(config.paper_order_pricing_policy or "MANUAL_LIMIT_PRICES").upper(),
        "order_action": None,
        "close_order_action": None,
        "quantity": config.quantity,
        "reference_price_source": None,
        "reference_price": None,
        "open_limit_price": config.manual_open_limit_price,
        "close_limit_price": config.manual_close_limit_price,
        "close_exit_policy": None,
        "paper_order_parameter_blocker": None,
        "pricing_context_path": str(config.pricing_context_json) if config.pricing_context_json else None,
        "live_quote_report_path": str(config.live_quote_report_json) if config.live_quote_report_json else None,
    }


def _resolve_paper_order_parameters(
    config: TrackBMultiStrategyRuntimeCycleConfig,
    chosen_signal: Mapping[str, object],
    now: datetime,
) -> dict[str, object]:
    policy = str(config.paper_order_pricing_policy or "MANUAL_LIMIT_PRICES").strip().upper()
    order_action = _paper_side_for_chosen_signal(config.side, chosen_signal).upper()
    if order_action not in {"BUY", "SELL"}:
        return {
            **_empty_paper_order_parameters(config),
            "order_action": order_action,
            "paper_order_parameter_blocker": "Chosen signal did not resolve to PAPER order_action BUY or SELL.",
        }
    close_action = "SELL" if order_action == "BUY" else "BUY"
    base = {
        **_empty_paper_order_parameters(config),
        "pricing_policy": policy,
        "order_action": order_action,
        "close_order_action": close_action,
        "quantity": config.quantity,
        "close_exit_policy": "MARKETABLE_LIMIT_FLATTEN_AFTER_OPEN_PROOF",
    }
    if policy == "MANUAL_LIMIT_PRICES":
        if config.manual_open_limit_price is None or config.manual_close_limit_price is None:
            return {**base, "paper_order_parameter_blocker": "MANUAL_LIMIT_PRICES requires manual open and close limit prices."}
        open_price = _positive_decimal(config.manual_open_limit_price)
        close_price = _positive_decimal(config.manual_close_limit_price)
        if open_price is None or close_price is None:
            return {**base, "paper_order_parameter_blocker": "Manual PAPER open/close prices must be positive decimals."}
        return {
            **base,
            "reference_price_source": "MANUAL_LIMIT_PRICES",
            "reference_price": _decimal_text(open_price),
            "open_limit_price": _decimal_text(open_price),
            "close_limit_price": _decimal_text(close_price),
        }
    if policy not in {"MARKETABLE_LIMIT_FROM_LIVE_CONTEXT", "LIMIT_AT_LAST", "LIMIT_AT_SIGNAL_PRICE"}:
        return {**base, "paper_order_parameter_blocker": f"Unsupported PAPER order pricing policy: {policy}."}

    context = _pricing_context(config)
    quote = _live_quote_context(config)
    context_blocker = _pricing_context_blocker(config=config, context=context, now=now)
    if context_blocker:
        return {**base, "paper_order_parameter_blocker": context_blocker}
    tick = _positive_decimal(config.tick_size)
    if tick is None:
        return {**base, "paper_order_parameter_blocker": "PAPER order pricing requires a positive tick_size."}

    signal_reference = _decimal_from_first(
        chosen_signal,
        ("reference_price", "signal_reference_price", "signal_price", "close", "last_price"),
    )
    last_price = _latest_last_price(context)
    bid = _decimal_from_first(quote, ("bid", "bid_price", "best_bid", "bid_px"))
    ask = _decimal_from_first(quote, ("ask", "ask_price", "best_ask", "ask_px"))

    if policy == "LIMIT_AT_SIGNAL_PRICE":
        if signal_reference is None:
            return {**base, "paper_order_parameter_blocker": "LIMIT_AT_SIGNAL_PRICE requires a signal reference price."}
        ref_open = signal_reference
        ref_close = signal_reference
        ref_source = "SIGNAL_REFERENCE_PRICE"
    elif policy == "LIMIT_AT_LAST":
        if last_price is None:
            return {**base, "paper_order_parameter_blocker": "LIMIT_AT_LAST requires a fresh live last/close price."}
        ref_open = last_price
        ref_close = last_price
        ref_source = "LIVE_1M_LAST_CLOSE"
    else:
        if order_action == "BUY":
            ref_open = ask or last_price
            ref_close = bid or last_price
            ref_source = "LIVE_QUOTE_ASK" if ask is not None else "LIVE_1M_LAST_CLOSE"
            close_ref_source = "LIVE_QUOTE_BID" if bid is not None else "LIVE_1M_LAST_CLOSE"
        else:
            ref_open = bid or last_price
            ref_close = ask or last_price
            ref_source = "LIVE_QUOTE_BID" if bid is not None else "LIVE_1M_LAST_CLOSE"
            close_ref_source = "LIVE_QUOTE_ASK" if ask is not None else "LIVE_1M_LAST_CLOSE"
        if ref_open is None or ref_close is None:
            return {
                **base,
                "paper_order_parameter_blocker": "MARKETABLE_LIMIT_FROM_LIVE_CONTEXT requires a fresh live quote or last price.",
            }
        open_offset = tick * Decimal(max(int(config.paper_order_price_offset_ticks), 0))
        close_offset = tick * Decimal(max(int(config.paper_exit_price_offset_ticks), 0))
        open_price = ref_open + open_offset if order_action == "BUY" else ref_open - open_offset
        close_price = ref_close - close_offset if close_action == "SELL" else ref_close + close_offset
        return {
            **base,
            "reference_price_source": ref_source,
            "close_reference_price_source": close_ref_source,
            "reference_price": _decimal_text(ref_open),
            "close_reference_price": _decimal_text(ref_close),
            "open_limit_price": _decimal_text(_round_to_tick(open_price, tick)),
            "close_limit_price": _decimal_text(_round_to_tick(close_price, tick)),
            "tick_size": _decimal_text(tick),
            "open_price_offset_ticks": config.paper_order_price_offset_ticks,
            "close_price_offset_ticks": config.paper_exit_price_offset_ticks,
        }

    return {
        **base,
        "reference_price_source": ref_source,
        "reference_price": _decimal_text(ref_open),
        "open_limit_price": _decimal_text(_round_to_tick(ref_open, tick)),
        "close_limit_price": _decimal_text(_round_to_tick(ref_close, tick)),
        "tick_size": _decimal_text(tick),
    }


def _pricing_context(config: TrackBMultiStrategyRuntimeCycleConfig) -> Mapping[str, object]:
    if config.pricing_context_payload is not None:
        return config.pricing_context_payload
    if config.pricing_context_json is None:
        return {}
    try:
        value = json.loads(Path(config.pricing_context_json).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, Mapping) else {}


def _live_quote_context(config: TrackBMultiStrategyRuntimeCycleConfig) -> Mapping[str, object]:
    if config.live_quote_report_payload is not None:
        return config.live_quote_report_payload
    if config.live_quote_report_json is None:
        return {}
    try:
        value = json.loads(Path(config.live_quote_report_json).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return value if isinstance(value, Mapping) else {}


def _pricing_context_blocker(
    *,
    config: TrackBMultiStrategyRuntimeCycleConfig,
    context: Mapping[str, object],
    now: datetime,
) -> str | None:
    if not context:
        return "Fresh Live pricing context JSON/payload is required for automatic PAPER order pricing."
    if context.get("fresh_for_execution") is not True or context.get("runtime_candle_context_ready") is not True:
        return "Live pricing context must be fresh_for_execution=true and runtime_candle_context_ready=true."
    expected = {
        "contract_key": config.contract_key,
        "local_symbol": config.allowlisted_local_symbol,
        "dataset": "GLBX.MDP3",
    }
    for key, expected_value in expected.items():
        observed = context.get(key)
        if observed is not None and str(observed) != str(expected_value):
            return f"Live pricing context {key} mismatch: expected {expected_value}, observed {observed}."
    age = _optional_decimal(context.get("latest_1m_age_seconds") or context.get("latest_1m_candle_age_seconds"))
    if age is None:
        latest_timestamp = _latest_context_timestamp(context)
        if latest_timestamp is not None:
            age = Decimal(str(max(0.0, (now.astimezone(UTC) - latest_timestamp.astimezone(UTC)).total_seconds())))
    if age is None:
        return "Live pricing context has no latest 1m age/timestamp for freshness validation."
    if age > Decimal(str(config.max_pricing_context_age_seconds)):
        return (
            f"Live pricing context is stale for PAPER pricing: age_seconds={_decimal_text(age)}, "
            f"max={config.max_pricing_context_age_seconds}."
        )
    return None


def _latest_context_timestamp(context: Mapping[str, object]) -> datetime | None:
    raw = context.get("candle_timestamp") or context.get("last_candle_timestamp") or context.get("latest_1m_timestamp")
    candles = context.get("candles") or context.get("candle_history")
    if isinstance(candles, list) and candles:
        latest = candles[-1]
        if isinstance(latest, Mapping):
            raw = latest.get("candle_timestamp") or latest.get("timestamp") or raw
    if not raw:
        return None
    try:
        parsed = datetime.fromisoformat(str(raw).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _latest_last_price(context: Mapping[str, object]) -> Decimal | None:
    direct = _decimal_from_first(context, ("last", "last_price", "close"))
    if direct is not None:
        return direct
    candles = context.get("candles") or context.get("candle_history")
    if isinstance(candles, list) and candles:
        latest = candles[-1]
        if isinstance(latest, Mapping):
            return _decimal_from_first(latest, ("close", "last", "last_price"))
    return None


def _decimal_from_first(payload: Mapping[str, object], keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        value = payload.get(key)
        parsed = _optional_decimal(value)
        if parsed is not None:
            return parsed
    return None


def _positive_decimal(value: object) -> Decimal | None:
    parsed = _optional_decimal(value)
    if parsed is None or parsed <= 0:
        return None
    return parsed


def _optional_decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None
    return parsed if parsed.is_finite() else None


def _round_to_tick(price: Decimal, tick: Decimal) -> Decimal:
    if tick <= 0:
        return price
    units = (price / tick).to_integral_value(rounding=ROUND_HALF_UP)
    return units * tick


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


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
    paper_order_parameters: Mapping[str, object],
    cycle_authority: Mapping[str, object],
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
        "multi_strategy_cycle_authority": dict(cycle_authority),
        "multi_strategy_cycle_authorization_classification": cycle_authority.get("authorization_classification"),
        "control_plane_snapshot_id": cycle_authority.get("control_plane_snapshot_id"),
        "shared_truth_generation_id": cycle_authority.get("shared_truth_generation_id"),
        "runtime_generation_id": cycle_authority.get("runtime_generation_id"),
        "safe_state_classification": cycle_authority.get("safe_state_classification"),
        "cycle_submit_allowed": cycle_authority.get("submit_allowed") is True,
        "cycle_broker_mutation_allowed": cycle_authority.get("broker_mutation_allowed") is True,
        "runtime_supervisor_classification": cycle_authority.get("runtime_supervisor_classification"),
        "runtime_resume_action_policy": cycle_authority.get("runtime_resume_action_policy"),
        "paper_recovery_policy": cycle_authority.get("paper_recovery_policy"),
        "cycle_authority_blocked_reason": (
            None
            if cycle_authority.get("authorization_classification")
            in {MULTI_STRATEGY_CYCLE_AUTHORIZED, MULTI_STRATEGY_CYCLE_OBSERVE_ONLY}
            else cycle_authority.get("reason")
        ),
        "paper_order_parameters": dict(paper_order_parameters),
        "paper_order_parameter_blocker": paper_order_parameters.get("paper_order_parameter_blocker"),
        "order_action": paper_order_parameters.get("order_action"),
        "quantity": paper_order_parameters.get("quantity"),
        "pricing_policy": paper_order_parameters.get("pricing_policy"),
        "reference_price_source": paper_order_parameters.get("reference_price_source"),
        "reference_price": paper_order_parameters.get("reference_price"),
        "open_limit_price": paper_order_parameters.get("open_limit_price"),
        "close_exit_policy": paper_order_parameters.get("close_exit_policy"),
        "close_limit_price": paper_order_parameters.get("close_limit_price"),
        "readiness_invoked": bool(paper_report.get("readiness_invoked")) if paper_report else False,
        "paper_proof_invoked": bool(paper_report.get("paper_proof_invoked")) if paper_report else False,
        "submit_allowed": bool(paper_report.get("submit_allowed")) if paper_report else False,
        "submit_attempted": bool(paper_report.get("submit_attempted")) if paper_report else False,
        "broker_state_mutated": bool(paper_report.get("broker_state_mutated")) if paper_report else False,
        "paper_runner_report_path": str(paper_result.report_json) if paper_result else None,
        "paper_runner_verdict": paper_report.get("strategy_paper_runner_verdict"),
        "strategy_trade_intent_created": paper_report.get("strategy_trade_intent_created"),
        "strategy_trade_intent_classification": paper_report.get("strategy_trade_intent_classification"),
        "strategy_trade_intent_id": paper_report.get("strategy_trade_intent_id"),
        "strategy_trade_intent_report_path": paper_report.get("strategy_trade_intent_report_path"),
        "intent_blocked_reason": paper_report.get("intent_blocked_reason"),
        "paper_execution_path": paper_report.get("paper_execution_path") or config.paper_execution_path,
        "managed_lifecycle_invoked": bool(paper_report.get("managed_lifecycle_invoked")) if paper_report else False,
        "managed_lifecycle_classification": paper_report.get("managed_lifecycle_classification"),
        "managed_lifecycle_report_path": paper_report.get("managed_lifecycle_report_path"),
        "managed_exit_policy_id": paper_report.get("managed_exit_policy_id") or config.managed_exit_policy_id,
        "managed_exit_policy_max_completed_5m_bars": paper_report.get("managed_exit_policy_max_completed_5m_bars"),
        "managed_open_position_age_completed_5m_bars": paper_report.get("managed_open_position_age_completed_5m_bars"),
        "managed_expected_exit_condition": paper_report.get("managed_expected_exit_condition"),
        "managed_close_intent_status": paper_report.get("managed_close_intent_status"),
        "paper_proof_classification": paper_report.get("paper_proof_classification"),
        "final_broker_state_classification": (
            paper_report.get("managed_lifecycle_classification")
            or paper_report.get("paper_proof_lifecycle_status")
        ),
        "final_flat": paper_report.get("final_flat"),
        "primary_blocker": None if primary_blocker is None else str(primary_blocker),
        "required_next_action": required_next_action,
        "live_money_readiness": False,
        "live_money_submit_allowed": False,
        "ui_authority": False,
        "hidden_submit": False,
        "direct_broker_path": False,
        "operator_status_invoked": False,
        "operator_status_verdict": None,
        "operator_status_report_path": None,
        "latest_operator_status_path": None,
        "operator_status_error": None,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_multi_strategy_runtime_cycle_report.json"),
    }


def _maybe_update_operator_status(
    config: TrackBMultiStrategyRuntimeCycleConfig,
    report: Mapping[str, object],
) -> dict[str, object]:
    if not config.update_operator_status:
        return {
            "operator_status_invoked": False,
            "operator_status_verdict": None,
            "operator_status_report_path": None,
            "latest_operator_status_path": None,
            "operator_status_error": None,
        }
    try:
        runtime_cycle_report_json = Path(str(report["latest_report_json_path"]))
        result = create_operator_status_summary(
            inputs=_operator_status_inputs_for_runtime_cycle(
                config=config,
                runtime_cycle_report_json=runtime_cycle_report_json,
            )
        )
        return {
            "operator_status_invoked": True,
            "operator_status_verdict": result.report.get("status_verdict"),
            "operator_status_report_path": str(result.report_json),
            "latest_operator_status_path": str(result.report.get("latest_report_json_path")),
            "operator_status_error": None,
        }
    except Exception as exc:  # noqa: BLE001 - status refresh must remain report-only.
        return {
            "operator_status_invoked": True,
            "operator_status_verdict": None,
            "operator_status_report_path": None,
            "latest_operator_status_path": None,
            "operator_status_error": str(exc),
        }


def _maybe_record_decision_journal(
    config: TrackBMultiStrategyRuntimeCycleConfig,
    report: Mapping[str, object],
    report_json: Path,
    now: datetime,
) -> dict[str, object]:
    if not config.decision_journal_enabled:
        return {
            "decision_journal_invoked": False,
            "decision_journal_summary_path": None,
            "decision_journal_active_path": None,
            "decision_journal_heartbeat_path": None,
            "decision_journal_full_records_written": 0,
            "decision_journal_tier_counts": {},
            "decision_journal_error": None,
        }
    output_root = config.decision_journal_output_root or config.output_root.parent / "track_b_decision_journal"
    try:
        result = record_track_b_decision_journal_cycle(
            runtime_cycle_report=report,
            runtime_cycle_report_json=report_json,
            output_root=output_root,
            now=now,
        )
        return {
            "decision_journal_invoked": True,
            "decision_journal_summary_path": str(result.summary_json),
            "decision_journal_active_path": str(result.active_journal_jsonl),
            "decision_journal_heartbeat_path": str(result.heartbeat_jsonl),
            "decision_journal_aggregate_path": str(result.aggregate_json),
            "decision_journal_full_records_written": result.summary.get("full_records_written"),
            "decision_journal_tier_counts": result.summary.get("latest_tier_counts") or {},
            "decision_journal_error": None,
        }
    except Exception as exc:  # noqa: BLE001 - journaling must not block shadow runtime status.
        return {
            "decision_journal_invoked": True,
            "decision_journal_summary_path": None,
            "decision_journal_active_path": None,
            "decision_journal_heartbeat_path": None,
            "decision_journal_aggregate_path": None,
            "decision_journal_full_records_written": 0,
            "decision_journal_tier_counts": {},
            "decision_journal_error": str(exc),
        }


def _operator_status_inputs_for_runtime_cycle(
    *,
    config: TrackBMultiStrategyRuntimeCycleConfig,
    runtime_cycle_report_json: Path,
) -> OperatorStatusInputs:
    return OperatorStatusInputs(
        backend_health_json=_existing_optional_path(config.backend_health_json),
        listener_heartbeat_json=_existing_path(
            "outputs/track_b_execution_core/shadow_listener/track_b_example_shadow_listener_v1/latest_shadow_listener_heartbeat.json"
        ),
        listener_health_json=_existing_path(
            "outputs/track_b_execution_core/shadow_listener/track_b_example_shadow_listener_v1/latest_shadow_listener_health.json"
        ),
        track_b_multi_strategy_runtime_cycle_report_json=runtime_cycle_report_json,
        databento_candle_observer_report_json=_existing_path(
            "outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json"
        ),
        databento_candle_observer_heartbeat_json=_existing_path(
            "outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_heartbeat.json"
        ),
        strategy_signal_adapter_report_json=_existing_path(
            "outputs/track_b_execution_core/strategy_signal_adapter/latest_strategy_signal_adapter_report.json"
        ),
        candle_signal_producer_report_json=_existing_path(
            "outputs/track_b_execution_core/candle_signal_producer/latest_candle_signal_producer_report.json"
        ),
        signal_batch_writer_report_json=_existing_path(
            "outputs/track_b_execution_core/signal_batch_writer/latest_signal_batch_writer_report.json"
        ),
        output_root=config.operator_status_output_root,
    )


def _existing_path(raw_path: str) -> Path | None:
    path = Path(raw_path)
    return path if path.exists() else None


def _existing_optional_path(path: Path | None) -> Path | None:
    if path is None:
        return None
    return path if path.exists() else None


def _reason_no_signal_chosen(
    verdict: TrackBMultiStrategyRuntimeCycleVerdict,
    arbitration: Mapping[str, object],
    primary_blocker: object | None,
) -> str | None:
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION:
        return "No registered Asia strategy emitted a real signal."
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.SIGNAL_READY_NO_SUBMIT:
        return str(primary_blocker or "One signal was chosen, but explicit PAPER submit flags were not supplied.")
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.ARBITRATION_BLOCKED:
        return str(primary_blocker or arbitration.get("primary_blocker") or "Strategy arbitration did not choose a signal.")
    return None


def _read_json_object(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _resolve_path(repo_root: Path, path: Path) -> Path:
    return path if path.is_absolute() else Path(repo_root) / path


def _parse_datetime(value: object) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _any_true(*payloads: Mapping[str, Any], key: str) -> bool:
    return any(payload.get(key) is True for payload in payloads)


def _write_report(report_json: Path, report: dict[str, object]) -> None:
    report_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
