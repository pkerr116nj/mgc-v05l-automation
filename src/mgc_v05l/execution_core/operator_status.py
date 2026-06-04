"""Track B no-submit operator status summary boundary.

This module aggregates existing observer reports into one read-only status
artifact. It is intended as a future dashboard read model and never becomes a
source of trading truth or submit authority.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping

from .models import require_aware_datetime, to_jsonable
from .track_b_paper_trade_ledger import DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT
from .track_b_startup_phase_classifier import (
    TrackBStartupPhaseClassifierConfig,
    classify_track_b_startup_phase,
)


DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT = Path("outputs/track_b_execution_core/operator_status")
NOT_PROVIDED = "NOT_PROVIDED"
DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON = (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON = (
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT / "latest_track_b_live_position_status.json"
)
DEFAULT_TRACK_B_PNL_SUMMARY_JSON = DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT / "latest_track_b_pnl_summary.json"


class OperatorStatusVerdict(str, Enum):
    OK_FOR_SHADOW_REVIEW = "OPERATOR_STATUS_OK_FOR_SHADOW_REVIEW"
    READY_FOR_PAPER_PROOF_REVIEW = "OPERATOR_STATUS_READY_FOR_PAPER_PROOF_REVIEW"
    DEGRADED_SHADOW_FAILURES = "OPERATOR_STATUS_DEGRADED_SHADOW_FAILURES"
    BLOCKED_READINESS = "OPERATOR_STATUS_BLOCKED_READINESS"
    BLOCKED_BROKER_STATE = "OPERATOR_STATUS_BLOCKED_BROKER_STATE"
    MISSING_REPORTS = "OPERATOR_STATUS_MISSING_REPORTS"
    UNKNOWN = "OPERATOR_STATUS_UNKNOWN"


@dataclass(frozen=True)
class OperatorStatusInputs:
    backend_health_json: Path | None = None
    listener_heartbeat_json: Path | None = None
    listener_health_json: Path | None = None
    listener_cycle_json: Path | None = None
    shadow_runner_summary_json: Path | None = None
    attrition_report_json: Path | None = None
    track_b_observation_runner_report_json: Path | None = None
    track_b_readiness_check_runner_report_json: Path | None = None
    track_b_strategy_rule_runner_report_json: Path | None = None
    track_b_strategy_paper_runner_report_json: Path | None = None
    track_b_multi_strategy_runtime_cycle_report_json: Path | None = None
    track_b_shadow_monitor_report_json: Path | None = None
    track_b_shadow_monitor_heartbeat_json: Path | None = None
    databento_candle_observer_report_json: Path | None = None
    databento_candle_observer_heartbeat_json: Path | None = None
    strategy_signal_adapter_report_json: Path | None = None
    candle_signal_producer_report_json: Path | None = None
    signal_batch_writer_report_json: Path | None = None
    readiness_summary_json: Path | None = None
    recovery_report_json: Path | None = None
    preflight_report_json: Path | None = None
    quote_report_json: Path | None = None
    track_b_paper_trade_summary_json: Path | None = DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON
    track_b_live_position_status_json: Path | None = DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON
    track_b_pnl_summary_json: Path | None = DEFAULT_TRACK_B_PNL_SUMMARY_JSON
    startup_phase_config: TrackBStartupPhaseClassifierConfig | None = None
    output_root: Path = DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT


@dataclass(frozen=True)
class OperatorStatusResult:
    verdict: OperatorStatusVerdict
    report_json: Path
    report: dict[str, Any]


def create_operator_status_summary(
    *,
    inputs: OperatorStatusInputs,
    status_id: str | None = None,
    now: datetime | None = None,
) -> OperatorStatusResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_status_id = status_id or f"operator_status_{uuid.uuid4().hex}"
    report_json = Path(inputs.output_root) / actual_status_id / "operator_status_summary.json"
    reports = _load_reports(inputs)
    verdict, primary_blocker, required_next_action = _classify(reports)
    report = _report(
        report_json=report_json,
        now=actual_now,
        status_id=actual_status_id,
        verdict=verdict,
        reports=reports,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
    )
    report_json.parent.mkdir(parents=True, exist_ok=True)
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    return OperatorStatusResult(verdict=verdict, report_json=report_json, report=report)


def _load_reports(inputs: OperatorStatusInputs) -> dict[str, dict[str, Any] | None]:
    def read_default_operator_summary(path: Path | None, default_path: Path) -> dict[str, Any] | None:
        if path == default_path and Path(inputs.output_root) != DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT:
            return None
        return _read_optional_json(path)

    return {
        "backend_health": _read_json(inputs.backend_health_json),
        "listener_heartbeat": _read_json(inputs.listener_heartbeat_json),
        "listener_health": _read_json(inputs.listener_health_json),
        "listener_cycle": _read_json(inputs.listener_cycle_json),
        "shadow_runner": _read_json(inputs.shadow_runner_summary_json),
        "attrition": _read_json(inputs.attrition_report_json),
        "track_b_observation_runner": _read_json(inputs.track_b_observation_runner_report_json),
        "track_b_readiness_check_runner": _read_json(inputs.track_b_readiness_check_runner_report_json),
        "track_b_strategy_rule_runner": _read_json(inputs.track_b_strategy_rule_runner_report_json),
        "track_b_strategy_paper_runner": _read_json(inputs.track_b_strategy_paper_runner_report_json),
        "track_b_multi_strategy_runtime_cycle": _read_json(inputs.track_b_multi_strategy_runtime_cycle_report_json),
        "track_b_shadow_monitor": _read_json(inputs.track_b_shadow_monitor_report_json),
        "track_b_shadow_monitor_heartbeat": _read_json(inputs.track_b_shadow_monitor_heartbeat_json),
        "databento_candle_observer": _read_json(inputs.databento_candle_observer_report_json),
        "databento_candle_observer_heartbeat": _read_json(inputs.databento_candle_observer_heartbeat_json),
        "strategy_signal_adapter": _read_json(inputs.strategy_signal_adapter_report_json),
        "candle_signal_producer": _read_json(inputs.candle_signal_producer_report_json),
        "signal_batch_writer": _read_json(inputs.signal_batch_writer_report_json),
        "readiness": _read_json(inputs.readiness_summary_json),
        "recovery": _read_json(inputs.recovery_report_json),
        "preflight": _read_json(inputs.preflight_report_json),
        "quote": _read_json(inputs.quote_report_json),
        "track_b_paper_trade_summary": read_default_operator_summary(
            inputs.track_b_paper_trade_summary_json,
            DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON,
        ),
        "track_b_live_position_status": read_default_operator_summary(
            inputs.track_b_live_position_status_json,
            DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON,
        ),
        "track_b_pnl_summary": read_default_operator_summary(
            inputs.track_b_pnl_summary_json,
            DEFAULT_TRACK_B_PNL_SUMMARY_JSON,
        ),
        "track_b_startup_phase": _startup_phase_status(inputs),
    }


def _startup_phase_status(inputs: OperatorStatusInputs) -> dict[str, Any] | None:
    config = inputs.startup_phase_config
    if config is None:
        if Path(inputs.output_root) != DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT:
            return None
        config = TrackBStartupPhaseClassifierConfig(repo_root=Path("."))
    return classify_track_b_startup_phase(config=config)


def _classify(reports: Mapping[str, Mapping[str, Any] | None]) -> tuple[OperatorStatusVerdict, str | None, str]:
    recovery = reports.get("recovery") or {}
    readiness = reports.get("readiness") or {}
    preflight = reports.get("preflight") or {}
    track_b_readiness_check_runner = reports.get("track_b_readiness_check_runner") or {}
    listener_health = reports.get("listener_health") or {}
    listener_heartbeat = reports.get("listener_heartbeat") or {}
    track_b_multi_strategy_runtime_cycle = reports.get("track_b_multi_strategy_runtime_cycle") or {}
    backend_health = reports.get("backend_health") or {}

    recovery_verdict = _recovery_verdict(recovery)
    if recovery_verdict in {"BLOCKED_UNRESOLVED_BROKER_ORDER", "RECOVERY_BLOCKED_UNRESOLVED_ORDER"}:
        return (
            OperatorStatusVerdict.BLOCKED_BROKER_STATE,
            str(recovery.get("primary_blocker") or "Recovery report shows unresolved broker state."),
            str(recovery.get("required_next_action") or "Resolve broker state before any paper proof submit."),
        )

    readiness_verdict = str(readiness.get("final_readiness_verdict") or "")
    if readiness_verdict and readiness_verdict != "READY_FOR_PAPER_PROOF":
        if readiness_verdict == "BLOCKED_UNRESOLVED_BROKER_ORDER":
            return (
                OperatorStatusVerdict.BLOCKED_BROKER_STATE,
                str(readiness.get("primary_blocker") or "Readiness summary shows unresolved broker state."),
                str(readiness.get("required_next_action") or "Resolve broker state before any paper proof submit."),
            )
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(readiness.get("primary_blocker") or f"Readiness summary is blocked: {readiness_verdict}"),
            str(readiness.get("required_next_action") or "Resolve readiness blocker before paper proof review."),
        )

    preflight_verdict = str(preflight.get("final_readiness_verdict") or "")
    preflight_classification = str(preflight.get("classification") or "")
    if preflight_verdict == "BLOCKED_UNRESOLVED_BROKER_ORDER" or preflight.get("unresolved_broker_order_detected"):
        return (
            OperatorStatusVerdict.BLOCKED_BROKER_STATE,
            str(preflight.get("primary_blocker") or preflight.get("failure_or_ambiguity") or "Preflight shows unresolved broker order."),
            str(preflight.get("required_next_action") or "Resolve broker state before any paper proof submit."),
        )
    if preflight_verdict and preflight_verdict != "READY_FOR_PAPER_PROOF":
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(preflight.get("primary_blocker") or "Preflight readiness is blocked."),
            str(preflight.get("required_next_action") or "Resolve preflight blocker before paper proof review."),
        )
    if preflight_classification and preflight_classification not in {"READY_READ_ONLY"}:
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(preflight.get("primary_blocker") or preflight.get("failure_or_ambiguity") or f"Preflight classification is {preflight_classification}."),
            str(preflight.get("required_next_action") or "Resolve preflight blocker before paper proof review."),
        )

    readiness_check_verdict = str(track_b_readiness_check_runner.get("runner_verdict") or "")
    if readiness_check_verdict.startswith("TRACK_B_READINESS_CHECK_BLOCKED"):
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(track_b_readiness_check_runner.get("primary_blocker") or f"Readiness check runner is blocked: {readiness_check_verdict}"),
            str(track_b_readiness_check_runner.get("required_next_action") or "Resolve readiness check runner blocker before paper proof review."),
        )
    if _readiness_check_runner_ready_for_paper_proof_review(track_b_readiness_check_runner):
        return (
            OperatorStatusVerdict.READY_FOR_PAPER_PROOF_REVIEW,
            None,
            str(
                track_b_readiness_check_runner.get("required_next_action")
                or "paper_proof_cli remains a separate explicit operator decision and was not called."
            ),
        )

    track_b_strategy_paper_runner = reports.get("track_b_strategy_paper_runner") or {}
    paper_runner_verdict = str(track_b_strategy_paper_runner.get("strategy_paper_runner_verdict") or "")
    if paper_runner_verdict in {
        "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_READINESS",
        "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_BLOCKED",
        "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_INVALID_SUBMIT_REQUEST",
        "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_NON_PAPER_MODE",
        "TRACK_B_STRATEGY_PAPER_RUNNER_BLOCKED_STAGE_ERROR",
    }:
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(track_b_strategy_paper_runner.get("primary_blocker") or f"Strategy PAPER runner is blocked: {paper_runner_verdict}"),
            str(track_b_strategy_paper_runner.get("required_next_action") or "Resolve strategy PAPER runner blocker before retrying."),
        )
    if paper_runner_verdict in {
        "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_AMBIGUOUS_MANUAL_REVIEW_REQUIRED",
        "TRACK_B_STRATEGY_PAPER_RUNNER_PAPER_PROOF_FLAT_BUT_CLOSE_PROVENANCE_INCOMPLETE",
    }:
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(track_b_strategy_paper_runner.get("primary_blocker") or "Strategy PAPER proof is ambiguous and requires manual review."),
            str(track_b_strategy_paper_runner.get("required_next_action") or "Reconcile broker state before any further PAPER submit."),
        )

    track_b_shadow_monitor = reports.get("track_b_shadow_monitor") or {}
    shadow_monitor_verdict = str(track_b_shadow_monitor.get("monitor_verdict") or "")
    if shadow_monitor_verdict in {
        "TRACK_B_SHADOW_MONITOR_CRITICAL_UNEXPECTED_MUTATION_FLAG",
        "TRACK_B_SHADOW_MONITOR_ERROR",
    }:
        return (
            OperatorStatusVerdict.DEGRADED_SHADOW_FAILURES,
            str(track_b_shadow_monitor.get("primary_blocker") or f"Track B shadow monitor is degraded: {shadow_monitor_verdict}"),
            str(track_b_shadow_monitor.get("required_next_action") or "Inspect Track B shadow monitor before continuing."),
        )
    if shadow_monitor_verdict in {
        "TRACK_B_SHADOW_MONITOR_BLOCKED_PROVIDER_ERROR",
        "TRACK_B_SHADOW_MONITOR_BLOCKED_PRODUCER_ERROR",
        "TRACK_B_SHADOW_MONITOR_NOT_READY_STALE_RUNTIME_CONTEXT",
    }:
        return (
            OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW,
            str(track_b_shadow_monitor.get("primary_blocker") or f"Track B shadow monitor is not ready: {shadow_monitor_verdict}"),
            str(track_b_shadow_monitor.get("required_next_action") or "Monitor remains no-submit; resolve the data/producer blocker."),
        )
    if shadow_monitor_verdict in {
        "TRACK_B_SHADOW_MONITOR_OK_NO_SIGNAL",
        "TRACK_B_SHADOW_MONITOR_OK_SIGNAL_READY_NO_SUBMIT",
        "TRACK_B_SHADOW_MONITOR_HEARTBEAT_NO_NEW_COMPLETED_BAR",
        "TRACK_B_SHADOW_MONITOR_NOT_READY_NO_STRATEGIES_CONFIGURED",
        "TRACK_B_SHADOW_MONITOR_NOT_READY_UNWIRED_INSTRUMENT",
    }:
        return (
            OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW,
            None if shadow_monitor_verdict.startswith("TRACK_B_SHADOW_MONITOR_OK") else str(track_b_shadow_monitor.get("primary_blocker") or ""),
            str(track_b_shadow_monitor.get("required_next_action") or "Track B shadow monitor is display/status only."),
        )

    multi_strategy_verdict = str(track_b_multi_strategy_runtime_cycle.get("multi_strategy_runtime_cycle_verdict") or "")
    if multi_strategy_verdict in {
        "TRACK_B_MULTI_STRATEGY_RUNTIME_ARBITRATION_BLOCKED",
        "TRACK_B_MULTI_STRATEGY_RUNTIME_PAPER_PROOF_REVIEW_REQUIRED",
        "TRACK_B_MULTI_STRATEGY_RUNTIME_BLOCKED_STAGE_ERROR",
    }:
        return (
            OperatorStatusVerdict.BLOCKED_READINESS,
            str(
                track_b_multi_strategy_runtime_cycle.get("primary_blocker")
                or f"Multi-strategy runtime cycle is blocked: {multi_strategy_verdict}"
            ),
            str(
                track_b_multi_strategy_runtime_cycle.get("required_next_action")
                or "Review multi-strategy runtime cycle arbitration before any PAPER retry."
            ),
        )
    if multi_strategy_verdict in {
        "TRACK_B_MULTI_STRATEGY_RUNTIME_NO_SIGNAL_NO_MUTATION",
        "TRACK_B_MULTI_STRATEGY_RUNTIME_SIGNAL_READY_NO_SUBMIT",
        "TRACK_B_MULTI_STRATEGY_RUNTIME_PAPER_PROOF_PASSED",
    }:
        return (
            OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW,
            None,
            str(
                track_b_multi_strategy_runtime_cycle.get("required_next_action")
                or "Review multi-strategy runtime cycle artifacts. Dashboard remains display-only."
            ),
        )

    health_verdict = str(listener_health.get("health_verdict") or listener_heartbeat.get("last_health_verdict") or "")
    if health_verdict == "SHADOW_LISTENER_HEALTH_DEGRADED_FAILURES":
        return (
            OperatorStatusVerdict.DEGRADED_SHADOW_FAILURES,
            str(listener_health.get("last_primary_blocker") or listener_heartbeat.get("primary_blocker") or "Listener health is degraded due to failed files."),
            str(listener_health.get("last_required_next_action") or listener_heartbeat.get("required_next_action") or "Review listener event and replay reports."),
        )
    if health_verdict == "SHADOW_LISTENER_HEALTH_OK":
        return (
            OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW,
            None,
            "Shadow observer reports are OK for no-submit review. This does not authorize paper or live submit.",
        )
    if health_verdict == "SHADOW_LISTENER_HEALTH_NO_FILES":
        return (
            OperatorStatusVerdict.OK_FOR_SHADOW_REVIEW,
            None,
            "Listener is healthy but no files were available. This does not authorize submit.",
        )
    if _all_missing(reports):
        return (
            OperatorStatusVerdict.MISSING_REPORTS,
            "No Track B observer reports were provided.",
            "Provide at least listener health or another Track B observer report.",
        )
    if backend_health and not _backend_health_ok(backend_health) and _all_track_b_runtime_reports_missing(reports):
        return (
            OperatorStatusVerdict.UNKNOWN,
            str(
                backend_health.get("error")
                or backend_health.get("message")
                or backend_health.get("reason_detail")
                or "Backend health is not OK."
            ),
            "Start or repair the local operator dashboard backend, then refresh Track B operator status artifacts.",
        )
    return (
        OperatorStatusVerdict.UNKNOWN,
        "Supplied reports do not contain enough status evidence for a clear verdict.",
        "Provide listener health, readiness, recovery, or preflight reports.",
    )


def _report(
    *,
    report_json: Path,
    now: datetime,
    status_id: str,
    verdict: OperatorStatusVerdict,
    reports: Mapping[str, Mapping[str, Any] | None],
    primary_blocker: str | None,
    required_next_action: str,
) -> dict[str, Any]:
    missing = [name for name, report in reports.items() if report is None]
    considered = {name: report is not None for name, report in reports.items()}
    backend_health = reports.get("backend_health") or {}
    listener_heartbeat = reports.get("listener_heartbeat") or {}
    listener_health = reports.get("listener_health") or {}
    listener_cycle = reports.get("listener_cycle") or {}
    shadow_runner = reports.get("shadow_runner") or {}
    attrition = reports.get("attrition") or {}
    track_b_observation_runner = reports.get("track_b_observation_runner") or {}
    track_b_readiness_check_runner = reports.get("track_b_readiness_check_runner") or {}
    track_b_strategy_rule_runner = reports.get("track_b_strategy_rule_runner") or {}
    track_b_strategy_paper_runner = reports.get("track_b_strategy_paper_runner") or {}
    track_b_multi_strategy_runtime_cycle = reports.get("track_b_multi_strategy_runtime_cycle") or {}
    track_b_shadow_monitor = reports.get("track_b_shadow_monitor") or {}
    track_b_shadow_monitor_heartbeat = reports.get("track_b_shadow_monitor_heartbeat") or {}
    databento_candle_observer = reports.get("databento_candle_observer") or {}
    databento_candle_observer_heartbeat = reports.get("databento_candle_observer_heartbeat") or {}
    strategy_signal_adapter = reports.get("strategy_signal_adapter") or {}
    candle_signal_producer = reports.get("candle_signal_producer") or {}
    signal_batch_writer = reports.get("signal_batch_writer") or {}
    readiness = reports.get("readiness") or {}
    recovery = reports.get("recovery") or {}
    preflight = reports.get("preflight") or {}
    quote = reports.get("quote") or {}
    track_b_paper_trade_summary = reports.get("track_b_paper_trade_summary") or {}
    track_b_live_position_status = reports.get("track_b_live_position_status") or {}
    track_b_pnl_summary = reports.get("track_b_pnl_summary") or {}
    track_b_startup_phase = reports.get("track_b_startup_phase") or {}
    latest_output_paths = {
        "backend_health": backend_health.get("report_json_path") or backend_health.get("health_json_path") or backend_health.get("info_file"),
        "listener_heartbeat": listener_heartbeat.get("heartbeat_json_path"),
        "listener_health": listener_health.get("health_report_path") or listener_health.get("latest_health_report_path"),
        "listener_cycle": listener_cycle.get("report_json_path") or listener_health.get("latest_cycle_summary_path"),
        "shadow_runner": shadow_runner.get("report_json_path"),
        "attrition": attrition.get("report_json_path"),
        "track_b_observation_runner": track_b_observation_runner.get("report_json_path"),
        "track_b_readiness_check_runner": track_b_readiness_check_runner.get("report_json_path"),
        "track_b_strategy_rule_runner": track_b_strategy_rule_runner.get("report_json_path"),
        "track_b_strategy_paper_runner": track_b_strategy_paper_runner.get("report_json_path"),
        "track_b_multi_strategy_runtime_cycle": track_b_multi_strategy_runtime_cycle.get("report_json_path"),
        "track_b_shadow_monitor": track_b_shadow_monitor.get("report_json_path"),
        "track_b_shadow_monitor_heartbeat": (
            track_b_shadow_monitor_heartbeat.get("heartbeat_json_path") or track_b_shadow_monitor.get("heartbeat_json_path")
        ),
        "track_b_decision_journal_summary": track_b_multi_strategy_runtime_cycle.get("decision_journal_summary_path"),
        "databento_candle_observer": databento_candle_observer.get("report_json_path"),
        "databento_candle_observer_heartbeat": databento_candle_observer_heartbeat.get("heartbeat_json_path"),
        "strategy_signal_adapter": strategy_signal_adapter.get("report_json_path"),
        "candle_signal_producer": candle_signal_producer.get("report_json_path"),
        "signal_batch_writer": signal_batch_writer.get("report_json_path"),
        "readiness": readiness.get("report_json_path"),
        "recovery": recovery.get("report_json_path"),
        "preflight": preflight.get("report_json_path"),
        "quote": quote.get("report_json_path"),
        "track_b_paper_trade_ledger": track_b_paper_trade_summary.get("latest_trade_ledger_path"),
        "track_b_paper_trade_summary": track_b_paper_trade_summary.get("latest_trade_summary_path"),
        "track_b_live_position_status": track_b_live_position_status.get("latest_live_position_status_path")
        or track_b_paper_trade_summary.get("latest_live_position_status_path"),
        "track_b_pnl_summary": track_b_pnl_summary.get("latest_pnl_summary_path")
        or track_b_paper_trade_summary.get("latest_pnl_summary_path"),
    }
    shadow_monitor_instrument_reports = track_b_shadow_monitor.get("instrument_reports") or []
    primary_shadow_monitor_instrument = next(
        (
            item
            for item in shadow_monitor_instrument_reports
            if isinstance(item, Mapping) and item.get("runtime_chain_wired") is True
        ),
        {},
    )
    if not primary_shadow_monitor_instrument:
        primary_shadow_monitor_instrument = next(
            (
                item
                for item in shadow_monitor_instrument_reports
                if isinstance(item, Mapping) and item.get("instrument_family") == "MGC"
            ),
            {},
        )
    latest_shadow_monitor_lifecycle_path = track_b_shadow_monitor.get("latest_paper_lifecycle_report_path")
    shadow_monitor_has_guarded_lifecycle_provenance = (
        bool(latest_shadow_monitor_lifecycle_path)
        and latest_shadow_monitor_lifecycle_path != NOT_PROVIDED
    )
    shadow_monitor_live_money_ready = bool(track_b_shadow_monitor.get("live_money_readiness"))
    shadow_monitor_unproven_mutation = bool(
        (track_b_shadow_monitor.get("submit_attempted") or track_b_shadow_monitor.get("broker_state_mutated"))
        and not shadow_monitor_has_guarded_lifecycle_provenance
    )
    track_b_safety_warnings: list[str] = []
    if shadow_monitor_live_money_ready:
        track_b_safety_warnings.append("CRITICAL: live_money_readiness=true in Track B PAPER monitor status.")
    if shadow_monitor_unproven_mutation:
        track_b_safety_warnings.append(
            "REVIEW_REQUIRED: submit or broker mutation flag is true without guarded lifecycle provenance."
        )
    return {
        "schema_version": "track_b_operator_status_v1",
        "generated_at": now.isoformat(),
        "operator_status_id": status_id,
        "status_verdict": verdict.value,
        "track_b_startup_phase": track_b_startup_phase.get("phase") or NOT_PROVIDED,
        "track_b_startup_phase_classification": track_b_startup_phase.get("classification") or NOT_PROVIDED,
        "track_b_startup_phase_next_expected_phase": track_b_startup_phase.get("next_expected_phase") or NOT_PROVIDED,
        "track_b_startup_phase_diagnostic": track_b_startup_phase or NOT_PROVIDED,
        "track_b_startup_phase_submit_authority": (
            track_b_startup_phase.get("submit_authority") if track_b_startup_phase else NOT_PROVIDED
        ),
        "track_b_startup_phase_broker_mutation_allowed": (
            track_b_startup_phase.get("broker_mutation_allowed") if track_b_startup_phase else NOT_PROVIDED
        ),
        "track_b_startup_phase_paper_proof_invoked": (
            track_b_startup_phase.get("paper_proof_invoked") if track_b_startup_phase else NOT_PROVIDED
        ),
        "track_b_startup_phase_live_money_eligible": (
            track_b_startup_phase.get("live_money_eligible") if track_b_startup_phase else NOT_PROVIDED
        ),
        "backend_health_status": _backend_health_status(backend_health) or NOT_PROVIDED,
        "backend_health_ready": _backend_health_ready(backend_health) if backend_health else NOT_PROVIDED,
        "backend_health_url": backend_health.get("url") or backend_health.get("configured_url") or NOT_PROVIDED,
        "backend_health_host": backend_health.get("host") or NOT_PROVIDED,
        "backend_health_port": backend_health.get("port") if backend_health else NOT_PROVIDED,
        "backend_health_pid": backend_health.get("pid") or _nested_get(backend_health, ("health", "pid"), NOT_PROVIDED),
        "backend_health_api_dashboard_ok": _backend_health_api_dashboard_ok(backend_health) if backend_health else NOT_PROVIDED,
        "backend_health_operator_surface_ok": _backend_health_operator_surface_ok(backend_health) if backend_health else NOT_PROVIDED,
        "backend_health_startup_stable": _backend_health_startup_stable(backend_health) if backend_health else NOT_PROVIDED,
        "backend_health_error": backend_health.get("error") or backend_health.get("reason_detail") or NOT_PROVIDED,
        "listener_mode": listener_heartbeat.get("listener_mode") or NOT_PROVIDED,
        "listener_current_cycle_number": listener_heartbeat.get("current_cycle_number") if listener_heartbeat else NOT_PROVIDED,
        "listener_last_cycle_number": listener_heartbeat.get("last_cycle_number") if listener_heartbeat else NOT_PROVIDED,
        "listener_last_cycle_start_at": listener_heartbeat.get("last_cycle_start_at") or NOT_PROVIDED,
        "listener_last_cycle_end_at": listener_heartbeat.get("last_cycle_end_at") or NOT_PROVIDED,
        "listener_processed_cycles": listener_heartbeat.get("processed_cycles") if listener_heartbeat else NOT_PROVIDED,
        "listener_failed_cycles": listener_heartbeat.get("failed_cycles") if listener_heartbeat else NOT_PROVIDED,
        "listener_no_file_cycles": listener_heartbeat.get("no_file_cycles") if listener_heartbeat else NOT_PROVIDED,
        "listener_watch_exited_normally": listener_heartbeat.get("watch_exited_normally") if listener_heartbeat else NOT_PROVIDED,
        "listener_watch_verdict": listener_heartbeat.get("watch_verdict") or NOT_PROVIDED,
        "shadow_listener_health_verdict": listener_health.get("health_verdict") or NOT_PROVIDED,
        "listener_last_health_verdict": listener_heartbeat.get("last_health_verdict") or listener_health.get("health_verdict") or NOT_PROVIDED,
        "latest_listener_cycle_verdict": listener_cycle.get("listener_verdict") or listener_health.get("last_cycle_verdict") or listener_heartbeat.get("last_listener_verdict") or NOT_PROVIDED,
        "shadow_replay_runner_verdict": shadow_runner.get("runner_verdict") or NOT_PROVIDED,
        "attrition_report_verdict": attrition.get("attrition_report_verdict") or NOT_PROVIDED,
        "observation_runner_verdict": track_b_observation_runner.get("runner_verdict") or NOT_PROVIDED,
        "observation_runner_mode": track_b_observation_runner.get("mode") or track_b_observation_runner.get("runner_mode") or NOT_PROVIDED,
        "observation_runner_source_id": track_b_observation_runner.get("source_id") or NOT_PROVIDED,
        "observation_runner_current_cycle": track_b_observation_runner.get("current_cycle") if track_b_observation_runner else NOT_PROVIDED,
        "observation_runner_watch_exited_normally": track_b_observation_runner.get("watch_exited_normally") if track_b_observation_runner else NOT_PROVIDED,
        "observation_runner_required_next_action": track_b_observation_runner.get("required_next_action") or NOT_PROVIDED,
        "observation_runner_latest_report_path": track_b_observation_runner.get("latest_report_json_path") or NOT_PROVIDED,
        "observation_runner_latest_operator_status_path": track_b_observation_runner.get("latest_operator_status_path") or NOT_PROVIDED,
        "observation_runner_databento_observer_verdict": track_b_observation_runner.get("databento_observer_verdict") or NOT_PROVIDED,
        "observation_runner_strategy_adapter_verdict": track_b_observation_runner.get("strategy_adapter_verdict") or NOT_PROVIDED,
        "observation_runner_candle_producer_verdict": track_b_observation_runner.get("candle_producer_verdict") or NOT_PROVIDED,
        "observation_runner_signal_batch_writer_verdict": track_b_observation_runner.get("signal_batch_writer_verdict") or NOT_PROVIDED,
        "observation_runner_listener_verdict": track_b_observation_runner.get("listener_verdict") or NOT_PROVIDED,
        "observation_runner_listener_health_verdict": track_b_observation_runner.get("listener_health_verdict") or NOT_PROVIDED,
        "observation_runner_submit_allowed": track_b_observation_runner.get("submit_allowed") if track_b_observation_runner else NOT_PROVIDED,
        "observation_runner_submit_attempted": track_b_observation_runner.get("submit_attempted") if track_b_observation_runner else NOT_PROVIDED,
        "observation_runner_live_money_readiness": track_b_observation_runner.get("live_money_readiness") if track_b_observation_runner else NOT_PROVIDED,
        "readiness_check_runner_verdict": track_b_readiness_check_runner.get("runner_verdict") or NOT_PROVIDED,
        "readiness_check_runner_recovery_verdict": track_b_readiness_check_runner.get("recovery_verdict") or NOT_PROVIDED,
        "readiness_check_runner_preflight_verdict": track_b_readiness_check_runner.get("preflight_verdict") or NOT_PROVIDED,
        "readiness_check_runner_databento_observer_verdict": track_b_readiness_check_runner.get("databento_observer_verdict") or NOT_PROVIDED,
        "readiness_check_runner_current_quote_available": (
            track_b_readiness_check_runner.get("current_quote_available") if track_b_readiness_check_runner else NOT_PROVIDED
        ),
        "readiness_check_runner_quote_provider_mode": track_b_readiness_check_runner.get("quote_provider_mode") or NOT_PROVIDED,
        "readiness_check_runner_realtime_quote_received": (
            track_b_readiness_check_runner.get("realtime_quote_received") if track_b_readiness_check_runner else NOT_PROVIDED
        ),
        "readiness_check_runner_quote_freshness_verdict": track_b_readiness_check_runner.get("quote_freshness_verdict") or NOT_PROVIDED,
        "readiness_check_runner_wait_succeeded": (
            track_b_readiness_check_runner.get("wait_succeeded") if track_b_readiness_check_runner else NOT_PROVIDED
        ),
        "readiness_check_runner_readiness_verdict": track_b_readiness_check_runner.get("readiness_verdict") or NOT_PROVIDED,
        "readiness_check_runner_required_next_action": track_b_readiness_check_runner.get("required_next_action") or NOT_PROVIDED,
        "readiness_check_runner_latest_report_path": track_b_readiness_check_runner.get("latest_report_json_path") or NOT_PROVIDED,
        "readiness_check_runner_submit_allowed": track_b_readiness_check_runner.get("submit_allowed") if track_b_readiness_check_runner else NOT_PROVIDED,
        "readiness_check_runner_submit_attempted": track_b_readiness_check_runner.get("submit_attempted") if track_b_readiness_check_runner else NOT_PROVIDED,
        "readiness_check_runner_paper_proof_cli_called": (
            track_b_readiness_check_runner.get("paper_proof_cli_called") if track_b_readiness_check_runner else NOT_PROVIDED
        ),
        "readiness_check_runner_live_money_readiness": (
            track_b_readiness_check_runner.get("live_money_readiness") if track_b_readiness_check_runner else NOT_PROVIDED
        ),
        "strategy_rule_runner_verdict": track_b_strategy_rule_runner.get("strategy_rule_runner_verdict") or NOT_PROVIDED,
        "strategy_rule_id": track_b_strategy_rule_runner.get("strategy_rule_id") or NOT_PROVIDED,
        "strategy_rule_name": track_b_strategy_rule_runner.get("rule_name") or NOT_PROVIDED,
        "strategy_rule_mode": track_b_strategy_rule_runner.get("rule_mode") or NOT_PROVIDED,
        "strategy_rule_decision": track_b_strategy_rule_runner.get("decision") or NOT_PROVIDED,
        "strategy_rule_decision_reason": track_b_strategy_rule_runner.get("decision_reason") or NOT_PROVIDED,
        "strategy_rule_signal_emitted": track_b_strategy_rule_runner.get("signal_emitted") if track_b_strategy_rule_runner else NOT_PROVIDED,
        "strategy_rule_signal_direction": track_b_strategy_rule_runner.get("signal_direction") or NOT_PROVIDED,
        "strategy_rule_input_quote_provider_mode": track_b_strategy_rule_runner.get("input_quote_provider_mode") or NOT_PROVIDED,
        "strategy_rule_realtime_quote_received": (
            track_b_strategy_rule_runner.get("realtime_quote_received") if track_b_strategy_rule_runner else NOT_PROVIDED
        ),
        "strategy_rule_current_quote_available": (
            track_b_strategy_rule_runner.get("current_quote_available") if track_b_strategy_rule_runner else NOT_PROVIDED
        ),
        "strategy_rule_downstream_strategy_adapter_report_path": (
            track_b_strategy_rule_runner.get("downstream_strategy_adapter_report_path") or NOT_PROVIDED
        ),
        "strategy_rule_downstream_candle_producer_report_path": (
            track_b_strategy_rule_runner.get("downstream_candle_producer_report_path") or NOT_PROVIDED
        ),
        "strategy_rule_downstream_signal_batch_writer_report_path": (
            track_b_strategy_rule_runner.get("downstream_signal_batch_writer_report_path") or NOT_PROVIDED
        ),
        "strategy_rule_output_batch_path": track_b_strategy_rule_runner.get("output_batch_path") or NOT_PROVIDED,
        "strategy_rule_paper_proof_cli_called": (
            track_b_strategy_rule_runner.get("paper_proof_cli_called") if track_b_strategy_rule_runner else NOT_PROVIDED
        ),
        "strategy_rule_submit_allowed": track_b_strategy_rule_runner.get("submit_allowed") if track_b_strategy_rule_runner else NOT_PROVIDED,
        "strategy_rule_submit_attempted": track_b_strategy_rule_runner.get("submit_attempted") if track_b_strategy_rule_runner else NOT_PROVIDED,
        "strategy_rule_live_money_readiness": (
            track_b_strategy_rule_runner.get("live_money_readiness") if track_b_strategy_rule_runner else NOT_PROVIDED
        ),
        "strategy_paper_runner_verdict": track_b_strategy_paper_runner.get("strategy_paper_runner_verdict") or NOT_PROVIDED,
        "strategy_paper_rule_decision": track_b_strategy_paper_runner.get("rule_decision") or NOT_PROVIDED,
        "strategy_paper_signal_emitted": track_b_strategy_paper_runner.get("signal_emitted") if track_b_strategy_paper_runner else NOT_PROVIDED,
        "strategy_paper_signal_direction": track_b_strategy_paper_runner.get("signal_direction") or NOT_PROVIDED,
        "strategy_paper_readiness_runner_verdict": track_b_strategy_paper_runner.get("readiness_runner_verdict") or NOT_PROVIDED,
        "strategy_paper_readiness_verdict": track_b_strategy_paper_runner.get("readiness_verdict") or NOT_PROVIDED,
        "strategy_paper_submit_requested": track_b_strategy_paper_runner.get("paper_submit_requested") if track_b_strategy_paper_runner else NOT_PROVIDED,
        "strategy_trade_intent_created": track_b_strategy_paper_runner.get("strategy_trade_intent_created") if track_b_strategy_paper_runner else NOT_PROVIDED,
        "strategy_trade_intent_classification": track_b_strategy_paper_runner.get("strategy_trade_intent_classification") or NOT_PROVIDED,
        "strategy_trade_intent_id": track_b_strategy_paper_runner.get("strategy_trade_intent_id") or NOT_PROVIDED,
        "strategy_trade_intent_report_path": track_b_strategy_paper_runner.get("strategy_trade_intent_report_path") or NOT_PROVIDED,
        "strategy_trade_intent_blocked_reason": track_b_strategy_paper_runner.get("intent_blocked_reason") or NOT_PROVIDED,
        "strategy_trade_intent_lifecycle_mode": track_b_strategy_paper_runner.get("lifecycle_mode") or NOT_PROVIDED,
        "strategy_managed_exit_policy_id": track_b_strategy_paper_runner.get("managed_exit_policy_id") or NOT_PROVIDED,
        "strategy_managed_exit_policy_max_completed_5m_bars": track_b_strategy_paper_runner.get(
            "managed_exit_policy_max_completed_5m_bars",
            NOT_PROVIDED,
        ),
        "strategy_managed_open_position_age_completed_5m_bars": track_b_strategy_paper_runner.get(
            "managed_open_position_age_completed_5m_bars",
            NOT_PROVIDED,
        ),
        "strategy_managed_expected_exit_condition": track_b_strategy_paper_runner.get("managed_expected_exit_condition")
        or NOT_PROVIDED,
        "strategy_managed_close_intent_status": track_b_strategy_paper_runner.get("managed_close_intent_status")
        or NOT_PROVIDED,
        "strategy_paper_proof_invoked": track_b_strategy_paper_runner.get("paper_proof_invoked") if track_b_strategy_paper_runner else NOT_PROVIDED,
        "strategy_paper_proof_classification": track_b_strategy_paper_runner.get("paper_proof_classification") or NOT_PROVIDED,
        "strategy_paper_proof_report_path": track_b_strategy_paper_runner.get("paper_proof_report_path") or NOT_PROVIDED,
        "strategy_paper_final_flat": track_b_strategy_paper_runner.get("final_flat") if track_b_strategy_paper_runner else NOT_PROVIDED,
        "strategy_paper_required_next_action": track_b_strategy_paper_runner.get("required_next_action") or NOT_PROVIDED,
        "strategy_paper_submit_allowed": track_b_strategy_paper_runner.get("submit_allowed") if track_b_strategy_paper_runner else NOT_PROVIDED,
        "strategy_paper_submit_attempted": track_b_strategy_paper_runner.get("submit_attempted") if track_b_strategy_paper_runner else NOT_PROVIDED,
        "strategy_paper_live_money_readiness": track_b_strategy_paper_runner.get("live_money_readiness") if track_b_strategy_paper_runner else NOT_PROVIDED,
        "latest_shadow_monitor_verdict": track_b_shadow_monitor.get("monitor_verdict") or NOT_PROVIDED,
        "latest_shadow_monitor_cycle_id": track_b_shadow_monitor.get("cycle_id") or NOT_PROVIDED,
        "latest_shadow_monitor_report_path": track_b_shadow_monitor.get("report_json_path") or NOT_PROVIDED,
        "latest_shadow_monitor_completed_at": track_b_shadow_monitor.get("completed_at") or NOT_PROVIDED,
        "shadow_monitor_mode": (
            track_b_shadow_monitor.get("monitor_mode") or track_b_shadow_monitor.get("mode") or NOT_PROVIDED
        ),
        "shadow_monitor_launchd_label": (
            "com.mgc.trackb.paper-monitor"
            if (track_b_shadow_monitor.get("monitor_mode") or track_b_shadow_monitor.get("mode")) == "PAPER"
            else NOT_PROVIDED
        ),
        "shadow_monitor_pid": (
            track_b_shadow_monitor.get("pid") or track_b_shadow_monitor_heartbeat.get("pid") or NOT_PROVIDED
        ),
        "shadow_monitor_runtime_decision_source": (
            track_b_shadow_monitor.get("runtime_decision_source")
            or track_b_shadow_monitor.get("runtime_data_source")
            or primary_shadow_monitor_instrument.get("runtime_decision_source")
            or primary_shadow_monitor_instrument.get("runtime_data_source")
            or NOT_PROVIDED
        ),
        "shadow_monitor_paper_trading_enabled": (
            track_b_shadow_monitor.get("paper_trading_enabled") if track_b_shadow_monitor else NOT_PROVIDED
        ),
        "shadow_monitor_paper_on_signal": (
            track_b_shadow_monitor.get("paper_on_signal") if track_b_shadow_monitor else NOT_PROVIDED
        ),
        "shadow_monitor_paper_trades_attempted_count": (
            track_b_shadow_monitor.get("paper_trades_attempted_count") if track_b_shadow_monitor else NOT_PROVIDED
        ),
        "shadow_monitor_latest_paper_lifecycle_report_path": (
            track_b_shadow_monitor.get("latest_paper_lifecycle_report_path") or NOT_PROVIDED
        ),
        "shadow_monitor_latest_broker_state_classification": (
            track_b_shadow_monitor.get("latest_broker_state_classification") or NOT_PROVIDED
        ),
        "shadow_monitor_live_feed_pid": primary_shadow_monitor_instrument.get("live_feed_pid") or NOT_PROVIDED,
        "shadow_monitor_live_feed_connected": (
            primary_shadow_monitor_instrument.get("live_feed_connected")
            if primary_shadow_monitor_instrument
            else NOT_PROVIDED
        ),
        "shadow_monitor_live_feed_status": primary_shadow_monitor_instrument.get("live_feed_status") or NOT_PROVIDED,
        "shadow_monitor_live_feed_subscription_status": (
            primary_shadow_monitor_instrument.get("live_feed_subscription_status") or NOT_PROVIDED
        ),
        "shadow_monitor_live_feed_heartbeat_age_seconds": (
            primary_shadow_monitor_instrument.get("live_feed_heartbeat_age_seconds")
            if primary_shadow_monitor_instrument
            else NOT_PROVIDED
        ),
        "shadow_monitor_live_feed_strategy_ready": (
            primary_shadow_monitor_instrument.get("live_feed_strategy_ready")
            if primary_shadow_monitor_instrument
            else NOT_PROVIDED
        ),
        "shadow_monitor_live_feed_warmup_1m_count": (
            primary_shadow_monitor_instrument.get("live_feed_warmup_1m_count")
            if primary_shadow_monitor_instrument
            else NOT_PROVIDED
        ),
        "shadow_monitor_live_feed_warmup_completed_5m_count": (
            primary_shadow_monitor_instrument.get("live_feed_warmup_completed_5m_count")
            if primary_shadow_monitor_instrument
            else NOT_PROVIDED
        ),
        "shadow_monitor_live_feed_required_1m_count": (
            primary_shadow_monitor_instrument.get("live_feed_required_1m_count")
            if primary_shadow_monitor_instrument
            else NOT_PROVIDED
        ),
        "shadow_monitor_live_feed_required_completed_5m_count": (
            primary_shadow_monitor_instrument.get("live_feed_required_completed_5m_count")
            if primary_shadow_monitor_instrument
            else NOT_PROVIDED
        ),
        "shadow_monitor_live_feed_blocker": primary_shadow_monitor_instrument.get("live_feed_blocker") or NOT_PROVIDED,
        "shadow_monitor_running": (
            track_b_shadow_monitor_heartbeat.get("monitor_running")
            if track_b_shadow_monitor_heartbeat
            else NOT_PROVIDED
        ),
        "shadow_monitor_heartbeat_generated_at": track_b_shadow_monitor_heartbeat.get("generated_at") or NOT_PROVIDED,
        "shadow_monitor_heartbeat_path": (
            track_b_shadow_monitor_heartbeat.get("heartbeat_json_path")
            or track_b_shadow_monitor.get("heartbeat_json_path")
            or NOT_PROVIDED
        ),
        "shadow_monitor_instrument_reports": shadow_monitor_instrument_reports,
        "shadow_monitor_instrument_families": track_b_shadow_monitor.get("instrument_families") or [],
        "shadow_monitor_evaluated_strategy_count": (
            track_b_shadow_monitor.get("evaluated_strategy_count") if track_b_shadow_monitor else NOT_PROVIDED
        ),
        "shadow_monitor_candidate_signals": track_b_shadow_monitor.get("candidate_signals") or [],
        "shadow_monitor_suppressed_signals": track_b_shadow_monitor.get("suppressed_signals") or [],
        "shadow_monitor_arbitration_result": track_b_shadow_monitor.get("arbitration_result") or {},
        "shadow_monitor_strategy_trade_intent_created": track_b_shadow_monitor.get("latest_strategy_trade_intent_created") if track_b_shadow_monitor else NOT_PROVIDED,
        "shadow_monitor_strategy_trade_intent_classification": track_b_shadow_monitor.get("latest_strategy_trade_intent_classification") or NOT_PROVIDED,
        "shadow_monitor_strategy_trade_intent_path": track_b_shadow_monitor.get("latest_strategy_trade_intent_path") or NOT_PROVIDED,
        "shadow_monitor_strategy_trade_intent_blocked_reason": track_b_shadow_monitor.get("latest_strategy_trade_intent_blocked_reason") or NOT_PROVIDED,
        "shadow_monitor_managed_exit_policy_id": track_b_shadow_monitor.get("latest_managed_exit_policy_id") or NOT_PROVIDED,
        "shadow_monitor_managed_expected_exit_condition": track_b_shadow_monitor.get("latest_managed_expected_exit_condition")
        or NOT_PROVIDED,
        "shadow_monitor_managed_close_intent_status": track_b_shadow_monitor.get("latest_managed_close_intent_status")
        or NOT_PROVIDED,
        "shadow_monitor_decision_journal_tier_counts": track_b_shadow_monitor.get("decision_journal_tier_counts") or {},
        "shadow_monitor_submit_allowed": track_b_shadow_monitor.get("submit_allowed") if track_b_shadow_monitor else NOT_PROVIDED,
        "shadow_monitor_submit_attempted": track_b_shadow_monitor.get("submit_attempted") if track_b_shadow_monitor else NOT_PROVIDED,
        "shadow_monitor_paper_proof_invoked": track_b_shadow_monitor.get("paper_proof_invoked") if track_b_shadow_monitor else NOT_PROVIDED,
        "shadow_monitor_broker_state_mutated": track_b_shadow_monitor.get("broker_state_mutated") if track_b_shadow_monitor else NOT_PROVIDED,
        "shadow_monitor_live_money_readiness": track_b_shadow_monitor.get("live_money_readiness") if track_b_shadow_monitor else NOT_PROVIDED,
        "track_b_safety_critical": bool(shadow_monitor_live_money_ready or shadow_monitor_unproven_mutation),
        "track_b_safety_review_required": bool(shadow_monitor_unproven_mutation),
        "track_b_safety_warnings": track_b_safety_warnings,
        "track_b_safety_primary_warning": track_b_safety_warnings[0] if track_b_safety_warnings else NOT_PROVIDED,
        "multi_strategy_runtime_cycle_verdict": track_b_multi_strategy_runtime_cycle.get("multi_strategy_runtime_cycle_verdict") or NOT_PROVIDED,
        "multi_strategy_runtime_cycle_mode": track_b_multi_strategy_runtime_cycle.get("mode") or NOT_PROVIDED,
        "multi_strategy_runtime_cycle_source_id": track_b_multi_strategy_runtime_cycle.get("source_id") or NOT_PROVIDED,
        "multi_strategy_evaluated_strategies": track_b_multi_strategy_runtime_cycle.get("evaluated_strategies") or [],
        "multi_strategy_candidate_signals": track_b_multi_strategy_runtime_cycle.get("candidate_signals") or [],
        "multi_strategy_suppressed_signals": track_b_multi_strategy_runtime_cycle.get("suppressed_signals") or [],
        "multi_strategy_arbitration_result": track_b_multi_strategy_runtime_cycle.get("arbitration_result") or {},
        "multi_strategy_chosen_signal": track_b_multi_strategy_runtime_cycle.get("chosen_signal") or {},
        "multi_strategy_chosen_strategy_id": track_b_multi_strategy_runtime_cycle.get("chosen_strategy_id") or NOT_PROVIDED,
        "multi_strategy_reason_no_signal_chosen": track_b_multi_strategy_runtime_cycle.get("reason_no_signal_chosen") or NOT_PROVIDED,
        "multi_strategy_readiness_invoked": (
            track_b_multi_strategy_runtime_cycle.get("readiness_invoked") if track_b_multi_strategy_runtime_cycle else NOT_PROVIDED
        ),
        "multi_strategy_paper_proof_invoked": (
            track_b_multi_strategy_runtime_cycle.get("paper_proof_invoked") if track_b_multi_strategy_runtime_cycle else NOT_PROVIDED
        ),
        "multi_strategy_submit_attempted": (
            track_b_multi_strategy_runtime_cycle.get("submit_attempted") if track_b_multi_strategy_runtime_cycle else NOT_PROVIDED
        ),
        "multi_strategy_broker_state_mutated": (
            track_b_multi_strategy_runtime_cycle.get("broker_state_mutated") if track_b_multi_strategy_runtime_cycle else NOT_PROVIDED
        ),
        "multi_strategy_live_money_readiness": (
            track_b_multi_strategy_runtime_cycle.get("live_money_readiness") if track_b_multi_strategy_runtime_cycle else NOT_PROVIDED
        ),
        "track_b_decision_journal_invoked": (
            track_b_multi_strategy_runtime_cycle.get("decision_journal_invoked") if track_b_multi_strategy_runtime_cycle else NOT_PROVIDED
        ),
        "track_b_decision_journal_summary_path": (
            track_b_multi_strategy_runtime_cycle.get("decision_journal_summary_path") or NOT_PROVIDED
        ),
        "track_b_decision_journal_active_path": (
            track_b_multi_strategy_runtime_cycle.get("decision_journal_active_path") or NOT_PROVIDED
        ),
        "track_b_decision_journal_heartbeat_path": (
            track_b_multi_strategy_runtime_cycle.get("decision_journal_heartbeat_path") or NOT_PROVIDED
        ),
        "track_b_decision_journal_full_records_written": (
            track_b_multi_strategy_runtime_cycle.get("decision_journal_full_records_written")
            if track_b_multi_strategy_runtime_cycle
            else NOT_PROVIDED
        ),
        "track_b_decision_journal_tier_counts": (
            track_b_multi_strategy_runtime_cycle.get("decision_journal_tier_counts") or {}
        ),
        "track_b_decision_journal_error": (
            track_b_multi_strategy_runtime_cycle.get("decision_journal_error") or NOT_PROVIDED
        ),
        "track_b_paper_results_source": (
            track_b_paper_trade_summary.get("source")
            or track_b_live_position_status.get("source")
            or track_b_pnl_summary.get("source")
            or "NO_TRACK_B_PAPER_TRADES_RECORDED"
        ),
        "track_b_paper_results_broker_reconciled": (
            track_b_live_position_status.get("broker_reconciled")
            if track_b_live_position_status
            else False
        ),
        "paper_trades_attempted_count": (
            track_b_paper_trade_summary.get("paper_trades_attempted_count")
            if track_b_paper_trade_summary
            else track_b_shadow_monitor.get("paper_trades_attempted_count", 0)
        ),
        "completed_trade_count": track_b_paper_trade_summary.get("completed_trade_count", track_b_paper_trade_summary.get("closed_trade_count", 0)),
        "managed_strategy_trade_count": track_b_paper_trade_summary.get("managed_strategy_trade_count", 0),
        "meaningful_strategy_trade_count": track_b_paper_trade_summary.get("meaningful_strategy_trade_count", 0),
        "proof_canary_trade_count": track_b_paper_trade_summary.get("proof_canary_trade_count", 0),
        "archived_manual_flat_count": track_b_paper_trade_summary.get("archived_manual_flat_count", 0),
        "proof_canary_excluded_from_meaningful_strategy_counts": track_b_paper_trade_summary.get(
            "proof_canary_excluded_from_meaningful_strategy_counts",
            True,
        ),
        "track_b_recent_trades": track_b_paper_trade_summary.get("recent_trades") or [],
        "open_position_count": (
            track_b_live_position_status.get("open_position_count")
            if track_b_live_position_status
            else 0
        ),
        "realized_pnl_today": track_b_pnl_summary.get("total_realized_pnl_today", "0"),
        "realized_pnl_session": track_b_pnl_summary.get("total_realized_pnl_session", "0"),
        "realized_pnl_week": track_b_pnl_summary.get("total_realized_pnl_week", "0"),
        "realized_pnl_month": track_b_pnl_summary.get("total_realized_pnl_month", "0"),
        "realized_pnl_ytd": track_b_pnl_summary.get("total_realized_pnl_ytd", "0"),
        "unrealized_pnl": track_b_pnl_summary.get("total_unrealized_pnl", "0"),
        "last_trade_strategy": track_b_pnl_summary.get("last_trade_strategy") or NOT_PROVIDED,
        "last_trade_pnl": track_b_pnl_summary.get("last_trade_pnl") if track_b_pnl_summary else NOT_PROVIDED,
        "latest_trade_ledger_path": (
            track_b_paper_trade_summary.get("latest_trade_ledger_path")
            or str(DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT / "track_b_paper_trade_ledger.jsonl")
        ),
        "latest_live_position_status_path": (
            track_b_live_position_status.get("latest_live_position_status_path")
            or track_b_paper_trade_summary.get("latest_live_position_status_path")
            or str(DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON)
        ),
        "latest_pnl_summary_path": (
            track_b_pnl_summary.get("latest_pnl_summary_path")
            or track_b_paper_trade_summary.get("latest_pnl_summary_path")
            or str(DEFAULT_TRACK_B_PNL_SUMMARY_JSON)
        ),
        "review_required_count": (
            track_b_pnl_summary.get("review_required_count")
            if track_b_pnl_summary
            else track_b_paper_trade_summary.get("review_required_count", 0)
        ),
        "track_b_positions_by_instrument": track_b_live_position_status.get("positions_by_instrument") or {},
        "track_b_positions_by_strategy": track_b_live_position_status.get("positions_by_strategy") or {},
        "track_b_pnl_by_strategy": track_b_pnl_summary.get("by_strategy") or {},
        "track_b_pnl_by_instrument": track_b_pnl_summary.get("by_instrument") or {},
        "track_b_paper_results_warning": (
            track_b_live_position_status.get("broker_truth_warning")
            or "No Track B PAPER trade ledger rows have been recorded yet."
        ),
        "databento_observer_verdict": databento_candle_observer.get("observer_verdict") or NOT_PROVIDED,
        "databento_contract_key": databento_candle_observer.get("contract_key") or databento_candle_observer_heartbeat.get("contract_key") or NOT_PROVIDED,
        "databento_symbol": databento_candle_observer.get("databento_continuous_symbol") or databento_candle_observer_heartbeat.get("databento_continuous_symbol") or NOT_PROVIDED,
        "databento_dataset": databento_candle_observer.get("dataset") or databento_candle_observer_heartbeat.get("dataset") or NOT_PROVIDED,
        "databento_timeframe": databento_candle_observer.get("timeframe") or NOT_PROVIDED,
        "databento_source_id": databento_candle_observer.get("source_id") or NOT_PROVIDED,
        "databento_event_timestamp": databento_candle_observer.get("event_timestamp") or databento_candle_observer.get("candle_timestamp") or databento_candle_observer_heartbeat.get("last_event_timestamp") or NOT_PROVIDED,
        "databento_output_event_path": databento_candle_observer.get("output_candle_event_path") or databento_candle_observer_heartbeat.get("output_event_path") or NOT_PROVIDED,
        "databento_observer_submit_allowed": databento_candle_observer.get("submit_allowed") if databento_candle_observer else NOT_PROVIDED,
        "databento_observer_submit_attempted": databento_candle_observer.get("submit_attempted") if databento_candle_observer else NOT_PROVIDED,
        "databento_observer_live_money_readiness": databento_candle_observer.get("live_money_readiness") if databento_candle_observer else NOT_PROVIDED,
        "databento_observer_mode": databento_candle_observer_heartbeat.get("observer_mode") or databento_candle_observer.get("observer_mode") or NOT_PROVIDED,
        "databento_observer_current_cycle": databento_candle_observer_heartbeat.get("current_cycle_number") if databento_candle_observer_heartbeat else NOT_PROVIDED,
        "databento_observer_processed_cycles": databento_candle_observer_heartbeat.get("processed_cycles") if databento_candle_observer_heartbeat else NOT_PROVIDED,
        "databento_observer_no_data_cycles": databento_candle_observer_heartbeat.get("no_data_cycles") if databento_candle_observer_heartbeat else NOT_PROVIDED,
        "databento_observer_error_cycles": databento_candle_observer_heartbeat.get("error_cycles") if databento_candle_observer_heartbeat else NOT_PROVIDED,
        "databento_observer_last_verdict": databento_candle_observer_heartbeat.get("last_observer_verdict") or NOT_PROVIDED,
        "databento_observer_watch_exited_normally": databento_candle_observer_heartbeat.get("watch_exited_normally") if databento_candle_observer_heartbeat else NOT_PROVIDED,
        "strategy_adapter_verdict": strategy_signal_adapter.get("adapter_verdict") or NOT_PROVIDED,
        "strategy_id": strategy_signal_adapter.get("strategy_id") or NOT_PROVIDED,
        "signal_family": strategy_signal_adapter.get("signal_family") or NOT_PROVIDED,
        "strategy_source_id": strategy_signal_adapter.get("source_id") or NOT_PROVIDED,
        "strategy_batch_id": strategy_signal_adapter.get("batch_id") or NOT_PROVIDED,
        "strategy_signal_count": strategy_signal_adapter.get("signal_count") if strategy_signal_adapter else NOT_PROVIDED,
        "strategy_output_batch_path": strategy_signal_adapter.get("output_batch_path") or NOT_PROVIDED,
        "strategy_downstream_candle_producer_report_path": strategy_signal_adapter.get("candle_producer_report_path") or NOT_PROVIDED,
        "strategy_downstream_writer_report_path": strategy_signal_adapter.get("downstream_writer_report_path") or NOT_PROVIDED,
        "candle_producer_verdict": candle_signal_producer.get("producer_verdict") or NOT_PROVIDED,
        "candle_source_id": candle_signal_producer.get("source_id") or NOT_PROVIDED,
        "candle_batch_id": candle_signal_producer.get("batch_id") or NOT_PROVIDED,
        "candle_signal_count": candle_signal_producer.get("signal_count") if candle_signal_producer else NOT_PROVIDED,
        "candle_output_batch_path": candle_signal_producer.get("output_batch_path") or NOT_PROVIDED,
        "candle_downstream_writer_report_path": candle_signal_producer.get("writer_report_path") or NOT_PROVIDED,
        "signal_batch_writer_verdict": signal_batch_writer.get("signal_batch_writer_verdict") or NOT_PROVIDED,
        "signal_batch_writer_batch_file_written": signal_batch_writer.get("batch_file_written") if signal_batch_writer else NOT_PROVIDED,
        "signal_batch_writer_batch_json_path": signal_batch_writer.get("batch_json_path") or NOT_PROVIDED,
        "signal_batch_writer_total_signals": signal_batch_writer.get("total_signals") if signal_batch_writer else NOT_PROVIDED,
        "recent_writer_output_present": bool(signal_batch_writer.get("batch_json_path")) if signal_batch_writer else False,
        "readiness_verdict": readiness.get("final_readiness_verdict") or NOT_PROVIDED,
        "recovery_verdict": _recovery_verdict(recovery) or NOT_PROVIDED,
        "preflight_verdict": preflight.get("final_readiness_verdict") or preflight.get("classification") or NOT_PROVIDED,
        "quote_status": quote.get("quote_status") or quote.get("classification") or NOT_PROVIDED,
        "primary_blocker": primary_blocker,
        "secondary_blockers": _secondary_blockers(reports, missing),
        "required_next_action": required_next_action,
        "reports_considered": considered,
        "reports_missing": missing,
        "latest_output_paths": latest_output_paths,
        "engine_running_inferred": False,
        "engine_running_inference": "UNKNOWN_UNLESS_EXPLICITLY_PROVIDED",
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "observer_status_only": True,
        "dashboard_is_not_authority": True,
        "listener_invoked": False,
        "runner_invoked": False,
        "paper_proof_cli_called": False,
        "broker_connection_attempted": False,
        "market_data_connection_attempted": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_operator_status_summary.json"),
    }


def _secondary_blockers(reports: Mapping[str, Mapping[str, Any] | None], missing: list[str]) -> list[str]:
    blockers = [f"missing_report:{name}" for name in missing]
    for report in reports.values():
        if not report:
            continue
        if report.get("primary_blocker"):
            blockers.append(str(report["primary_blocker"]))
        blockers.extend(str(item) for item in report.get("secondary_blockers") or ())
        if report.get("last_primary_blocker"):
            blockers.append(str(report["last_primary_blocker"]))
    return _dedupe(blockers)


def _recovery_verdict(report: Mapping[str, Any]) -> str:
    return str(report.get("final_readiness_verdict") or report.get("classification") or "")


def _readiness_check_runner_ready_for_paper_proof_review(report: Mapping[str, Any]) -> bool:
    if not report:
        return False
    return (
        report.get("runner_verdict") == "TRACK_B_READINESS_CHECK_READY_FOR_PAPER_PROOF_REVIEW"
        and report.get("recovery_verdict") == "RECOVERY_READY_CLEAN"
        and report.get("preflight_verdict") == "READY_READ_ONLY"
        and report.get("quote_provider_mode") == "REALTIME"
        and report.get("realtime_quote_received") is True
        and report.get("current_quote_available") is True
        and report.get("readiness_verdict") == "READY_FOR_PAPER_PROOF"
        and report.get("submit_attempted") is False
        and report.get("paper_proof_cli_called") is False
        and report.get("live_money_readiness") is False
    )


def _all_missing(reports: Mapping[str, Mapping[str, Any] | None]) -> bool:
    return all(report is None for name, report in reports.items() if name != "track_b_startup_phase")


def _all_track_b_runtime_reports_missing(reports: Mapping[str, Mapping[str, Any] | None]) -> bool:
    return all(
        report is None
        for name, report in reports.items()
        if name not in {"backend_health", "track_b_startup_phase"}
    )


def _backend_health_ok(report: Mapping[str, Any]) -> bool:
    return _backend_health_status(report) == "ok" and _backend_health_ready(report) is True


def _backend_health_status(report: Mapping[str, Any]) -> str:
    direct = str(report.get("status") or "").strip()
    if direct:
        return direct
    nested = str(_nested_get(report, ("health", "status"), "") or "").strip()
    if nested:
        return nested
    readiness_state = str(report.get("readiness_state") or "").strip()
    if readiness_state == "READY":
        return "ok"
    if readiness_state:
        return readiness_state.lower()
    return ""


def _backend_health_ready(report: Mapping[str, Any]) -> bool:
    if isinstance(report.get("ready"), bool):
        return bool(report["ready"])
    nested_ready = _nested_get(report, ("health", "ready"), None)
    if isinstance(nested_ready, bool):
        return nested_ready
    launch_allowed = report.get("launch_allowed")
    listener_reachable = _nested_get(report, ("listener", "reachable"), None)
    if isinstance(launch_allowed, bool) and isinstance(listener_reachable, bool):
        return launch_allowed and listener_reachable
    return False


def _backend_health_api_dashboard_ok(report: Mapping[str, Any]) -> Any:
    direct = _nested_get(report, ("checks", "api_dashboard_responding", "ok"), None)
    if direct is not None:
        return direct
    payload_reachable = _nested_get(report, ("payload", "reachable"), None)
    payload_json_valid = _nested_get(report, ("payload", "json_valid"), None)
    if isinstance(payload_reachable, bool) and isinstance(payload_json_valid, bool):
        return payload_reachable and payload_json_valid
    listener_reachable = _nested_get(report, ("listener", "reachable"), None)
    if isinstance(listener_reachable, bool):
        return listener_reachable
    return NOT_PROVIDED


def _backend_health_operator_surface_ok(report: Mapping[str, Any]) -> Any:
    direct = _nested_get(report, ("checks", "operator_surface_loadable", "ok"), None)
    if direct is not None:
        return direct
    startup_control_plane_present = _nested_get(report, ("payload", "startup_control_plane_present"), None)
    if isinstance(startup_control_plane_present, bool):
        return startup_control_plane_present
    return NOT_PROVIDED


def _backend_health_startup_stable(report: Mapping[str, Any]) -> Any:
    direct = _nested_get(report, ("checks", "startup_convergence_stable", "ok"), None)
    if direct is not None:
        return direct
    convergence_stable_ready = _nested_get(report, ("control_plane", "convergence_stable_ready"), None)
    if isinstance(convergence_stable_ready, bool):
        return convergence_stable_ready
    return NOT_PROVIDED


def _dedupe(values: list[str]) -> list[str]:
    unique: list[str] = []
    seen: set[str] = set()
    for value in values:
        if value and value not in seen:
            seen.add(value)
            unique.append(value)
    return unique


def _nested_get(report: Mapping[str, Any], path: tuple[str, ...], default: Any) -> Any:
    current: Any = report
    for key in path:
        if not isinstance(current, Mapping):
            return default
        current = current.get(key)
    return default if current is None else current


def _read_json(path: Path | None) -> dict[str, Any] | None:
    if path is None:
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value


def _read_optional_json(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.exists():
        return None
    return _read_json(path)
