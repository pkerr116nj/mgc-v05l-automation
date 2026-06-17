"""Cadenced close-only service for Track B managed exits.

The service owns scheduling only. Broker mutation, when explicitly enabled, is
delegated to the guarded managed-exit actuator.
"""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.models import require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_contract_identity import normalize_track_b_contract_row
from mgc_v05l.execution_core.track_b_managed_exit_pipeline_dry_run import (
    TrackBManagedExitPipelineDryRunConfig,
    build_track_b_managed_exit_pipeline_dry_run_report,
)
from mgc_v05l.execution_core.track_b_managed_exit_actuator import (
    MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING,
    MANAGED_EXIT_ACTUATOR_DRY_RUN_READY,
    MANAGED_EXIT_ACTUATOR_PARTIAL,
    TrackBManagedExitActuatorConfig,
)
from mgc_v05l.execution_core.track_b_broker_effect_recognition import BROKER_EFFECT_OBSERVED
from mgc_v05l.execution_core.track_b_managed_order_modify_in_place import (
    IbkrPaperManagedOrderModifyAdapter,
    ManagedOrderModifyInPlaceConfig,
    run_track_b_managed_order_modify_in_place,
)
from mgc_v05l.execution_core.track_b_managed_order_registry import (
    DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_order_adjustment_planner import (
    MODIFY_IN_PLACE_ELIGIBLE,
    TrackBOrderAdjustmentPlannerConfig,
    build_track_b_order_adjustment_plan,
    write_track_b_order_adjustment_plan,
)
from mgc_v05l.execution_core.track_b_post_broker_mutation_refresh import (
    PostBrokerMutationRefreshConfig,
    post_position_order_change_refresh,
)
from mgc_v05l.execution_core.track_b_strategy_attrition_funnel import (
    events_from_managed_exit_service_status,
    try_record_strategy_funnel_events,
)
from mgc_v05l.execution_core.track_b_live_trade_registry import load_live_trade_registry_records


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STATUS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "managed_exit_service"
    / "latest_managed_exit_service_status.json"
)
DEFAULT_HEARTBEAT_PATH = Path("var") / "track_b_managed_exit_service_heartbeat.json"
DEFAULT_CADENCE_SECONDS = 45.0
DEFAULT_BROKER_POSITIONS_SNAPSHOT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json"
)
DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_OPERATOR_MANAGED_POSITION_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_track_b_managed_positions.json"
)
DEFAULT_STRATEGY_LIFECYCLE_ROOT = (
    Path("outputs") / "track_b_execution_core" / "track_b_strategy_managed_paper_lifecycle"
)

MANAGED_EXIT_SERVICE_NOOP = "TRACK_B_MANAGED_EXIT_SERVICE_NOOP"
MANAGED_EXIT_SERVICE_DRY_RUN_READY = "TRACK_B_MANAGED_EXIT_SERVICE_DRY_RUN_READY"
MANAGED_EXIT_SERVICE_BLOCKED = "TRACK_B_MANAGED_EXIT_SERVICE_BLOCKED"
MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING = "TRACK_B_MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING"
MANAGED_EXIT_SERVICE_PARTIAL = "TRACK_B_MANAGED_EXIT_SERVICE_PARTIAL"
MANAGED_EXIT_SERVICE_REFRESH_FAILED = "TRACK_B_MANAGED_EXIT_SERVICE_REFRESH_FAILED"
MANAGED_EXIT_SERVICE_RUNNING = "TRACK_B_MANAGED_EXIT_SERVICE_RUNNING"
MANAGED_EXIT_SERVICE_STOPPING = "TRACK_B_MANAGED_EXIT_SERVICE_STOPPING"
MANAGED_EXIT_SERVICE_CYCLE_STARTED = "TRACK_B_MANAGED_EXIT_SERVICE_CYCLE_STARTED"
MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS = "NO_ELIGIBLE_EXITS"
MANAGED_EXIT_SERVICE_APPLY_ATTEMPTED = "APPLY_ATTEMPTED"
MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED = "APPLY_SUCCEEDED"
MANAGED_EXIT_SERVICE_APPLY_BLOCKED = "APPLY_BLOCKED"
MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT = "ACTUATOR_TIMEOUT"
MANAGED_EXIT_SERVICE_ERROR = "SERVICE_ERROR"
MANAGED_EXIT_SERVICE_REFRESH_DEGRADED_ACTUATOR_ATTEMPTED = "REFRESH_DEGRADED_ACTUATOR_ATTEMPTED"
MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE = "MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE"
MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED = "MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED"
MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED = "MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED"

_PAPER_ACCOUNT_ID = "DUM882026"
_PAPER_EXECUTION_DOMAIN = "TRACK_B_PAPER"


@dataclass(frozen=True)
class TrackBManagedExitServiceConfig:
    repo_root: Path = REPO_ROOT
    status_path: Path = DEFAULT_STATUS_PATH
    heartbeat_path: Path | None = DEFAULT_HEARTBEAT_PATH
    cadence_seconds: float = DEFAULT_CADENCE_SECONDS
    apply: bool = False
    operator_authorized_managed_exit: bool = False
    max_cycles_per_tick: int = 4
    authority_refresh_before_apply: bool = True
    authority_refresh_after_attempt: bool = True
    authority_refresh_timeout_seconds: float = 120.0
    actuator_timeout_seconds: float = 90.0
    service_label: str | None = None

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


ActuatorRunner = Callable[[TrackBManagedExitActuatorConfig, datetime, float], Mapping[str, Any]]
AuthorityRefresher = Callable[[TrackBManagedExitServiceConfig, str], Mapping[str, Any]]
CommandRunner = Callable[[Sequence[str], Path, float], subprocess.CompletedProcess[str]]
PipelineBuilder = Callable[[TrackBManagedExitServiceConfig, datetime], Mapping[str, Any]]
OrderMaintenanceRunner = Callable[[TrackBManagedExitServiceConfig, datetime, float], Mapping[str, Any]]
PostMutationRefresher = Callable[..., Mapping[str, Any]]
SleepFunc = Callable[[float], None]


def run_track_b_managed_exit_service_once(
    *,
    config: TrackBManagedExitServiceConfig,
    now: datetime | None = None,
    actuator_runner: ActuatorRunner | None = None,
    authority_refresher: AuthorityRefresher | None = None,
    command_runner: CommandRunner | None = None,
    pipeline_builder: PipelineBuilder | None = None,
    order_maintenance_runner: OrderMaintenanceRunner | None = None,
    post_mutation_refresher: PostMutationRefresher | None = None,
    write: bool = True,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actuator_runner = actuator_runner or (
        lambda actuator_config, actuator_now, timeout: _run_actuator_child(
            actuator_config,
            actuator_now,
            timeout_seconds=timeout,
            command_runner=command_runner,
        )
    )
    authority_refresher = authority_refresher or _refresh_operator_authority
    pipeline_builder = pipeline_builder or _run_pipeline_dry_run
    order_maintenance_runner = order_maintenance_runner or _run_managed_order_maintenance
    post_mutation_refresher = post_mutation_refresher or post_position_order_change_refresh
    authority_refreshes: list[dict[str, Any]] = []
    actuator_reports: list[dict[str, Any]] = []
    order_maintenance_reports: list[dict[str, Any]] = []
    service_diagnostics: list[dict[str, Any]] = []
    phase_timings: list[dict[str, Any]] = [
        {
            "phase": "pipeline_candidate_discovery",
            "duration_seconds": 0.0,
            "classification": "STARTS_BEFORE_AUTHORITY_REFRESH",
        }
    ]

    if write:
        write_track_b_managed_exit_service_status(
            config=config,
            payload=_cycle_started_payload(config=config, now=actual_now),
        )

    sweeper_report = _run_broker_truth_sweeper(config=config, now=actual_now, write=write)
    if _publish_broker_truth_sweeper_diagnostic(sweeper_report):
        service_diagnostics.append({"kind": "broker_truth_sweeper", **sweeper_report})

    phase_started = time.monotonic()
    execution_plan = _build_pipeline_execution_plan(config=config, now=actual_now, pipeline_builder=pipeline_builder)
    phase_timings.append(
        {
            **_phase_timing("pipeline_execution_plan", phase_started),
            "classification": execution_plan.get("classification"),
            "executable_intent_count": len(execution_plan.get("executable_intents") or []),
            "blocked_intent_count": len(execution_plan.get("blocked_intents") or []),
        }
    )

    if execution_plan.get("classification") == MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE:
        payload = _service_payload(
            config=config,
            now=actual_now,
            classification=MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE,
            authority_refreshes=authority_refreshes,
            actuator_reports=actuator_reports,
            phase_timings=phase_timings,
            execution_plan=execution_plan,
        )
        if write:
            write_track_b_managed_exit_service_status(config=config, payload=payload)
        return payload

    executable_intents = list(execution_plan.get("executable_intents") or [])
    if not executable_intents:
        blocked_intents = list(execution_plan.get("blocked_intents") or [])
        if not blocked_intents and _working_close_order_maintenance_needed(config):
            if config.apply and config.authority_refresh_before_apply:
                maintenance_authority = _classify_working_close_order_maintenance_authority(config)
                if not _managed_paper_exit_authority_allowed(maintenance_authority):
                    payload = _service_payload(
                        config=config,
                        now=actual_now,
                        classification=MANAGED_EXIT_SERVICE_APPLY_BLOCKED,
                        authority_refreshes=authority_refreshes,
                        actuator_reports=actuator_reports,
                        phase_timings=phase_timings,
                        execution_plan=execution_plan,
                        order_maintenance_reports=order_maintenance_reports,
                        service_diagnostics=[
                            *service_diagnostics,
                            maintenance_authority,
                        ],
                    )
                    if write:
                        write_track_b_managed_exit_service_status(config=config, payload=payload)
                    return payload
                service_diagnostics.append(_legacy_refresh_diagnostic_skipped("before_order_maintenance", maintenance_authority))
            phase_started = time.monotonic()
            maintenance_report = dict(
                order_maintenance_runner(config, actual_now, config.actuator_timeout_seconds)
            )
            phase_timings.append(
                {
                    **_phase_timing("working_close_order_maintenance", phase_started),
                    "classification": maintenance_report.get("classification"),
                    "managed_order_count": maintenance_report.get("managed_order_count"),
                    "mutation_attempted": maintenance_report.get("broker_mutation_attempted") is True,
                    "mutation_performed": maintenance_report.get("broker_mutation_performed") is True,
                }
            )
            if maintenance_report.get("classification") != "MANAGED_ORDER_MAINTENANCE_NO_ACTION":
                order_maintenance_reports.append(maintenance_report)
                payload = _service_payload(
                    config=config,
                    now=actual_now,
                    classification=_classification_from_order_maintenance(maintenance_report),
                    authority_refreshes=authority_refreshes,
                    actuator_reports=actuator_reports,
                    phase_timings=phase_timings,
                    execution_plan=execution_plan,
                    order_maintenance_reports=order_maintenance_reports,
                    service_diagnostics=service_diagnostics,
                )
                payload = _attach_post_broker_mutation_refresh(
                    config=config,
                    payload=payload,
                    trigger="managed_exit_service_order_maintenance",
                    post_mutation_refresher=post_mutation_refresher,
                    write=write,
                )
                if write:
                    write_track_b_managed_exit_service_status(config=config, payload=payload)
                return payload
        payload = _service_payload(
            config=config,
            now=actual_now,
            classification=MANAGED_EXIT_SERVICE_BLOCKED if blocked_intents else MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS,
            authority_refreshes=authority_refreshes,
            actuator_reports=actuator_reports,
            phase_timings=phase_timings,
            execution_plan=execution_plan,
            order_maintenance_reports=order_maintenance_reports,
            service_diagnostics=service_diagnostics,
        )
        if write:
            write_track_b_managed_exit_service_status(config=config, payload=payload)
        return payload

    max_cycles = min(max(int(config.max_cycles_per_tick or 0), 1), len(executable_intents))
    for cycle_index in range(max_cycles):
        executable_intent = _mapping(executable_intents[cycle_index])
        mutation_authority = _classify_managed_paper_risk_reducing_exit_authority(
            executable_intent=executable_intent,
            execution_plan=execution_plan,
        )
        if not _managed_paper_exit_authority_allowed(mutation_authority):
            payload = _service_payload(
                config=config,
                now=actual_now,
                classification=MANAGED_EXIT_SERVICE_APPLY_BLOCKED,
                authority_refreshes=authority_refreshes,
                actuator_reports=actuator_reports,
                phase_timings=phase_timings,
                execution_plan=execution_plan,
                order_maintenance_reports=order_maintenance_reports,
                service_diagnostics=[
                    *service_diagnostics,
                    mutation_authority,
                ],
            )
            if write:
                write_track_b_managed_exit_service_status(config=config, payload=payload)
            return payload
        if config.authority_refresh_before_apply:
            service_diagnostics.append(_legacy_refresh_diagnostic_skipped("before_actuator", mutation_authority))
        phase_started = time.monotonic()
        actuator_config = TrackBManagedExitActuatorConfig(
            repo_root=config.repo_root,
            apply=config.apply is True,
            operator_authorized_managed_exit=config.operator_authorized_managed_exit is True or config.apply is True,
            max_closes_per_run=1,
        )
        try:
            actuator_report = dict(actuator_runner(actuator_config, actual_now, config.actuator_timeout_seconds))
        except Exception as exc:  # defensive: publish a terminal service error instead of vanishing mid-cycle.
            actuator_report = {
                "classification": MANAGED_EXIT_SERVICE_ERROR,
                "error": str(exc),
                "submit_attempted": False,
                "submitted_count": 0,
                "broker_state_mutated": False,
            }
        phase_timings.append(_phase_timing("actuator_apply" if config.apply else "actuator_dry_run", phase_started))
        actuator_report["service_cycle_index"] = cycle_index
        actuator_reports.append(actuator_report)

        if actuator_report.get("classification") == MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT:
            break

        submitted = int(actuator_report.get("submitted_count") or 0)
        should_refresh_after = config.authority_refresh_after_attempt and (
            submitted > 0
            or actuator_report.get("submit_attempted") is True
            or _actuator_broker_effect_observed(actuator_report)
        )
        if should_refresh_after:
            phase_started = time.monotonic()
            authority_refreshes.append(_run_authority_refresh(authority_refresher, config, "after_actuator_attempt"))
            phase_timings.append(_phase_timing("post_refresh", phase_started))
            phase_started = time.monotonic()
            execution_plan = _build_pipeline_execution_plan(
                config=config,
                now=actual_now,
                pipeline_builder=pipeline_builder,
            )
            phase_timings.append(
                {
                    **_phase_timing("post_refresh_pipeline_execution_plan", phase_started),
                    "classification": execution_plan.get("classification"),
                    "executable_intent_count": len(execution_plan.get("executable_intents") or []),
                    "blocked_intent_count": len(execution_plan.get("blocked_intents") or []),
                }
            )

        if _should_stop_after_actuator(actuator_report):
            break

    classification = _service_classification(actuator_reports)
    if any(row.get("succeeded") is not True for row in authority_refreshes) and actuator_reports:
        classification = MANAGED_EXIT_SERVICE_REFRESH_DEGRADED_ACTUATOR_ATTEMPTED
    payload = _service_payload(
        config=config,
        now=actual_now,
        classification=classification,
        authority_refreshes=authority_refreshes,
        actuator_reports=actuator_reports,
        phase_timings=phase_timings,
        execution_plan=execution_plan,
        order_maintenance_reports=order_maintenance_reports,
        service_diagnostics=service_diagnostics,
    )
    payload = _attach_post_broker_mutation_refresh(
        config=config,
        payload=payload,
        trigger="managed_exit_service_actuator",
        post_mutation_refresher=post_mutation_refresher,
        write=write,
    )
    if write:
        write_track_b_managed_exit_service_status(config=config, payload=payload)
    return payload


def run_track_b_managed_exit_service(
    *,
    config: TrackBManagedExitServiceConfig,
    actuator_runner: ActuatorRunner | None = None,
    authority_refresher: AuthorityRefresher | None = None,
    pipeline_builder: PipelineBuilder | None = None,
    sleep_func: SleepFunc = time.sleep,
    max_iterations: int | None = None,
) -> int:
    stopping = False

    def _handle_stop(signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True
        payload = {
            "schema_version": "track_b_managed_exit_service_status_v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "classification": MANAGED_EXIT_SERVICE_STOPPING,
            "reason": f"signal={signum}",
            "repo_root": str(config.repo_root),
            "cadence_seconds": config.cadence_seconds,
            "close_only": True,
            "entry_allowed": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
        write_track_b_managed_exit_service_status(config=config, payload=payload)
        _write_heartbeat(config=config, payload=payload, service_running=False)

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    iteration = 0
    while not stopping:
        _write_heartbeat(
            config=config,
            payload={
                "classification": MANAGED_EXIT_SERVICE_RUNNING,
                "repo_root": str(config.repo_root),
                "cadence_seconds": config.cadence_seconds,
                "close_only": True,
                "entry_allowed": False,
            },
            service_running=True,
        )
        payload = run_track_b_managed_exit_service_once(
            config=config,
            actuator_runner=actuator_runner,
            authority_refresher=authority_refresher,
            pipeline_builder=pipeline_builder,
            write=True,
        )
        _write_heartbeat(config=config, payload=payload, service_running=True)
        iteration += 1
        if max_iterations is not None and iteration >= max_iterations:
            break
        sleep_func(max(float(config.cadence_seconds), 1.0))
    return 0


def read_track_b_managed_exit_service_status(
    *, repo_root: Path = REPO_ROOT, status_path: Path = DEFAULT_STATUS_PATH
) -> dict[str, Any]:
    resolved = status_path if status_path.is_absolute() else repo_root / status_path
    try:
        payload = json.loads(resolved.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {
            "schema_version": "track_b_managed_exit_service_status_v1",
            "generated_at": datetime.now(UTC).isoformat(),
            "classification": "TRACK_B_MANAGED_EXIT_SERVICE_STATUS_MISSING",
            "status_path": str(resolved),
            "fresh": False,
            "close_only": True,
            "entry_allowed": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        }
    return dict(payload) if isinstance(payload, Mapping) else {}


def write_track_b_managed_exit_service_status(
    *, config: TrackBManagedExitServiceConfig, payload: Mapping[str, Any]
) -> Path:
    record = to_jsonable(dict(payload))
    path = write_json_atomic(config.resolve(config.status_path), record)
    try_record_strategy_funnel_events(
        events_from_managed_exit_service_status(record),
        repo_root=config.repo_root,
    )
    return path


def _service_payload(
    *,
    config: TrackBManagedExitServiceConfig,
    now: datetime,
    classification: str,
    authority_refreshes: Sequence[Mapping[str, Any]],
    actuator_reports: Sequence[Mapping[str, Any]],
    phase_timings: Sequence[Mapping[str, Any]],
    execution_plan: Mapping[str, Any] | None = None,
    order_maintenance_reports: Sequence[Mapping[str, Any]] = (),
    service_diagnostics: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    attempted = [row for report in actuator_reports for row in report.get("attempted_closes") or []]
    final_classification = _final_cycle_classification(classification=classification, actuator_reports=actuator_reports)
    plan = dict(execution_plan or {})
    return {
        "schema_version": "track_b_managed_exit_service_status_v1",
        "generated_at": now.isoformat(),
        "classification": final_classification,
        "legacy_service_classification": classification,
        "repo_root": str(config.repo_root),
        "pid": os.getpid(),
        "service_label": _service_label(config),
        "cadence_seconds": config.cadence_seconds,
        "close_only": True,
        "entry_allowed": False,
        "apply_requested": config.apply is True,
        "apply_mode": "GUARDED_CLOSE_ONLY_APPLY" if config.apply is True else "DRY_RUN_ONLY",
        "operator_authorized_managed_exit": config.operator_authorized_managed_exit is True,
        "max_closes_per_run": 1,
        "max_cycles_per_tick": config.max_cycles_per_tick,
        "actuator_timeout_seconds": config.actuator_timeout_seconds,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "authority_inputs": [
            "managed_positions",
            "managed_orders",
            "reconciliation",
            "open_order_truth",
            "broker_session_authority",
            "safe_state",
            "guardian",
        ],
        "authority_refresh_before_apply": config.authority_refresh_before_apply,
        "authority_refresh_after_attempt": config.authority_refresh_after_attempt,
        "authority_refreshes": list(authority_refreshes),
        "authority_refresh_failed": any(row.get("succeeded") is not True for row in authority_refreshes),
        "authority_refresh_degraded_actuator_attempted": any(
            row.get("succeeded") is not True for row in authority_refreshes
        )
        and bool(actuator_reports),
        "execution_plan": plan,
        "pipeline_classification": plan.get("classification"),
        "pipeline_diagnostics": list(plan.get("diagnostics") or []),
        "service_diagnostics": list(service_diagnostics),
        "exit_intent_count": len(plan.get("exit_intents") or []),
        "authority_decision_count": len(plan.get("authority_decisions") or []),
        "executable_intent_count": len(plan.get("executable_intents") or []),
        "blocked_intent_count": len(plan.get("blocked_intents") or []),
        "executable_exit_intent_ids": [
            row.get("exit_intent_id") for row in plan.get("executable_intents") or [] if isinstance(row, Mapping)
        ],
        "blocked_exit_intent_ids": [
            row.get("exit_intent_id") for row in plan.get("blocked_intents") or [] if isinstance(row, Mapping)
        ],
        "phase_timings": list(phase_timings),
        "actuator_invocation_count": len(actuator_reports),
        "actuator_reports": list(actuator_reports),
        "managed_order_maintenance_invocation_count": len(order_maintenance_reports),
        "managed_order_maintenance_reports": list(order_maintenance_reports),
        "latest_managed_order_maintenance_classification": order_maintenance_reports[-1].get("classification")
        if order_maintenance_reports
        else None,
        "latest_actuator_classification": actuator_reports[-1].get("classification") if actuator_reports else None,
        "exit_due_count": _max_int(actuator_reports, "exit_due_count"),
        "eligible_count": _max_int(actuator_reports, "eligible_count"),
        "attempted_count": len(attempted),
        "submitted_count": sum(int(report.get("submitted_count") or 0) for report in actuator_reports),
        "submit_attempted": any(report.get("submit_attempted") is True for report in actuator_reports),
        "broker_state_mutated": any(report.get("broker_state_mutated") is True for report in actuator_reports)
        or any(report.get("broker_mutation_performed") is True for report in order_maintenance_reports),
        "managed_order_maintenance_mutation_attempted": any(
            report.get("broker_mutation_attempted") is True for report in order_maintenance_reports
        ),
        "managed_order_maintenance_mutation_performed": any(
            report.get("broker_mutation_performed") is True for report in order_maintenance_reports
        ),
        "attempted_closes": attempted,
        "required_next_action": _next_action(final_classification),
        "status_path": str(config.resolve(config.status_path)),
    }


def _attach_post_broker_mutation_refresh(
    *,
    config: TrackBManagedExitServiceConfig,
    payload: Mapping[str, Any],
    trigger: str,
    post_mutation_refresher: PostMutationRefresher,
    write: bool,
) -> dict[str, Any]:
    enriched = dict(payload)
    if write is not True or enriched.get("broker_state_mutated") is not True:
        return enriched
    refresh_config = PostBrokerMutationRefreshConfig(repo_root=config.repo_root)
    try:
        refresh = dict(
            post_mutation_refresher(
                config=refresh_config,
                trigger=trigger,
                mutation_report=enriched,
            )
        )
    except Exception as exc:  # defensive: convergence publication must not unwind broker mutation.
        refresh = {
            "classification": "POST_BROKER_MUTATION_REFRESH_EXCEPTION",
            "trigger": trigger,
            "error": str(exc),
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "global_cancel_allowed": False,
            "broad_flatten_allowed": False,
        }
    enriched["post_broker_mutation_refresh"] = refresh
    return enriched


def _cycle_started_payload(*, config: TrackBManagedExitServiceConfig, now: datetime) -> dict[str, Any]:
    return {
        "schema_version": "track_b_managed_exit_service_status_v1",
        "generated_at": now.isoformat(),
        "classification": MANAGED_EXIT_SERVICE_CYCLE_STARTED,
        "cycle_started": True,
        "pid": os.getpid(),
        "service_label": _service_label(config),
        "repo_root": str(config.repo_root),
        "cadence_seconds": config.cadence_seconds,
        "close_only": True,
        "entry_allowed": False,
        "apply_requested": config.apply is True,
        "apply_mode": "GUARDED_CLOSE_ONLY_APPLY" if config.apply is True else "DRY_RUN_ONLY",
        "operator_authorized_managed_exit": config.operator_authorized_managed_exit is True,
        "max_closes_per_run": 1,
        "max_cycles_per_tick": config.max_cycles_per_tick,
        "actuator_timeout_seconds": config.actuator_timeout_seconds,
        "detected_candidates_count": None,
        "candidate_detection": "deferred_to_v1_pipeline_execution_plan",
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "phase_timings": [],
        "status_path": str(config.resolve(config.status_path)),
    }


def _service_classification(actuator_reports: Sequence[Mapping[str, Any]]) -> str:
    if not actuator_reports:
        return MANAGED_EXIT_SERVICE_NOOP
    classifications = [str(report.get("classification") or "") for report in actuator_reports]
    submitted = sum(int(report.get("submitted_count") or 0) for report in actuator_reports)
    if submitted > 0 and any("BLOCKED" in item or item == MANAGED_EXIT_ACTUATOR_PARTIAL for item in classifications):
        return MANAGED_EXIT_SERVICE_PARTIAL
    if submitted > 0:
        return MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING
    if classifications[-1] == MANAGED_EXIT_ACTUATOR_DRY_RUN_READY:
        return MANAGED_EXIT_SERVICE_DRY_RUN_READY
    if any("BLOCKED" in item for item in classifications):
        return MANAGED_EXIT_SERVICE_BLOCKED
    return MANAGED_EXIT_SERVICE_NOOP


def _classification_from_order_maintenance(report: Mapping[str, Any]) -> str:
    classification = str(report.get("classification") or "")
    if report.get("broker_mutation_performed") is True:
        return MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    if report.get("broker_mutation_attempted") is True:
        return MANAGED_EXIT_SERVICE_APPLY_ATTEMPTED
    if classification == "MANAGED_ORDER_MAINTENANCE_DRY_RUN_READY":
        return MANAGED_EXIT_SERVICE_DRY_RUN_READY
    if "BLOCKED" in classification or "REVIEW" in classification:
        return MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    return MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS


def _final_cycle_classification(*, classification: str, actuator_reports: Sequence[Mapping[str, Any]]) -> str:
    if classification == MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE:
        return MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE
    if classification == MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS:
        return MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS
    if classification == MANAGED_EXIT_SERVICE_REFRESH_DEGRADED_ACTUATOR_ATTEMPTED:
        return MANAGED_EXIT_SERVICE_REFRESH_DEGRADED_ACTUATOR_ATTEMPTED
    if classification in {MANAGED_EXIT_SERVICE_REFRESH_FAILED, MANAGED_EXIT_SERVICE_ERROR}:
        return MANAGED_EXIT_SERVICE_ERROR if classification == MANAGED_EXIT_SERVICE_ERROR else classification
    if any(str(report.get("classification") or "") == MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT for report in actuator_reports):
        return MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT
    submitted = sum(int(report.get("submitted_count") or 0) for report in actuator_reports)
    attempted = any(report.get("submit_attempted") is True for report in actuator_reports)
    if submitted > 0:
        return MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED
    if attempted:
        return MANAGED_EXIT_SERVICE_APPLY_ATTEMPTED
    if any("BLOCKED" in str(report.get("classification") or "") for report in actuator_reports):
        return MANAGED_EXIT_SERVICE_APPLY_BLOCKED
    if actuator_reports and all(int(report.get("eligible_count") or 0) == 0 for report in actuator_reports):
        return MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS
    if classification == MANAGED_EXIT_SERVICE_DRY_RUN_READY:
        return MANAGED_EXIT_SERVICE_DRY_RUN_READY
    return classification


def _should_stop_after_actuator(report: Mapping[str, Any]) -> bool:
    classification = str(report.get("classification") or "")
    submitted = int(report.get("submitted_count") or 0)
    if submitted > 0 or report.get("submit_attempted") is True or _actuator_broker_effect_observed(report):
        return True
    if classification in {MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING, MANAGED_EXIT_ACTUATOR_PARTIAL}:
        return True
    return True


def _actuator_broker_effect_observed(report: Mapping[str, Any]) -> bool:
    if int(report.get("broker_effect_observed_count") or 0) > 0:
        return True
    for row in report.get("attempted_closes") or []:
        if not isinstance(row, Mapping):
            continue
        if row.get("broker_effect_observed") is True:
            return True
        close_submit = row.get("close_submit_attempt") if isinstance(row.get("close_submit_attempt"), Mapping) else {}
        if close_submit.get("classification") == BROKER_EFFECT_OBSERVED:
            return True
    return False


def _next_action(classification: str) -> str:
    if classification == MANAGED_EXIT_SERVICE_DRY_RUN_READY:
        return "ENABLE_APPLY_MODE_ONLY_IF_OPERATOR_INTENDS_AUTONOMOUS_CLOSE_SERVICE"
    if classification in {MANAGED_EXIT_SERVICE_APPLIED_OR_PENDING, MANAGED_EXIT_SERVICE_APPLY_SUCCEEDED}:
        return "VERIFY_BROKER_AND_MANAGED_TRUTH_AFTER_GUARDED_CLOSE"
    if classification in {MANAGED_EXIT_SERVICE_PARTIAL, MANAGED_EXIT_SERVICE_APPLY_ATTEMPTED}:
        return "REFRESH_AUTHORITY_AND_REVIEW_BLOCKED_CLOSE"
    if classification == MANAGED_EXIT_SERVICE_REFRESH_DEGRADED_ACTUATOR_ATTEMPTED:
        return "VERIFY_GUARDED_CLOSE_AND_REFRESH_AUTHORITY"
    if classification == MANAGED_EXIT_SERVICE_REFRESH_FAILED:
        return "REFRESH_AUTHORITY"
    if classification in {MANAGED_EXIT_SERVICE_BLOCKED, MANAGED_EXIT_SERVICE_APPLY_BLOCKED}:
        return "OPERATOR_REVIEW_REQUIRED"
    if classification == MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT:
        return "STOP_SERVICE_AND_INSPECT_ACTUATOR_TIMEOUT"
    if classification == MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE:
        return "REPAIR_MANAGED_EXIT_PIPELINE"
    if classification == MANAGED_EXIT_SERVICE_ERROR:
        return "OPERATOR_REVIEW_REQUIRED"
    return "NO_ACTION"


def _run_managed_order_maintenance(
    config: TrackBManagedExitServiceConfig,
    now: datetime,
    timeout_seconds: float,
) -> Mapping[str, Any]:
    planner_config = TrackBOrderAdjustmentPlannerConfig(repo_root=config.repo_root)
    order_adjustment_plan = build_track_b_order_adjustment_plan(config=planner_config, now=now)
    write_track_b_order_adjustment_plan(config=planner_config, payload=order_adjustment_plan)
    managed_orders = _read_json(config.resolve(DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT))
    plans_by_order = {
        str(plan.get("broker_order_id") or ""): plan
        for plan in order_adjustment_plan.get("plans") or []
        if isinstance(plan, Mapping)
    }
    results: list[dict[str, Any]] = []
    for order in managed_orders.get("managed_orders") or []:
        if not isinstance(order, Mapping):
            continue
        if str(order.get("classification") or "") not in {
            "WORKING_CLOSE_ORDER",
            "CLOSE_ORDER_MODIFIABLE",
            "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
        }:
            continue
        if order.get("is_close_order") is not True:
            continue
        broker_order_id = str(order.get("broker_order_id") or "")
        plan = plans_by_order.get(broker_order_id)
        if not plan:
            results.append(
                {
                    "classification": "MANAGED_ORDER_MAINTENANCE_BLOCKED_NO_PLAN",
                    "broker_order_id": broker_order_id,
                    "lifecycle_id": order.get("lifecycle_id"),
                    "broker_mutation_attempted": False,
                    "broker_mutation_performed": False,
                    "detail": "No current order-adjustment plan matched the known managed close order.",
                }
            )
            continue
        if str(plan.get("classification") or "") != MODIFY_IN_PLACE_ELIGIBLE:
            results.append(
                {
                    "classification": "MANAGED_ORDER_MAINTENANCE_WAIT",
                    "broker_order_id": broker_order_id,
                    "perm_id": order.get("perm_id"),
                    "lifecycle_id": order.get("lifecycle_id"),
                    "order_adjustment_classification": plan.get("classification"),
                    "recommended_operator_action": plan.get("recommended_operator_action"),
                    "broker_mutation_attempted": False,
                    "broker_mutation_performed": False,
                    "detail": plan.get("rationale") or "Managed close order does not require modify-in-place.",
                }
            )
            continue
        modify_config = _modify_config_from_order_plan(
            service_config=config,
            order=order,
            plan=plan,
            timeout_seconds=timeout_seconds,
        )
        if modify_config is None:
            results.append(
                {
                    "classification": "MANAGED_ORDER_MAINTENANCE_BLOCKED_INCOMPLETE_IDENTITY",
                    "broker_order_id": broker_order_id,
                    "perm_id": order.get("perm_id"),
                    "lifecycle_id": order.get("lifecycle_id"),
                    "order_adjustment_classification": plan.get("classification"),
                    "broker_mutation_attempted": False,
                    "broker_mutation_performed": False,
                    "detail": "Modify-in-place was eligible, but exact identity or target price was incomplete.",
                }
            )
            continue
        adapter: IbkrPaperManagedOrderModifyAdapter | None = None
        hooks: dict[str, Any] = {}
        if config.apply:
            adapter = IbkrPaperManagedOrderModifyAdapter(config=modify_config)
            hooks = {
                "pre_modify_open_order_refresh": adapter.refresh_open_orders,
                "modify_order_limit": adapter.modify_order_limit,
                "post_modify_open_order_refresh": adapter.refresh_open_orders,
            }
        try:
            report = run_track_b_managed_order_modify_in_place(config=modify_config, now=now, **hooks)
        finally:
            if adapter is not None:
                adapter.disconnect()
        results.append(
            {
                "classification": report.get("classification"),
                "broker_order_id": broker_order_id,
                "perm_id": order.get("perm_id"),
                "lifecycle_id": order.get("lifecycle_id"),
                "order_adjustment_classification": plan.get("classification"),
                "current_known_limit": modify_config.current_known_limit,
                "new_limit": modify_config.new_limit,
                "apply": modify_config.apply,
                "broker_mutation_attempted": report.get("broker_mutation_attempted") is True,
                "broker_mutation_performed": report.get("broker_mutation_performed") is True,
                "new_order_created": report.get("new_order_created") is True,
                "artifact_path": report.get("artifact_path"),
                "detail": report.get("detail"),
                "report": report,
            }
        )
        break
    return _managed_order_maintenance_report(
        now=now,
        managed_orders=managed_orders,
        order_adjustment_plan=order_adjustment_plan,
        results=results,
    )


def _working_close_order_maintenance_needed(config: TrackBManagedExitServiceConfig) -> bool:
    managed_orders = _read_json(config.resolve(DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT))
    for order in managed_orders.get("managed_orders") or []:
        if not isinstance(order, Mapping):
            continue
        if order.get("is_close_order") is not True:
            continue
        if str(order.get("classification") or "") in {
            "WORKING_CLOSE_ORDER",
            "CLOSE_ORDER_MODIFIABLE",
            "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
        }:
            return True
    return False


def _authority_refresh_succeeded(refresh: Mapping[str, Any]) -> bool:
    return refresh.get("succeeded") is True


def _managed_paper_exit_authority_allowed(authority: Mapping[str, Any]) -> bool:
    return str(authority.get("classification") or "") == MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED


def _legacy_refresh_diagnostic_skipped(phase: str, authority: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "code": "LEGACY_OPERATOR_READINESS_REFRESH_DIAGNOSTIC_ONLY",
        "phase": phase,
        "classification": "LEGACY_OPERATOR_READINESS_REFRESH_SKIPPED",
        "detail": (
            "Legacy operator readiness refresh is diagnostic-only because managed PAPER "
            "risk-reducing exit authority is satisfied."
        ),
        "managed_paper_risk_reducing_exit_authority": dict(authority),
    }


def _classify_managed_paper_risk_reducing_exit_authority(
    *,
    executable_intent: Mapping[str, Any],
    execution_plan: Mapping[str, Any],
) -> dict[str, Any]:
    blockers: list[str] = []
    diagnostics: list[dict[str, Any]] = []
    authority = _mapping(executable_intent.get("authority_decision"))
    decision = _mapping(authority.get("authority_decision"))
    if not decision:
        decision = authority

    _require(blockers, str(executable_intent.get("account_id") or executable_intent.get("account") or "") == _PAPER_ACCOUNT_ID, "wrong_account")
    _require(blockers, str(executable_intent.get("execution_domain") or "") == _PAPER_EXECUTION_DOMAIN, "non_paper_execution_domain")
    _require(blockers, executable_intent.get("live_money_eligible") is not True, "live_money_eligible")
    _require(blockers, executable_intent.get("live_money_allowed") is not True, "live_money_allowed")
    _require(blockers, executable_intent.get("paper_proof_invoked") is not True, "paper_proof_invoked")
    _require(blockers, executable_intent.get("broad_flatten_allowed") is not True, "broad_flatten_requested")
    _require(blockers, executable_intent.get("global_flatten_allowed") is not True, "global_flatten_requested")
    _require(blockers, str(executable_intent.get("decision") or decision.get("decision") or "") == "ALLOWED", "exit_authority_decision_not_allowed")
    _require(blockers, _decimal(executable_intent.get("close_qty")) is not None and _decimal(executable_intent.get("close_qty")) > 0, "close_qty_missing_or_zero")
    _require(blockers, str(executable_intent.get("close_action") or "").upper() in {"BUY", "SELL"}, "close_action_invalid")
    _require(blockers, bool(_managed_lifecycle_identity(executable_intent)), "managed_lifecycle_identity_missing")
    _require(blockers, bool(executable_intent.get("source_policy_id") or executable_intent.get("exit_reason")), "exit_policy_evidence_missing")

    _require_check_passed(blockers, decision, "known_current_broker_position", "broker_position_unavailable")
    _require_check_passed(blockers, decision, "account_matches", "account_mismatch")
    _require_check_passed(blockers, decision, "execution_domain_matches", "execution_domain_mismatch")
    _require_check_passed(blockers, decision, "contract_matches", "contract_mismatch")
    _require_check_passed(blockers, decision, "close_qty_within_broker_position", "over_close_risk")
    _require_check_passed(blockers, decision, "risk_reducing_action", "not_risk_reducing")
    _require_check_passed(blockers, decision, "same_contract_working_close_does_not_over_close", "same_contract_working_close_over_close_risk")
    _require_check_passed(blockers, decision, "safe_state_no_hard_halt", "safe_state_hard_halt")
    _require_check_passed(blockers, decision, "live_money_domain_allowed", "live_money_not_allowed")
    _require_check_passed(blockers, decision, "paper_proof_not_invoked", "paper_proof_invoked")
    _require_check_passed(blockers, decision, "broad_or_global_flatten_not_requested", "broad_or_global_flatten_requested")
    _require_conditional_check_passed(blockers, decision, "same_contract_unknown_order_risk", "same_contract_unknown_order_over_close_risk")

    source_classifications = _source_classifications(execution_plan)
    open_order_truth = str(source_classifications.get("open_order_truth") or source_classifications.get("Open Order Truth") or "")
    if open_order_truth and open_order_truth not in {"NO_OPEN_ORDERS", "BROKER_POSITION_WITHOUT_CLOSE_ORDER"}:
        if _exit_authority_checks_clear_order_risk(decision):
            diagnostics.append(
                {
                    "kind": "diagnostic_open_order_truth_classification",
                    "classification": open_order_truth,
                    "detail": (
                        "Open-order truth publication is diagnostic because ExitAuthority V1.1 "
                        "proved same-contract working-close and unknown-order risk are clear."
                    ),
                }
            )
        else:
            blockers.append(f"open_order_truth_not_clean:{open_order_truth}")
    managed_positions = str(source_classifications.get("managed_positions") or source_classifications.get("Managed Position Registry") or "")
    if managed_positions and managed_positions not in {"OPEN_MANAGED_EXIT_DUE", "OPEN_MANAGED_MATCHED", "OPEN_MANAGED_CLOSE_WORKING"}:
        if _exit_authority_checks_prove_current_managed_exposure(decision, executable_intent):
            diagnostics.append(
                {
                    "kind": "diagnostic_managed_position_classification",
                    "classification": managed_positions,
                    "detail": (
                        "Managed-position publication is diagnostic because ExitAuthority V1.1 "
                        "proved an attributed current broker-backed managed exposure."
                    ),
                }
            )
        else:
            blockers.append(f"managed_position_not_current:{managed_positions}")
    managed_orders = str(source_classifications.get("managed_orders") or source_classifications.get("Managed Order Registry") or "")
    if managed_orders and managed_orders not in {
        "POSITION_WITHOUT_CLOSE_ORDER",
        "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
        "WORKING_CLOSE_ORDER",
        "CLOSE_ORDER_MODIFIABLE",
        "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
    }:
        if _exit_authority_checks_clear_order_risk(decision) and _exit_authority_checks_prove_current_managed_exposure(
            decision,
            executable_intent,
        ):
            diagnostics.append(
                {
                    "kind": "diagnostic_managed_order_classification",
                    "classification": managed_orders,
                    "detail": (
                        "Managed-order publication is diagnostic because ExitAuthority V1.1 proved same-contract "
                        "working-close risk is clear for this broker-backed managed exposure."
                    ),
                }
            )
        else:
            blockers.append(f"managed_order_not_current:{managed_orders}")
    if source_classifications:
        diagnostics.append({"kind": "source_classifications", "rows": source_classifications})

    return {
        "classification": MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED
        if not blockers
        else MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED,
        "code": "managed_paper_risk_reducing_exit_authority",
        "exit_intent_id": executable_intent.get("exit_intent_id"),
        "blockers": blockers,
        "diagnostics": diagnostics,
        "account_id": executable_intent.get("account_id") or executable_intent.get("account"),
        "execution_domain": executable_intent.get("execution_domain"),
        "close_action": executable_intent.get("close_action"),
        "close_qty": executable_intent.get("close_qty"),
        "lifecycle_identity": _managed_lifecycle_identity(executable_intent),
    }


def _exit_authority_checks_clear_order_risk(decision: Mapping[str, Any]) -> bool:
    hard_checks = _mapping(decision.get("hard_required_checks"))
    conditional_checks = _mapping(decision.get("conditional_risk_checks") or decision.get("conditional_checks"))
    working_close = _mapping(hard_checks.get("same_contract_working_close_does_not_over_close"))
    unknown_order = _mapping(conditional_checks.get("same_contract_unknown_order_risk"))
    return working_close.get("passed") is True and unknown_order.get("passed") is True


def _exit_authority_checks_prove_current_managed_exposure(
    decision: Mapping[str, Any],
    executable_intent: Mapping[str, Any],
) -> bool:
    hard_checks = _mapping(decision.get("hard_required_checks"))
    required = (
        "known_current_broker_position",
        "account_matches",
        "execution_domain_matches",
        "contract_matches",
        "close_qty_within_broker_position",
        "risk_reducing_action",
    )
    if any(_mapping(hard_checks.get(name)).get("passed") is not True for name in required):
        return False
    attribution = _mapping(decision.get("attribution_diagnostics"))
    if attribution and attribution.get("blocks_authority") is True:
        return False
    return bool(_managed_lifecycle_identity(executable_intent))


def _classify_working_close_order_maintenance_authority(config: TrackBManagedExitServiceConfig) -> dict[str, Any]:
    managed_orders = _read_json(config.resolve(DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT))
    blockers: list[str] = []
    diagnostics: list[dict[str, Any]] = []
    candidates: list[dict[str, Any]] = []
    for order in managed_orders.get("managed_orders") or []:
        order = _mapping(order)
        if not order or order.get("is_close_order") is not True:
            continue
        if str(order.get("classification") or "") not in {
            "WORKING_CLOSE_ORDER",
            "CLOSE_ORDER_MODIFIABLE",
            "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
        }:
            continue
        candidates.append(order)

    if not candidates:
        blockers.append("working_close_order_missing")
    if len(candidates) > 1:
        blockers.append("multiple_working_close_orders")
    order = candidates[0] if candidates else {}
    _require(blockers, str(order.get("account_id") or "") == _PAPER_ACCOUNT_ID, "wrong_account")
    _require(blockers, order.get("live_money_eligible") is not True, "live_money_eligible")
    _require(blockers, order.get("paper_proof_invoked") is not True, "paper_proof_invoked")
    _require(blockers, str(order.get("broker_order_id") or ""), "broker_order_id_missing")
    _require(blockers, str(order.get("perm_id") or ""), "perm_id_missing")
    _require(blockers, str(order.get("action") or "").upper() in {"BUY", "SELL"}, "close_order_action_invalid")
    quantity = _decimal(order.get("quantity"))
    _require(blockers, quantity is not None and quantity > 0, "close_order_quantity_missing_or_zero")

    managed_position = _mapping(order.get("canonical_managed_position")) or _mapping(_mapping(order.get("broker_position")).get("canonical_managed_position"))
    broker_position = _mapping(order.get("broker_position")) or _mapping(managed_position.get("broker_position"))
    broker_qty = _decimal(broker_position.get("quantity") or managed_position.get("aggregate_qty"))
    action = str(order.get("action") or "").upper()
    if broker_qty is not None and quantity is not None:
        expected_action = "SELL" if broker_qty > 0 else "BUY"
        _require(blockers, action == expected_action, "close_order_not_risk_reducing")
        _require(blockers, quantity <= abs(broker_qty), "close_order_over_close_risk")
    else:
        blockers.append("broker_position_unavailable")
    _require(blockers, bool(order.get("lifecycle_id") or managed_position.get("lifecycle_id")), "managed_lifecycle_identity_missing")
    diagnostics.append({"kind": "managed_order_registry", "classification": managed_orders.get("classification")})

    return {
        "classification": MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_ALLOWED
        if not blockers
        else MANAGED_PAPER_RISK_REDUCING_EXIT_AUTHORITY_BLOCKED,
        "code": "managed_paper_risk_reducing_working_close_authority",
        "blockers": blockers,
        "diagnostics": diagnostics,
        "broker_order_id": order.get("broker_order_id"),
        "perm_id": order.get("perm_id"),
        "account_id": order.get("account_id"),
        "close_action": order.get("action"),
        "close_qty": order.get("quantity"),
    }


def _source_classifications(execution_plan: Mapping[str, Any]) -> dict[str, Any]:
    pipeline = _mapping(execution_plan.get("pipeline"))
    source_classifications = _mapping(pipeline.get("source_classifications"))
    if source_classifications:
        return source_classifications
    for diagnostic in _list(execution_plan.get("diagnostics")):
        diagnostic = _mapping(diagnostic)
        if diagnostic.get("kind") == "legacy_source_classifications":
            return _mapping(diagnostic.get("rows"))
    return {}


def _managed_lifecycle_identity(intent: Mapping[str, Any]) -> dict[str, Any]:
    attribution = _mapping(intent.get("attribution"))
    lifecycle_id = str(intent.get("lifecycle_id") or attribution.get("lifecycle_id") or "").strip()
    trade_id = str(intent.get("trade_id") or attribution.get("trade_id") or "").strip()
    strategy_id = str(intent.get("strategy_id") or attribution.get("strategy_id") or "").strip()
    lane_id = str(intent.get("lane_id") or attribution.get("lane_id") or "").strip()
    if not lifecycle_id or not trade_id:
        return {}
    return {
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "strategy_id": strategy_id or None,
        "lane_id": lane_id or None,
    }


def _require(blockers: list[str], condition: object, code: str) -> None:
    if not condition:
        blockers.append(code)


def _require_check_passed(blockers: list[str], decision: Mapping[str, Any], check_name: str, code: str) -> None:
    checks = _mapping(decision.get("hard_required_checks"))
    if not checks:
        blockers.append("hard_required_checks_missing")
        return
    if _mapping(checks.get(check_name)).get("passed") is not True:
        blockers.append(code)


def _require_conditional_check_passed(blockers: list[str], decision: Mapping[str, Any], check_name: str, code: str) -> None:
    checks = _mapping(decision.get("conditional_risk_checks") or decision.get("conditional_checks"))
    if not checks:
        return
    if _mapping(checks.get(check_name)).get("passed") is not True:
        blockers.append(code)


def _managed_order_maintenance_report(
    *,
    now: datetime,
    managed_orders: Mapping[str, Any],
    order_adjustment_plan: Mapping[str, Any],
    results: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    if not results:
        classification = "MANAGED_ORDER_MAINTENANCE_NO_ACTION"
    elif any(row.get("broker_mutation_performed") is True for row in results):
        classification = "MANAGED_ORDER_MAINTENANCE_APPLIED"
    elif any(str(row.get("classification") or "") == "MODIFY_IN_PLACE_DRY_RUN_READY" for row in results):
        classification = "MANAGED_ORDER_MAINTENANCE_DRY_RUN_READY"
    elif any("BLOCKED" in str(row.get("classification") or "") for row in results):
        classification = "MANAGED_ORDER_MAINTENANCE_BLOCKED"
    else:
        classification = "MANAGED_ORDER_MAINTENANCE_WAIT"
    return {
        "schema_version": "track_b_managed_exit_service_order_maintenance_v1",
        "generated_at": now.isoformat(),
        "classification": classification,
        "close_only": True,
        "entry_allowed": False,
        "broad_cancel_allowed": False,
        "global_cancel_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "managed_order_classification": managed_orders.get("classification"),
        "order_adjustment_classification": order_adjustment_plan.get("classification"),
        "managed_order_count": len(managed_orders.get("managed_orders") or []),
        "result_count": len(results),
        "results": list(results),
        "broker_mutation_attempted": any(row.get("broker_mutation_attempted") is True for row in results),
        "broker_mutation_performed": any(row.get("broker_mutation_performed") is True for row in results),
    }


def _modify_config_from_order_plan(
    *,
    service_config: TrackBManagedExitServiceConfig,
    order: Mapping[str, Any],
    plan: Mapping[str, Any],
    timeout_seconds: float,
) -> ManagedOrderModifyInPlaceConfig | None:
    identity = _mapping(plan.get("identity"))
    source_order = _mapping(plan.get("source_order"))
    broker_order_id = _string_or_none(order.get("broker_order_id") or plan.get("broker_order_id") or identity.get("broker_order_id"))
    perm_id = _string_or_none(order.get("perm_id") or plan.get("perm_id") or identity.get("perm_id"))
    action = _string_or_none(order.get("action") or plan.get("action") or identity.get("action"))
    quantity = _string_or_none(order.get("quantity") or plan.get("quantity") or identity.get("quantity"))
    contract = _string_or_none(order.get("contract") or order.get("local_symbol") or plan.get("contract") or identity.get("contract"))
    symbol = _string_or_none(order.get("symbol") or plan.get("symbol") or source_order.get("symbol"))
    account_id = _string_or_none(order.get("account_id") or identity.get("account_id") or source_order.get("account_id") or "DUM882026")
    current_limit = _decimal(order.get("limit_price") or plan.get("limit_price"))
    new_limit = _decimal(_mapping(plan.get("managed_close_reprice_policy")).get("limit_price"))
    client_id = _int_or_none(
        order.get("client_id")
        or plan.get("client_id")
        or source_order.get("client_id")
        or _mapping(order.get("source_order")).get("client_id")
    )
    if new_limit is None:
        new_limit = _marketable_limit_from_plan(order=order, plan=plan)
    if not all([broker_order_id, perm_id, action, quantity, contract, symbol, account_id, current_limit, new_limit]):
        return None
    return ManagedOrderModifyInPlaceConfig(
        repo_root=service_config.repo_root,
        broker_order_id=str(broker_order_id),
        perm_id=str(perm_id),
        symbol=str(symbol).upper(),
        contract=str(contract).upper(),
        con_id=_string_or_none(order.get("con_id") or plan.get("con_id") or identity.get("con_id")),
        action=str(action).upper(),
        quantity=str(_decimal(quantity) or quantity),
        current_known_limit=str(current_limit),
        new_limit=str(new_limit),
        account_id=str(account_id),
        apply=service_config.apply is True,
        operator_authorized_modify=service_config.apply is True,
        broker_timeout_seconds=float(timeout_seconds),
        tws_client_id=int(client_id) if client_id is not None else 1967,
    )


def _marketable_limit_from_plan(*, order: Mapping[str, Any], plan: Mapping[str, Any]) -> Decimal | None:
    market_ref = _mapping(plan.get("market_reference")) or _mapping(_mapping(order.get("marketability")).get("market_reference"))
    reference = _decimal(market_ref.get("reference_price"))
    if reference is None:
        return None
    action = str(order.get("action") or plan.get("action") or "").upper()
    symbol = str(order.get("symbol") or plan.get("symbol") or "").upper()
    tick = Decimal("0.25") if symbol in {"MNQ", "MES", "NQ", "ES"} else Decimal("0.1")
    offset = tick * Decimal("4")
    if action == "SELL":
        return reference - offset
    if action == "BUY":
        return reference + offset
    return None


def _run_actuator_child(
    config: TrackBManagedExitActuatorConfig,
    now: datetime,
    *,
    timeout_seconds: float,
    command_runner: CommandRunner | None = None,
) -> Mapping[str, Any]:
    command_runner = command_runner or _run_command
    command = [
        sys.executable,
        "-m",
        "mgc_v05l.execution_core.track_b_managed_exit_actuator",
        "--repo-root",
        str(config.repo_root),
        "--output-path",
        str(config.output_path),
        "--max-closes-per-run",
        str(config.max_closes_per_run or 1),
        "--json",
    ]
    if config.apply:
        command.append("--apply")
    if config.operator_authorized_managed_exit:
        command.append("--operator-authorized-managed-exit")
    completed = command_runner(command, config.repo_root, timeout_seconds)
    if completed.returncode == 124:
        return {
            "classification": MANAGED_EXIT_SERVICE_ACTUATOR_TIMEOUT,
            "generated_at": now.isoformat(),
            "timeout_seconds": timeout_seconds,
            "command": command,
            "stdout_tail": _tail(completed.stdout),
            "stderr_tail": _tail(completed.stderr),
            "submit_attempted": False,
            "submitted_count": 0,
            "broker_state_mutated": False,
        }
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        return {
            "classification": MANAGED_EXIT_SERVICE_ERROR,
            "generated_at": now.isoformat(),
            "returncode": completed.returncode,
            "command": command,
            "stdout_tail": _tail(completed.stdout),
            "stderr_tail": _tail(completed.stderr),
            "submit_attempted": False,
            "submitted_count": 0,
            "broker_state_mutated": False,
        }
    if isinstance(payload, Mapping):
        result = dict(payload)
        result.setdefault("returncode", completed.returncode)
        result.setdefault("command", command)
        result.setdefault("stdout_tail", _tail(completed.stdout))
        result.setdefault("stderr_tail", _tail(completed.stderr))
        return result
    return {
        "classification": MANAGED_EXIT_SERVICE_ERROR,
        "generated_at": now.isoformat(),
        "returncode": completed.returncode,
        "command": command,
        "stdout_tail": _tail(completed.stdout),
        "stderr_tail": _tail(completed.stderr),
        "submit_attempted": False,
        "submitted_count": 0,
        "broker_state_mutated": False,
    }


def _run_command(command: Sequence[str], repo_root: Path, timeout_seconds: float) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{repo_root / 'src'}{':' + existing_pythonpath if existing_pythonpath else ''}"
    process = subprocess.Popen(
        list(command),
        cwd=repo_root,
        env=env,
        text=True,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        start_new_session=True,
    )
    try:
        stdout, stderr = process.communicate(timeout=timeout_seconds)
    except subprocess.TimeoutExpired:
        try:
            os.killpg(process.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
        except OSError:
            process.kill()
        stdout, stderr = process.communicate(timeout=10)
        return subprocess.CompletedProcess(list(command), 124, stdout=stdout or "", stderr=stderr or "")
    return subprocess.CompletedProcess(list(command), process.returncode, stdout=stdout or "", stderr=stderr or "")


def _phase_timing(phase: str, started_monotonic: float) -> dict[str, Any]:
    return {
        "phase": phase,
        "duration_seconds": round(max(time.monotonic() - started_monotonic, 0.0), 3),
    }


def _service_label(config: TrackBManagedExitServiceConfig) -> str | None:
    return config.service_label or os.environ.get("TRACK_B_MANAGED_EXIT_SERVICE_LABEL")


def _tail(value: str | None, limit: int = 4000) -> str:
    text = value or ""
    return text[-limit:]


def _run_authority_refresh(
    authority_refresher: AuthorityRefresher,
    config: TrackBManagedExitServiceConfig,
    phase: str,
) -> dict[str, Any]:
    try:
        payload = dict(authority_refresher(config, phase))
    except Exception as exc:  # defensive: fail closed and surface the refresh fault.
        return {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_MANAGED_EXIT_SERVICE_AUTHORITY_REFRESH_EXCEPTION",
            "error": str(exc),
        }
    payload.setdefault("phase", phase)
    payload.setdefault("succeeded", False)
    return payload


def _run_pipeline_dry_run(config: TrackBManagedExitServiceConfig, now: datetime) -> Mapping[str, Any]:
    return build_track_b_managed_exit_pipeline_dry_run_report(
        config=TrackBManagedExitPipelineDryRunConfig(repo_root=config.repo_root),
        now=now,
    )


def _run_broker_truth_sweeper(
    *,
    config: TrackBManagedExitServiceConfig,
    now: datetime,
    write: bool,
) -> dict[str, Any]:
    positions_snapshot = _read_json(config.resolve(DEFAULT_BROKER_POSITIONS_SNAPSHOT))
    open_orders_snapshot = _read_json(config.resolve(DEFAULT_BROKER_OPEN_ORDERS_SNAPSHOT))
    if _positions_complete(positions_snapshot) is not True:
        return {
            "classification": "MANAGED_EXIT_BROKER_TRUTH_SWEEP_BLOCKED",
            "reason": "broker_positions_unavailable",
            "broker_state_mutated": False,
            "submit_attempted": False,
        }
    broker_positions = _current_track_b_broker_positions(positions_snapshot)
    if not broker_positions:
        return {
            "classification": "MANAGED_EXIT_BROKER_TRUTH_SWEEP_NO_POSITIONS",
            "broker_position_count": 0,
            "broker_open_order_count": len(_list(open_orders_snapshot.get("open_orders"))),
            "broker_state_mutated": False,
            "submit_attempted": False,
        }
    registry_path = config.resolve(DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT)
    registry = _read_json(registry_path)
    managed_positions = [_mapping(row) for row in _list(registry.get("managed_positions"))]
    terminal_records = load_live_trade_registry_records(repo_root=config.repo_root)
    diagnostics: list[dict[str, Any]] = []
    changed = False
    visible_positions: list[dict[str, Any]] = []
    for broker_position in broker_positions:
        identity = _broker_position_identity(broker_position)
        if not identity.get("local_symbol") or not identity.get("con_id"):
            diagnostics.append(
                {
                    "classification": "MANAGED_EXIT_SERVICE_RESTART_OR_SCHEMA_REPAIR_REQUIRED",
                    "reason": "broker_contract_identity_unparseable",
                    "broker_position": broker_position,
                    "contract_identity": identity,
                }
            )
            continue
        index, existing = _matching_managed_position(
            managed_positions,
            broker_position,
            terminal_records=terminal_records,
        )
        if existing is None:
            lifecycle = _best_lifecycle_for_broker_position(
                config=config,
                broker_position=broker_position,
                terminal_records=terminal_records,
            )
            if not identity.get("con_id") and not _int_or_none(_mapping(lifecycle).get("con_id")):
                diagnostics.append(
                    {
                        "classification": "MANAGED_EXIT_SERVICE_RESTART_OR_SCHEMA_REPAIR_REQUIRED",
                        "reason": "broker_contract_identity_unparseable",
                        "broker_position": broker_position,
                        "contract_identity": identity,
                    }
                )
                continue
            adopted = _adopt_broker_position(
                broker_position=broker_position,
                lifecycle=lifecycle,
                terminal_records=terminal_records,
                now=now,
            )
            managed_positions.append(adopted)
            visible_positions.append(adopted)
            changed = True
            diagnostics.append(
                {
                    "classification": adopted.get("classification"),
                    "reason": "broker_truth_position_adopted",
                    "local_symbol": adopted.get("local_symbol"),
                    "con_id": adopted.get("con_id"),
                    "lifecycle_id": adopted.get("lifecycle_id"),
                    "managed_exit_policy_id": adopted.get("managed_exit_policy_id"),
                }
            )
            continue
        superseded = _demote_stale_same_contract_managed_positions(
            managed_positions=managed_positions,
            selected_index=index,
            broker_position=broker_position,
            selected_position=existing,
            terminal_records=terminal_records,
            now=now,
        )
        if superseded:
            changed = True
            diagnostics.extend(superseded)
        repaired = _repair_managed_position_from_broker(
            existing,
            broker_position,
            terminal_records=terminal_records,
            now=now,
        )
        visible_positions.append(repaired)
        if repaired != existing:
            managed_positions[index] = repaired
            changed = True
            diagnostics.append(
                {
                    "classification": "MANAGED_EXIT_BROKER_TRUTH_POSITION_REPAIRED",
                    "reason": "broker_truth_contract_identity_refreshed",
                    "local_symbol": repaired.get("local_symbol"),
                    "con_id": repaired.get("con_id"),
                    "lifecycle_id": repaired.get("lifecycle_id"),
                }
            )
    classification = _sweeper_classification(visible_positions=visible_positions, diagnostics=diagnostics)
    if changed and write:
        payload = dict(registry)
        payload.update(
            {
                "schema_version": payload.get("schema_version") or "track_b_managed_position_registry_v1",
                "generated_at": now.isoformat(),
                "classification": _managed_registry_classification(managed_positions),
                "managed_positions": managed_positions,
                "managed_position_count": len(managed_positions),
                "broker_truth_sweeper": {
                    "classification": classification,
                    "generated_at": now.isoformat(),
                    "diagnostics": diagnostics,
                },
            }
        )
        write_json_atomic(registry_path, to_jsonable(payload))
        write_json_atomic(config.resolve(DEFAULT_OPERATOR_MANAGED_POSITION_ARTIFACT), to_jsonable(payload))
    return {
        "classification": classification,
        "broker_position_count": len(broker_positions),
        "managed_position_count": len(visible_positions),
        "registry_updated": changed and write,
        "broker_open_order_count": len(_list(open_orders_snapshot.get("open_orders"))),
        "diagnostics": diagnostics,
        "broker_state_mutated": False,
        "submit_attempted": False,
    }


def _positions_complete(snapshot: Mapping[str, Any]) -> bool:
    if snapshot.get("positions_complete") is True or snapshot.get("ok") is True:
        return True
    return bool(snapshot.get("positions")) and str(snapshot.get("selected_account_id") or snapshot.get("account") or "") == _PAPER_ACCOUNT_ID


def _current_track_b_broker_positions(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    positions: list[dict[str, Any]] = []
    for row in (_mapping(item) for item in _list(snapshot.get("positions"))):
        if str(row.get("account_id") or row.get("account") or snapshot.get("account") or "") != _PAPER_ACCOUNT_ID:
            continue
        normalized = normalize_track_b_contract_row(row, account_id=_PAPER_ACCOUNT_ID)
        if not _is_track_b_futures_broker_position(normalized):
            continue
        quantity = _decimal(normalized.get("quantity") or normalized.get("position") or normalized.get("signed_qty")) or Decimal("0")
        if quantity == 0:
            continue
        positions.append(normalized)
    return positions


def _is_track_b_futures_broker_position(row: Mapping[str, Any]) -> bool:
    sec_type = str(row.get("security_type") or row.get("secType") or "").strip().upper()
    if sec_type and sec_type != "FUT":
        return False
    return _instrument_from_position(row) in {"MES", "MNQ", "MGC", "ES", "NQ", "GC", "ZT"}


def _instrument_from_position(row: Mapping[str, Any]) -> str:
    contract_identity = _mapping(row.get("contract_identity"))
    symbol = str(
        row.get("symbol")
        or row.get("track_b_root")
        or row.get("instrument")
        or row.get("instrument_family")
        or contract_identity.get("symbol")
        or ""
    ).strip().upper()
    if symbol:
        return symbol
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    return "".join(ch for ch in local_symbol if ch.isalpha())[:3]


def _broker_position_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    contract_identity = _mapping(row.get("contract_identity"))
    resolved = contract_identity if contract_identity.get("resolved") is True else {}
    return {
        "account_id": str(row.get("account_id") or row.get("account") or resolved.get("account_id") or _PAPER_ACCOUNT_ID),
        "con_id": _int_or_none(row.get("con_id") or row.get("conId") or resolved.get("con_id")),
        "local_symbol": _string_or_none(row.get("local_symbol") or row.get("localSymbol") or resolved.get("local_symbol")),
        "expiry": _string_or_none(row.get("expiry") or row.get("lastTradeDateOrContractMonth") or resolved.get("expiry")),
        "contract_key": _string_or_none(row.get("contract_key") or resolved.get("contract_key")),
        "instrument_family": _instrument_from_position(row),
        "classification": contract_identity.get("classification"),
        "resolved": contract_identity.get("resolved") is True,
        "blockers": _list(contract_identity.get("blockers")),
    }


def _matching_managed_position(
    managed_positions: Sequence[Mapping[str, Any]],
    broker_position: Mapping[str, Any],
    *,
    terminal_records: Sequence[Any] = (),
) -> tuple[int, dict[str, Any] | None]:
    identity = _broker_position_identity(broker_position)
    candidates: list[tuple[tuple[int, str, str], int, dict[str, Any]]] = []
    for index, row in enumerate(managed_positions):
        if _managed_position_diagnostic_only(row):
            continue
        broker_nested = _mapping(row.get("broker_position"))
        row_con_id = _int_or_none(row.get("con_id") or broker_nested.get("con_id"))
        row_symbol = _string_or_none(row.get("local_symbol") or broker_nested.get("local_symbol"))
        row_account = str(broker_nested.get("account_id") or row.get("account_id") or _PAPER_ACCOUNT_ID)
        if row_account != identity["account_id"]:
            continue
        if row_con_id and identity["con_id"] and row_con_id == identity["con_id"]:
            candidates.append((_managed_position_priority(row, broker_position, terminal_records), index, dict(row)))
            continue
        if row_symbol and identity["local_symbol"] and row_symbol == identity["local_symbol"]:
            candidates.append((_managed_position_priority(row, broker_position, terminal_records), index, dict(row)))
    if not candidates:
        return -1, None
    _, index, row = max(candidates, key=lambda item: item[0])
    return index, row


def _demote_stale_same_contract_managed_positions(
    *,
    managed_positions: list[dict[str, Any]],
    selected_index: int,
    broker_position: Mapping[str, Any],
    selected_position: Mapping[str, Any],
    terminal_records: Sequence[Any],
    now: datetime,
) -> list[dict[str, Any]]:
    diagnostics: list[dict[str, Any]] = []
    selected_lifecycle_id = _string_or_none(selected_position.get("lifecycle_id"))
    selected_trade_id = _string_or_none(selected_position.get("trade_id"))
    for index, row in enumerate(list(managed_positions)):
        if index == selected_index or _managed_position_diagnostic_only(row):
            continue
        if not _managed_position_matches_broker_position(row, broker_position):
            continue
        row_priority = _managed_position_priority(row, broker_position, terminal_records)
        selected_priority = _managed_position_priority(selected_position, broker_position, terminal_records)
        if row_priority >= selected_priority:
            continue
        lifecycle_id = _string_or_none(row.get("lifecycle_id"))
        trade_id = _string_or_none(row.get("trade_id"))
        managed_positions[index] = {
            **dict(row),
            "classification": "STALE_SUPERSEDED_LIFECYCLE_PROJECTION",
            "diagnostic_only": True,
            "current_hot_path_scope": "FULL_AUDIT_ONLY",
            "superseded_by_lifecycle_id": selected_lifecycle_id,
            "superseded_by_trade_id": selected_trade_id,
            "superseded_at": now.isoformat(),
            "superseded_reason": "freshest_broker_backed_same_contract_lifecycle_selected",
        }
        diagnostics.append(
            {
                "classification": "STALE_SUPERSEDED_LIFECYCLE_PROJECTION",
                "reason": "freshest_broker_backed_same_contract_lifecycle_selected",
                "local_symbol": _broker_position_identity(broker_position).get("local_symbol"),
                "con_id": _broker_position_identity(broker_position).get("con_id"),
                "superseded_lifecycle_id": lifecycle_id,
                "superseded_trade_id": trade_id,
                "selected_lifecycle_id": selected_lifecycle_id,
                "selected_trade_id": selected_trade_id,
            }
        )
    return diagnostics


def _managed_position_priority(
    row: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    terminal_records: Sequence[Any],
) -> tuple[int, str, str]:
    record_state = _registry_record_current_state(row, terminal_records)
    if record_state == "CLOSED_FLAT":
        state_score = 0
    elif record_state in {"OPEN_MANAGED", "EXIT_DUE", "WORKING_EXIT"}:
        state_score = 3
    else:
        state_score = 1
    entry_fill = _entry_fill_event_from_registry(
        position=row,
        broker_position=broker_position,
        terminal_records=terminal_records,
    )
    entry_time = (
        getattr(entry_fill, "generated_at", None).isoformat()
        if entry_fill is not None and getattr(entry_fill, "generated_at", None) is not None
        else str(row.get("entry_time") or row.get("entry_timestamp") or "")
    )
    lifecycle_id = _string_or_none(row.get("lifecycle_id")) or ""
    return state_score, entry_time, lifecycle_id


def _registry_record_current_state(row: Mapping[str, Any], terminal_records: Sequence[Any]) -> str | None:
    lifecycle_id = _string_or_none(row.get("lifecycle_id"))
    trade_id = _string_or_none(row.get("trade_id"))
    for record in terminal_records:
        owner = getattr(record, "ownership_identity", None)
        if trade_id and getattr(record, "trade_id", None) == trade_id:
            return str(getattr(getattr(record, "current_state", None), "value", getattr(record, "current_state", "")))
        if lifecycle_id and owner is not None and getattr(owner, "lifecycle_id", None) == lifecycle_id:
            return str(getattr(getattr(record, "current_state", None), "value", getattr(record, "current_state", "")))
    return None


def _managed_position_diagnostic_only(row: Mapping[str, Any]) -> bool:
    if row.get("diagnostic_only") is True or row.get("historical_only") is True:
        return True
    classification = str(row.get("classification") or "").upper()
    scope = str(row.get("current_hot_path_scope") or row.get("scope") or "").upper()
    return "STALE_SUPERSEDED" in classification or "FULL_AUDIT_ONLY" in scope or "DIAGNOSTIC" in scope


def _managed_position_matches_broker_position(row: Mapping[str, Any], broker_position: Mapping[str, Any]) -> bool:
    identity = _broker_position_identity(broker_position)
    broker_nested = _mapping(row.get("broker_position"))
    row_account = str(broker_nested.get("account_id") or row.get("account_id") or _PAPER_ACCOUNT_ID)
    if row_account != identity["account_id"]:
        return False
    row_con_id = _int_or_none(row.get("con_id") or broker_nested.get("con_id"))
    row_symbol = _string_or_none(row.get("local_symbol") or broker_nested.get("local_symbol"))
    if row_con_id and identity["con_id"] and row_con_id == identity["con_id"]:
        return True
    return bool(row_symbol and identity["local_symbol"] and row_symbol == identity["local_symbol"])


def _repair_managed_position_from_broker(
    managed_position: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    *,
    terminal_records: Sequence[Any] = (),
    now: datetime,
) -> dict[str, Any]:
    repaired = dict(managed_position)
    identity = _broker_position_identity(broker_position)
    con_id = identity["con_id"] or _int_or_none(managed_position.get("con_id") or _mapping(managed_position.get("broker_position")).get("con_id"))
    repaired["broker_position"] = dict(broker_position)
    repaired["account_id"] = identity["account_id"]
    repaired["con_id"] = con_id
    repaired["local_symbol"] = identity["local_symbol"]
    repaired["expiry"] = identity["expiry"] or repaired.get("expiry")
    repaired["symbol"] = identity["instrument_family"] or repaired.get("symbol")
    repaired["track_b_root"] = identity["instrument_family"] or repaired.get("track_b_root")
    repaired["side"] = repaired.get("side") or _side_from_signed_quantity(broker_position.get("quantity"))
    repaired["quantity"] = repaired.get("quantity") or str(abs(_decimal(broker_position.get("quantity")) or Decimal("0")))
    repaired["aggregate_qty"] = repaired.get("aggregate_qty") or str(broker_position.get("quantity") or "")
    repaired["freshness_state"] = "FRESH"
    repaired["broker_qty_match"] = True
    repaired["broker_truth_swept_at"] = now.isoformat()
    if not repaired.get("managed_exit_policy_id"):
        repaired["managed_exit_policy_id"] = _managed_exit_policy_from_lane(repaired.get("lane_id") or repaired.get("strategy_id"))
    entry_fill = _entry_fill_event_from_registry(
        position=repaired,
        broker_position=broker_position,
        terminal_records=terminal_records,
    )
    if entry_fill is not None:
        if not repaired.get("entry_time"):
            repaired["entry_time"] = getattr(entry_fill, "generated_at").isoformat()
        if not repaired.get("entry_price") and getattr(entry_fill, "price", None) is not None:
            repaired["entry_price"] = str(getattr(entry_fill, "price"))
        if not repaired.get("entry_order_ids") and getattr(entry_fill, "order_id", None):
            repaired["entry_order_ids"] = [str(getattr(entry_fill, "order_id"))]
        if not repaired.get("entry_perm_ids") and getattr(entry_fill, "perm_id", None):
            repaired["entry_perm_ids"] = [str(getattr(entry_fill, "perm_id"))]
        if not repaired.get("entry_exec_ids") and getattr(entry_fill, "exec_id", None):
            repaired["entry_exec_ids"] = [str(getattr(entry_fill, "exec_id"))]
    if repaired.get("managed_exit_policy_id"):
        repaired["lifecycle_units"] = [
            {
                **dict(unit),
                "managed_exit_policy_id": unit.get("managed_exit_policy_id") or repaired.get("managed_exit_policy_id"),
                "entry_time": unit.get("entry_time") or repaired.get("entry_time"),
            }
            for unit in _list(repaired.get("lifecycle_units"))
            if isinstance(unit, Mapping)
        ] or repaired.get("lifecycle_units")
    if not repaired.get("managed_exit_policy_id"):
        repaired["classification"] = "STRAY_POSITION_REVIEW_REQUIRED"
        repaired["attention_required"] = True
    elif str(repaired.get("classification") or "") in {"", "LIFECYCLE_WITHOUT_BROKER", "NO_MANAGED_POSITIONS"}:
        repaired["classification"] = "OPEN_MANAGED_MATCHED"
    return repaired


def _adopt_broker_position(
    *,
    broker_position: Mapping[str, Any],
    lifecycle: Mapping[str, Any] | None,
    terminal_records: Sequence[Any] = (),
    now: datetime,
) -> dict[str, Any]:
    identity = _broker_position_identity(broker_position)
    lifecycle = _mapping(lifecycle)
    con_id = identity["con_id"] or _int_or_none(lifecycle.get("con_id"))
    local_symbol = identity["local_symbol"] or _string_or_none(lifecycle.get("local_symbol"))
    lane_id = _string_or_none(lifecycle.get("lane_id") or lifecycle.get("strategy_id"))
    policy_id = _string_or_none(lifecycle.get("managed_exit_policy_id")) or _managed_exit_policy_from_lane(lane_id)
    quantity = abs(_decimal(broker_position.get("quantity")) or Decimal("0"))
    signed_quantity = _decimal(broker_position.get("quantity")) or Decimal("0")
    classification = "OPEN_MANAGED_MATCHED" if policy_id else "STRAY_POSITION_REVIEW_REQUIRED"
    lifecycle_id = _string_or_none(lifecycle.get("lifecycle_id")) or f"broker_truth_adopted_{identity['local_symbol']}"
    trade_id = _string_or_none(lifecycle.get("trade_id")) or f"trade_broker_truth_adopted_{identity['local_symbol']}"
    entry_fill = _entry_fill_event_from_registry(
        position={**lifecycle, "lifecycle_id": lifecycle_id, "trade_id": trade_id, "lane_id": lane_id},
        broker_position=broker_position,
        terminal_records=terminal_records,
    )
    entry_time = lifecycle.get("entry_timestamp") or lifecycle.get("entry_time")
    if not entry_time and entry_fill is not None:
        entry_time = getattr(entry_fill, "generated_at").isoformat()
    entry_price = lifecycle.get("entry_price") or lifecycle.get("avg_entry_price")
    if not entry_price and entry_fill is not None and getattr(entry_fill, "price", None) is not None:
        entry_price = str(getattr(entry_fill, "price"))
    return {
        "classification": classification,
        "source": "BROKER_TRUTH_SWEEPER",
        "account_id": identity["account_id"],
        "symbol": identity["instrument_family"],
        "track_b_root": identity["instrument_family"],
        "instrument_family": identity["instrument_family"],
        "local_symbol": local_symbol,
        "con_id": con_id,
        "expiry": identity["expiry"],
        "side": _side_from_signed_quantity(signed_quantity),
        "quantity": str(quantity),
        "aggregate_qty": str(signed_quantity),
        "signed_broker_qty": str(signed_quantity),
        "broker_qty_match": True,
        "broker_position": dict(broker_position),
        "lane_id": lane_id,
        "strategy_id": lane_id,
        "lifecycle_id": lifecycle_id,
        "trade_id": trade_id,
        "managed_exit_policy_id": policy_id,
        "entry_time": entry_time,
        "entry_price": entry_price,
        "entry_order_ids": lifecycle.get("entry_order_ids") or ([lifecycle.get("entry_order_id")] if lifecycle.get("entry_order_id") else []),
        "entry_perm_ids": lifecycle.get("entry_perm_ids") or ([lifecycle.get("entry_perm_id")] if lifecycle.get("entry_perm_id") else []),
        "entry_exec_ids": lifecycle.get("entry_exec_ids") or ([lifecycle.get("entry_exec_id")] if lifecycle.get("entry_exec_id") else []),
        "freshness_state": "FRESH",
        "attention_required": not bool(policy_id),
        "diagnostic_only": False,
        "broker_truth_swept_at": now.isoformat(),
        "review_reason": None if policy_id else "managed_exit_policy_unresolved",
    }


def _best_lifecycle_for_broker_position(
    *,
    config: TrackBManagedExitServiceConfig,
    broker_position: Mapping[str, Any],
    terminal_records: Sequence[Any] = (),
) -> dict[str, Any] | None:
    identity = _broker_position_identity(broker_position)
    lifecycle_root = config.resolve(DEFAULT_STRATEGY_LIFECYCLE_ROOT)
    candidates: list[dict[str, Any]] = []
    if lifecycle_root.exists():
        for report_path in lifecycle_root.glob("*/track_b_strategy_managed_paper_lifecycle_report.json"):
            report = _read_json(report_path)
            if not report or report.get("close_fill"):
                continue
            local_symbol = _string_or_none(report.get("local_symbol") or _mapping(report.get("entry_intent")).get("local_symbol"))
            con_id = _int_or_none(report.get("con_id") or _mapping(report.get("entry_intent")).get("con_id"))
            if identity["local_symbol"] and local_symbol and local_symbol != identity["local_symbol"]:
                continue
            if identity["con_id"] and con_id and con_id != identity["con_id"]:
                continue
            if not local_symbol and not con_id:
                continue
            entry_fill = _mapping(report.get("entry_fill"))
            entry_intent = _mapping(report.get("entry_intent"))
            candidates.append(
                {
                    "lifecycle_id": report.get("lifecycle_id"),
                    "trade_id": report.get("trade_id"),
                    "lane_id": report.get("lane_id") or report.get("strategy_id") or entry_intent.get("lane_id"),
                    "strategy_id": report.get("strategy_id") or entry_intent.get("strategy_id"),
                    "managed_exit_policy_id": report.get("managed_exit_policy_id") or entry_intent.get("managed_exit_policy_id"),
                    "entry_timestamp": entry_fill.get("filled_at") or _mapping(report.get("open_state")).get("entry_timestamp"),
                    "entry_price": entry_fill.get("price") or _mapping(report.get("open_state")).get("entry_price"),
                    "entry_order_id": entry_fill.get("broker_order_id"),
                    "entry_perm_id": entry_fill.get("perm_id"),
                    "entry_exec_id": entry_fill.get("execution_id") or entry_fill.get("exec_id"),
                    "report_path": str(report_path),
                }
            )
    candidates.extend(
        _registry_lifecycle_candidates_for_broker_position(
            broker_position=broker_position,
            terminal_records=terminal_records,
        )
    )
    return max(candidates, key=lambda row: str(row.get("entry_timestamp") or "")) if candidates else None


def _registry_lifecycle_candidates_for_broker_position(
    *,
    broker_position: Mapping[str, Any],
    terminal_records: Sequence[Any],
) -> list[dict[str, Any]]:
    identity = _broker_position_identity(broker_position)
    candidates: list[dict[str, Any]] = []
    for record in terminal_records:
        state = str(getattr(getattr(record, "current_state", None), "value", getattr(record, "current_state", "")))
        if state == "CLOSED_FLAT":
            continue
        for event in getattr(record, "event_chain", ()) or ():
            if str(getattr(getattr(event, "event_type", ""), "value", getattr(event, "event_type", ""))) != "ENTRY_FILL_BROKER_BACKED":
                continue
            if not _event_matches_broker_position(event, identity, None):
                continue
            lane_id = _string_or_none(getattr(event, "lane_id", None) or getattr(event, "thesis_strategy_id", None))
            candidates.append(
                {
                    "lifecycle_id": _string_or_none(getattr(event, "lifecycle_id", None)),
                    "trade_id": _string_or_none(getattr(event, "trade_id", None)),
                    "lane_id": lane_id,
                    "strategy_id": lane_id,
                    "managed_exit_policy_id": _managed_exit_policy_from_lane(lane_id),
                    "entry_timestamp": getattr(event, "generated_at", None).isoformat()
                    if getattr(event, "generated_at", None) is not None
                    else None,
                    "entry_price": str(getattr(event, "price", "")) if getattr(event, "price", None) is not None else None,
                    "entry_order_id": _string_or_none(getattr(event, "order_id", None)),
                    "entry_perm_id": _string_or_none(getattr(event, "perm_id", None)),
                    "entry_exec_id": _string_or_none(getattr(event, "exec_id", None)),
                    "source": "TRACK_B_LIVE_TRADE_REGISTRY_ENTRY_FILL",
                }
            )
    return candidates


def _entry_fill_event_from_registry(
    *,
    position: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    terminal_records: Sequence[Any],
) -> Any | None:
    if not terminal_records:
        return None
    identity = _broker_position_identity(broker_position)
    lifecycle_id = _string_or_none(position.get("lifecycle_id"))
    trade_id = _string_or_none(position.get("trade_id"))
    lane_id = _string_or_none(position.get("lane_id") or position.get("strategy_id"))
    exec_ids = _identity_set(position.get("entry_exec_ids"), position.get("entry_exec_id"))
    perm_ids = _identity_set(position.get("entry_perm_ids"), position.get("entry_perm_id"))
    order_ids = _identity_set(position.get("entry_order_ids"), position.get("entry_order_id"))
    candidates: list[Any] = []
    for record in terminal_records:
        for event in getattr(record, "event_chain", ()) or ():
            if str(getattr(getattr(event, "event_type", ""), "value", getattr(event, "event_type", ""))) != "ENTRY_FILL_BROKER_BACKED":
                continue
            if lifecycle_id and str(getattr(event, "lifecycle_id", "") or "") == lifecycle_id:
                candidates.append(event)
                continue
            if trade_id and str(getattr(event, "trade_id", "") or "") == trade_id:
                candidates.append(event)
                continue
            if exec_ids and str(getattr(event, "exec_id", "") or "") in exec_ids:
                candidates.append(event)
                continue
            if perm_ids and str(getattr(event, "perm_id", "") or "") in perm_ids:
                candidates.append(event)
                continue
            if order_ids and str(getattr(event, "order_id", "") or "") in order_ids and _event_matches_broker_position(event, identity, lane_id):
                candidates.append(event)
                continue
            if _event_matches_broker_position(event, identity, lane_id):
                candidates.append(event)
    if not candidates:
        return None
    return max(candidates, key=lambda event: getattr(event, "generated_at", datetime.min.replace(tzinfo=UTC)))


def _event_matches_broker_position(event: Any, identity: Mapping[str, Any], lane_id: str | None) -> bool:
    con_id = identity.get("con_id")
    local_symbol = identity.get("local_symbol")
    if con_id is not None and _int_or_none(getattr(event, "con_id", None)) != con_id:
        return False
    if local_symbol and str(getattr(event, "local_symbol", "") or "").strip() != local_symbol:
        return False
    if lane_id and str(getattr(event, "lane_id", "") or "").strip() != lane_id:
        return False
    return bool(con_id or local_symbol or lane_id)


def _identity_set(*values: Any) -> set[str]:
    identities: set[str] = set()
    for value in values:
        if isinstance(value, (list, tuple, set)):
            identities.update(str(item or "").strip() for item in value if str(item or "").strip())
        elif str(value or "").strip():
            identities.add(str(value or "").strip())
    return identities


def _managed_exit_policy_from_lane(lane_id: object) -> str | None:
    text = str(lane_id or "")
    if "_active_participation_" in text:
        return "GLOBEX_ACTIVE_EVIDENCE_TIMEBOX_15M_EXIT_V1"
    return None


def _side_from_signed_quantity(value: object) -> str:
    quantity = _decimal(value) or Decimal("0")
    return "LONG" if quantity > 0 else "SHORT"


def _sweeper_classification(
    *,
    visible_positions: Sequence[Mapping[str, Any]],
    diagnostics: Sequence[Mapping[str, Any]],
) -> str:
    if any(str(row.get("classification") or "") == "MANAGED_EXIT_SERVICE_RESTART_OR_SCHEMA_REPAIR_REQUIRED" for row in diagnostics):
        return "MANAGED_EXIT_SERVICE_RESTART_OR_SCHEMA_REPAIR_REQUIRED"
    if any(str(row.get("classification") or "") == "STRAY_POSITION_REVIEW_REQUIRED" for row in visible_positions):
        return "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REVIEW_REQUIRED"
    if any(str(row.get("reason") or "") == "broker_truth_position_adopted" for row in diagnostics):
        return "MANAGED_EXIT_BROKER_TRUTH_SWEEP_ADOPTED"
    if any(str(row.get("reason") or "") == "broker_truth_contract_identity_refreshed" for row in diagnostics):
        return "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REPAIRED"
    return "MANAGED_EXIT_BROKER_TRUTH_SWEEP_OK"


def _publish_broker_truth_sweeper_diagnostic(report: Mapping[str, Any]) -> bool:
    return str(report.get("classification") or "") in {
        "MANAGED_EXIT_BROKER_TRUTH_SWEEP_ADOPTED",
        "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REPAIRED",
        "MANAGED_EXIT_BROKER_TRUTH_SWEEP_REVIEW_REQUIRED",
        "MANAGED_EXIT_SERVICE_RESTART_OR_SCHEMA_REPAIR_REQUIRED",
    }


def _managed_registry_classification(managed_positions: Sequence[Mapping[str, Any]]) -> str:
    classifications = {str(row.get("classification") or "") for row in managed_positions}
    if "STRAY_POSITION_REVIEW_REQUIRED" in classifications:
        return "STRAY_POSITION_REVIEW_REQUIRED"
    if "OPEN_MANAGED_EXIT_DUE" in classifications:
        return "OPEN_MANAGED_EXIT_DUE"
    if "OPEN_MANAGED_CLOSE_WORKING" in classifications:
        return "OPEN_MANAGED_CLOSE_WORKING"
    if "OPEN_MANAGED_MATCHED" in classifications:
        return "OPEN_MANAGED_MATCHED"
    return "NO_MANAGED_POSITIONS"


def _build_pipeline_execution_plan(
    *,
    config: TrackBManagedExitServiceConfig,
    now: datetime,
    pipeline_builder: PipelineBuilder | None = None,
) -> dict[str, Any]:
    pipeline_builder = pipeline_builder or _run_pipeline_dry_run
    diagnostics: list[dict[str, Any]] = []
    try:
        pipeline = dict(pipeline_builder(config, now))
    except Exception as exc:
        return {
            "schema_version": "track_b_managed_exit_service_pipeline_execution_plan_v1",
            "generated_at": now.isoformat(),
            "classification": MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE,
            "exit_intents": [],
            "authority_decisions": [],
            "executable_intents": [],
            "blocked_intents": [],
            "diagnostics": [{"kind": "pipeline_exception", "detail": str(exc)}],
        }

    exit_intents = [_mapping(row) for row in _list(pipeline.get("generated_exit_intents"))]
    authority_decisions = [_mapping(row) for row in _list(pipeline.get("exit_authority_decisions"))]
    intent_by_id = {str(row.get("exit_intent_id") or ""): row for row in exit_intents}
    executable_intents: list[dict[str, Any]] = []
    blocked_intents: list[dict[str, Any]] = []

    for decision in authority_decisions:
        decision_value = str(decision.get("decision") or _mapping(decision.get("authority_decision")).get("decision") or "")
        intent_id = str(decision.get("exit_intent_id") or "")
        row = {
            **intent_by_id.get(intent_id, {}),
            "exit_intent_id": intent_id,
            "authority_decision": decision,
            "decision": decision_value,
        }
        if decision_value in {"ALLOWED", "DEGRADED_ALLOWED"}:
            executable_intents.append(row)
        elif decision_value == "BLOCKED":
            blocked_intents.append(row)

    if _list(pipeline.get("pipeline_errors")):
        diagnostics.append({"kind": "pipeline_errors", "rows": _list(pipeline.get("pipeline_errors"))})
    if _list(pipeline.get("pipeline_blockers")):
        diagnostics.append({"kind": "pipeline_blockers", "rows": _list(pipeline.get("pipeline_blockers"))})
    if _mapping(pipeline.get("source_classifications")):
        diagnostics.append({"kind": "legacy_source_classifications", "rows": _mapping(pipeline.get("source_classifications"))})

    classification = str(pipeline.get("classification") or "")
    if _list(pipeline.get("pipeline_errors")):
        classification = MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE
    return {
        "schema_version": "track_b_managed_exit_service_pipeline_execution_plan_v1",
        "generated_at": now.isoformat(),
        "classification": classification,
        "exit_intents": exit_intents,
        "authority_decisions": authority_decisions,
        "executable_intents": executable_intents,
        "blocked_intents": blocked_intents,
        "diagnostics": diagnostics,
        "pipeline": pipeline,
    }


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _string_or_none(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int_or_none(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _refresh_operator_authority(config: TrackBManagedExitServiceConfig, phase: str) -> Mapping[str, Any]:
    command = [
        sys.executable,
        "-m",
        "mgc_v05l.app.track_b_operator_readiness_refresher",
        "--repo-root",
        str(config.repo_root),
        "--refresh-seconds",
        str(config.cadence_seconds),
        "--timeout-seconds",
        str(config.authority_refresh_timeout_seconds),
        "--no-heartbeat",
        "--once",
    ]
    completed = _run_command(command, config.repo_root, config.authority_refresh_timeout_seconds)
    if completed.returncode == 124:
        return {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_TIMEOUT",
            "generated_at": datetime.now(UTC).isoformat(),
            "dependency_refresh_failures": [
                {
                    "step": "operator_readiness_refresher",
                    "code": "operator_readiness_refresher_timeout",
                    "returncode": 124,
                    "stderr_tail": _tail(completed.stderr),
                }
            ],
            "command": command,
            "returncode": 124,
            "stdout_tail": _tail(completed.stdout),
            "stderr_tail": _tail(completed.stderr),
            "status_path": str(config.resolve(config.status_path)),
        }
    try:
        payload = json.loads(completed.stdout or "{}")
    except json.JSONDecodeError:
        return {
            "phase": phase,
            "succeeded": False,
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_INVALID_JSON",
            "generated_at": datetime.now(UTC).isoformat(),
            "dependency_refresh_failures": [
                {
                    "step": "operator_readiness_refresher",
                    "code": "operator_readiness_refresher_invalid_json",
                    "returncode": completed.returncode,
                    "stderr_tail": _tail(completed.stderr),
                }
            ],
            "command": command,
            "returncode": completed.returncode,
            "stdout_tail": _tail(completed.stdout),
            "stderr_tail": _tail(completed.stderr),
            "status_path": str(config.resolve(config.status_path)),
        }
    if not isinstance(payload, Mapping):
        payload = {}
    return {
        "phase": phase,
        "succeeded": payload.get("last_success") is True,
        "classification": payload.get("classification"),
        "generated_at": payload.get("generated_at"),
        "dependency_refresh_failures": payload.get("dependency_refresh_failures") or [],
        "command": command,
        "returncode": completed.returncode,
        "status_path": str(config.resolve(config.status_path)),
    }


def _write_heartbeat(
    *,
    config: TrackBManagedExitServiceConfig,
    payload: Mapping[str, Any],
    service_running: bool,
) -> Path | None:
    if config.heartbeat_path is None:
        return None
    heartbeat = {
        "schema_version": "track_b_managed_exit_service_heartbeat_v1",
        "generated_at": datetime.now(UTC).isoformat(),
        "service_running": service_running,
        "pid": os.getpid(),
        "classification": payload.get("classification"),
        "cadence_seconds": config.cadence_seconds,
        "status_path": str(config.resolve(config.status_path)),
        "close_only": True,
        "entry_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    return write_json_atomic(config.resolve(config.heartbeat_path), heartbeat)


def _max_int(rows: Sequence[Mapping[str, Any]], key: str) -> int:
    values: list[int] = []
    for row in rows:
        try:
            values.append(int(row.get(key) or 0))
        except (TypeError, ValueError):
            values.append(0)
    return max(values or [0])


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--status-path", type=Path, default=DEFAULT_STATUS_PATH)
    parser.add_argument("--heartbeat-path", type=Path, default=DEFAULT_HEARTBEAT_PATH)
    parser.add_argument("--no-heartbeat", action="store_true")
    parser.add_argument("--cadence-seconds", type=float, default=DEFAULT_CADENCE_SECONDS)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--operator-authorized-managed-exit", action="store_true")
    parser.add_argument("--max-cycles-per-tick", type=int, default=4)
    parser.add_argument("--no-authority-refresh-before-apply", action="store_true")
    parser.add_argument("--no-authority-refresh-after-attempt", action="store_true")
    parser.add_argument("--authority-refresh-timeout-seconds", type=float, default=120.0)
    parser.add_argument("--actuator-timeout-seconds", type=float, default=90.0)
    parser.add_argument("--service-label")
    parser.add_argument("--service", action="store_true")
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--status", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def _config_from_args(args: argparse.Namespace) -> TrackBManagedExitServiceConfig:
    return TrackBManagedExitServiceConfig(
        repo_root=args.repo_root.expanduser().resolve(),
        status_path=args.status_path,
        heartbeat_path=None if args.no_heartbeat else args.heartbeat_path,
        cadence_seconds=args.cadence_seconds,
        apply=bool(args.apply),
        operator_authorized_managed_exit=bool(args.operator_authorized_managed_exit),
        max_cycles_per_tick=args.max_cycles_per_tick,
        authority_refresh_before_apply=not bool(args.no_authority_refresh_before_apply),
        authority_refresh_after_attempt=not bool(args.no_authority_refresh_after_attempt),
        authority_refresh_timeout_seconds=args.authority_refresh_timeout_seconds,
        actuator_timeout_seconds=args.actuator_timeout_seconds,
        service_label=args.service_label,
    )


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = _config_from_args(args)
    if args.status:
        payload = read_track_b_managed_exit_service_status(repo_root=config.repo_root, status_path=config.status_path)
        if args.json:
            print(json.dumps(payload, indent=2, sort_keys=True))
        else:
            print(f"classification={payload.get('classification')}")
            print(f"generated_at={payload.get('generated_at')}")
        return 0
    if args.service:
        return run_track_b_managed_exit_service(config=config)
    payload = run_track_b_managed_exit_service_once(config=config)
    if args.json:
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(f"classification={payload.get('classification')}")
        print(f"submitted_count={payload.get('submitted_count')}")
    return 0 if str(payload.get("classification") or "") != MANAGED_EXIT_SERVICE_REFRESH_FAILED else 2


if __name__ == "__main__":
    raise SystemExit(main())
