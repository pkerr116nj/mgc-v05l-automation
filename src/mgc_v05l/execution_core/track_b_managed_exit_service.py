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
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.models import require_aware_datetime, to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
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


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STATUS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "managed_exit_service"
    / "latest_managed_exit_service_status.json"
)
DEFAULT_HEARTBEAT_PATH = Path("var") / "track_b_managed_exit_service_heartbeat.json"
DEFAULT_CADENCE_SECONDS = 45.0

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
SleepFunc = Callable[[float], None]


def run_track_b_managed_exit_service_once(
    *,
    config: TrackBManagedExitServiceConfig,
    now: datetime | None = None,
    actuator_runner: ActuatorRunner | None = None,
    authority_refresher: AuthorityRefresher | None = None,
    command_runner: CommandRunner | None = None,
    pipeline_builder: PipelineBuilder | None = None,
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
    authority_refreshes: list[dict[str, Any]] = []
    actuator_reports: list[dict[str, Any]] = []
    phase_timings: list[dict[str, Any]] = [
        {
            "phase": "pipeline_candidate_discovery",
            "duration_seconds": 0.0,
            "classification": "DEFERRED_UNTIL_AFTER_REFRESH",
        }
    ]

    if write:
        write_track_b_managed_exit_service_status(
            config=config,
            payload=_cycle_started_payload(config=config, now=actual_now),
        )

    if config.authority_refresh_before_apply:
        phase_started = time.monotonic()
        pre_refresh = _run_authority_refresh(authority_refresher, config, "before_actuator")
        phase_timings.append(_phase_timing("authority_refresh", phase_started))
        authority_refreshes.append(pre_refresh)

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
        payload = _service_payload(
            config=config,
            now=actual_now,
            classification=MANAGED_EXIT_SERVICE_BLOCKED if blocked_intents else MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS,
            authority_refreshes=authority_refreshes,
            actuator_reports=actuator_reports,
            phase_timings=phase_timings,
            execution_plan=execution_plan,
        )
        if write:
            write_track_b_managed_exit_service_status(config=config, payload=payload)
        return payload

    max_cycles = min(max(int(config.max_cycles_per_tick or 0), 1), len(executable_intents))
    for cycle_index in range(max_cycles):
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
            submitted > 0 or actuator_report.get("submit_attempted") is True
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
    return write_json_atomic(config.resolve(config.status_path), to_jsonable(dict(payload)))


def _service_payload(
    *,
    config: TrackBManagedExitServiceConfig,
    now: datetime,
    classification: str,
    authority_refreshes: Sequence[Mapping[str, Any]],
    actuator_reports: Sequence[Mapping[str, Any]],
    phase_timings: Sequence[Mapping[str, Any]],
    execution_plan: Mapping[str, Any] | None = None,
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
        "latest_actuator_classification": actuator_reports[-1].get("classification") if actuator_reports else None,
        "exit_due_count": _max_int(actuator_reports, "exit_due_count"),
        "eligible_count": _max_int(actuator_reports, "eligible_count"),
        "attempted_count": len(attempted),
        "submitted_count": sum(int(report.get("submitted_count") or 0) for report in actuator_reports),
        "submit_attempted": any(report.get("submit_attempted") is True for report in actuator_reports),
        "broker_state_mutated": any(report.get("broker_state_mutated") is True for report in actuator_reports),
        "attempted_closes": attempted,
        "required_next_action": _next_action(final_classification),
        "status_path": str(config.resolve(config.status_path)),
    }


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


def _final_cycle_classification(*, classification: str, actuator_reports: Sequence[Mapping[str, Any]]) -> str:
    if classification == MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE:
        return MANAGED_EXIT_SERVICE_PIPELINE_UNAVAILABLE
    if classification == MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS:
        return MANAGED_EXIT_SERVICE_NO_ELIGIBLE_EXITS
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
    if submitted > 0 and classification in {MANAGED_EXIT_ACTUATOR_APPLIED_OR_PENDING, MANAGED_EXIT_ACTUATOR_PARTIAL}:
        return False
    return True


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
