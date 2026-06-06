"""Read-only Track B operator readiness artifact refresher."""

from __future__ import annotations

import argparse
import json
import os
import signal
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_STATUS_PATH = (
    REPO_ROOT
    / "outputs"
    / "reports"
    / "track_b_operator_readiness_refresher"
    / "latest_track_b_operator_readiness_refresher_status.json"
)
DEFAULT_HEARTBEAT_PATH = REPO_ROOT / "var" / "track_b_operator_readiness_refresh_heartbeat.json"
DEFAULT_CANONICAL_READINESS_PATH = (
    REPO_ROOT / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
)
DEFAULT_CANONICAL_READINESS_SUMMARY_PATH = (
    REPO_ROOT / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness_summary.json"
)
DEFAULT_SERVICE_PID_PATH = REPO_ROOT / "var" / "track_b_operator_readiness_refresh_service.pid"
DEFAULT_CHILD_PID_PATH = REPO_ROOT / "var" / "track_b_operator_readiness_refresh_child.pid"
DEFAULT_SUPERVISOR_STATUS_PATH = REPO_ROOT / "var" / "track_b_operator_readiness_refresh_supervisor.json"
DEFAULT_REFRESH_SECONDS = 15.0
DEFAULT_PREFLIGHT_MODE = "monday-live"


@dataclass(frozen=True)
class RefreshCommandResult:
    name: str
    command: list[str]
    returncode: int
    stdout_tail: str
    stderr_tail: str
    duration_seconds: float


@dataclass(frozen=True)
class RefreshConfig:
    repo_root: Path = REPO_ROOT
    status_path: Path = DEFAULT_STATUS_PATH
    heartbeat_path: Path | None = None
    canonical_readiness_path: Path = DEFAULT_CANONICAL_READINESS_PATH
    canonical_readiness_summary_path: Path = DEFAULT_CANONICAL_READINESS_SUMMARY_PATH
    refresh_seconds: float = DEFAULT_REFRESH_SECONDS
    preflight_mode: str = DEFAULT_PREFLIGHT_MODE
    timeout_seconds: float = 120.0


Runner = Callable[[Sequence[str], Path, float], subprocess.CompletedProcess[str]]


def refresh_once(*, config: RefreshConfig, runner: Runner | None = None) -> dict[str, Any]:
    repo_root = Path(config.repo_root)
    runner = runner or _run_command
    started = _utc_now()
    commands = _refresh_commands(
        repo_root=repo_root,
        preflight_mode=config.preflight_mode,
        canonical_readiness_path=config.canonical_readiness_path,
        canonical_readiness_summary_path=config.canonical_readiness_summary_path,
    )
    results: list[RefreshCommandResult] = []
    for name, command in commands:
        command_started = time.monotonic()
        completed = _run_safely(runner, command, repo_root, config.timeout_seconds)
        results.append(
            RefreshCommandResult(
                name=name,
                command=list(command),
                returncode=completed.returncode,
                stdout_tail=_tail(completed.stdout),
                stderr_tail=_tail(completed.stderr),
                duration_seconds=round(time.monotonic() - command_started, 3),
            )
        )
    succeeded = all(_command_result_succeeded(result) for result in results)
    payload = _status_payload(
        config=config,
        started_at=started,
        command_results=results,
        succeeded=succeeded,
    )
    _write_json_atomic(config.status_path, payload)
    _write_heartbeat(config=config, payload=payload, refresh_running=False)
    return payload


def run_service(*, config: RefreshConfig) -> int:
    stopping = False

    def _handle_stop(signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True
        payload = {
            "schema_version": "track_b_operator_readiness_refresher_status_v1",
            "generated_at": _utc_now().isoformat(),
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_STOPPING",
            "reason": f"signal={signum}",
            "repo_root": str(config.repo_root),
            "refresh_seconds": config.refresh_seconds,
            "preflight_mode": config.preflight_mode,
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }
        _write_json_atomic(config.status_path, payload)
        _write_heartbeat(config=config, payload=payload, refresh_running=False)

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    while not stopping:
        _write_heartbeat(
            config=config,
            payload={
                "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_RUNNING",
                "last_success": None,
                "last_success_at": None,
            },
            refresh_running=True,
        )
        refresh_once(config=config)
        deadline = time.monotonic() + max(config.refresh_seconds, 1.0)
        while not stopping and time.monotonic() < deadline:
            time.sleep(min(1.0, deadline - time.monotonic()))
    return 0


def run_supervisor(
    *,
    config: RefreshConfig,
    service_pid_path: Path = DEFAULT_SERVICE_PID_PATH,
    child_pid_path: Path = DEFAULT_CHILD_PID_PATH,
    supervisor_status_path: Path = DEFAULT_SUPERVISOR_STATUS_PATH,
) -> int:
    stopping = False
    child: subprocess.Popen[str] | None = None
    restart_count = 0

    def _child_command() -> list[str]:
        command = [
            sys.executable,
            "-m",
            "mgc_v05l.app.track_b_operator_readiness_refresher",
            "--service",
            "--repo-root",
            str(config.repo_root),
            "--status-path",
            str(config.status_path),
            "--canonical-readiness-path",
            str(config.canonical_readiness_path),
            "--canonical-readiness-summary-path",
            str(config.canonical_readiness_summary_path),
            "--refresh-seconds",
            str(config.refresh_seconds),
            "--preflight-mode",
            config.preflight_mode,
            "--timeout-seconds",
            str(config.timeout_seconds),
        ]
        if config.heartbeat_path is None:
            command.append("--no-heartbeat")
        else:
            command.extend(["--heartbeat-path", str(config.heartbeat_path)])
        return command

    def _write_supervisor_status(classification: str, reason: str | None = None) -> None:
        payload = {
            "schema_version": "track_b_operator_readiness_refresh_supervisor_v1",
            "generated_at": _utc_now().isoformat(),
            "classification": classification,
            "reason": reason,
            "repo_root": str(config.repo_root),
            "pid": os.getpid(),
            "child_pid": None if child is None else child.pid,
            "child_running": bool(child is not None and child.poll() is None),
            "restart_count": restart_count,
            "refresh_seconds": config.refresh_seconds,
            "preflight_mode": config.preflight_mode,
            "status_path": str(config.status_path),
            "heartbeat_path": None if config.heartbeat_path is None else str(config.heartbeat_path),
            "canonical_readiness_path": str(config.canonical_readiness_path),
            "canonical_readiness_summary_path": str(config.canonical_readiness_summary_path),
            "service_pid_path": str(service_pid_path),
            "child_pid_path": str(child_pid_path),
            "command": _child_command(),
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }
        _write_json_atomic(supervisor_status_path, payload)

    def _child_env() -> dict[str, str]:
        env = dict(os.environ)
        existing_pythonpath = env.get("PYTHONPATH", "")
        env["PYTHONPATH"] = f"{config.repo_root / 'src'}{':' + existing_pythonpath if existing_pythonpath else ''}"
        return env

    def _start_child(reason: str) -> None:
        nonlocal child, restart_count
        if child is not None and child.poll() is None:
            return
        restart_count += 1
        with open(os.devnull, "rb") as devnull:
            child = subprocess.Popen(
                _child_command(),
                cwd=config.repo_root,
                env=_child_env(),
                stdin=devnull,
                text=True,
                start_new_session=True,
                close_fds=True,
            )
        child_pid_path.parent.mkdir(parents=True, exist_ok=True)
        child_pid_path.write_text(f"{child.pid}\n", encoding="utf-8")
        _write_supervisor_status("TRACK_B_OPERATOR_READINESS_REFRESH_SUPERVISOR_RUNNING", reason)

    def _stop_child() -> None:
        nonlocal child
        if child is not None and child.poll() is None:
            child.terminate()
            try:
                child.wait(timeout=10)
            except subprocess.TimeoutExpired:
                child.kill()
                child.wait(timeout=10)
        child = None
        try:
            child_pid_path.unlink()
        except FileNotFoundError:
            pass

    def _handle_stop(signum: int, _frame: Any) -> None:
        nonlocal stopping
        stopping = True
        _write_supervisor_status("TRACK_B_OPERATOR_READINESS_REFRESH_SUPERVISOR_STOPPING", f"signal={signum}")
        _stop_child()

    signal.signal(signal.SIGTERM, _handle_stop)
    signal.signal(signal.SIGINT, _handle_stop)
    service_pid_path.parent.mkdir(parents=True, exist_ok=True)
    service_pid_path.write_text(f"{os.getpid()}\n", encoding="utf-8")
    try:
        _start_child("initial_start")
        while not stopping:
            time.sleep(5.0)
            if child is None or child.poll() is not None:
                code = None if child is None else child.poll()
                _start_child(f"child_exited_returncode={code}")
            else:
                _write_supervisor_status("TRACK_B_OPERATOR_READINESS_REFRESH_SUPERVISOR_RUNNING")
    finally:
        _stop_child()
        try:
            service_pid_path.unlink()
        except FileNotFoundError:
            pass
        _write_supervisor_status("TRACK_B_OPERATOR_READINESS_REFRESH_SUPERVISOR_STOPPED")
    return 0


def read_status(*, status_path: Path = DEFAULT_STATUS_PATH) -> dict[str, Any]:
    try:
        payload = json.loads(Path(status_path).read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {
            "schema_version": "track_b_operator_readiness_refresher_status_v1",
            "generated_at": _utc_now().isoformat(),
            "classification": "TRACK_B_OPERATOR_READINESS_REFRESH_STATUS_MISSING",
            "path": str(status_path),
            "fresh": False,
            "refresh_running": False,
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }
    return _status_with_freshness(payload)


def _refresh_commands(
    *,
    repo_root: Path,
    preflight_mode: str,
    canonical_readiness_path: Path | None = None,
    canonical_readiness_summary_path: Path | None = None,
) -> list[tuple[str, list[str]]]:
    python_bin = str(repo_root / ".venv" / "bin" / "python")
    canonical_path = canonical_readiness_path or (
        repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
    )
    return [
        (
            "phase1_runtime_data_readiness",
            [
                python_bin,
                "-m",
                "mgc_v05l.execution_core.phase1_runtime_data_readiness",
                "--repo-root",
                str(repo_root),
            ],
        ),
        (
            "phase1_ticker_readiness_matrix",
            [
                python_bin,
                "-m",
                "mgc_v05l.app.phase1_ticker_readiness_matrix",
                "--repo-root",
                str(repo_root),
            ],
        ),
        (
            "broker_truth_broker_truth_lease_bsa",
            [
                python_bin,
                "-m",
                "mgc_v05l.app.ibkr_broker_truth_refresher",
                "--once",
                "--read-only",
                "--mode",
                "PAPER",
                "--account-id",
                "DUM882026",
            ],
        ),
        (
            "track_b_paper_broker_reconciliation",
            [
                python_bin,
                "-m",
                "mgc_v05l.execution_core.track_b_paper_broker_reconciliation",
                "--repo-root",
                str(repo_root),
                "--account",
                "DUM882026",
                "--symbols",
                "MNQ,MES",
            ],
        ),
        (
            "open_order_truth",
            [
                python_bin,
                "-m",
                "mgc_v05l.app.track_b_open_order_truth",
                "--repo-root",
                str(repo_root),
                "--once",
            ],
        ),
        (
            "managed_position_registry",
            [
                python_bin,
                "-m",
                "mgc_v05l.app.track_b_managed_position_registry",
                "--repo-root",
                str(repo_root),
                "--once",
            ],
        ),
        (
            "managed_order_registry",
            [
                python_bin,
                "-m",
                "mgc_v05l.app.track_b_managed_order_registry",
                "--repo-root",
                str(repo_root),
                "--once",
            ],
        ),
        (
            "shared_truth",
            [
                python_bin,
                "-m",
                "mgc_v05l.execution_core.track_b_shared_truth_refresh_cli",
                "--repo-root",
                str(repo_root),
                "--account",
                "DUM882026",
                "--symbols",
                "MNQ,MES",
                "--no-broker-lease-history",
                "--json",
            ],
        ),
        (
            "canonical_readiness",
            [
                python_bin,
                "-m",
                "mgc_v05l.app.track_b_canonical_readiness",
                "--repo-root",
                str(repo_root),
                "--expected-root",
                str(repo_root),
                "--output-path",
                str(canonical_path),
                "--summary-output-path",
                str(
                    canonical_readiness_summary_path
                    or canonical_path.with_name("latest_canonical_readiness_summary.json")
                ),
                "--json",
            ],
        ),
        (
            "agent_health",
            [
                python_bin,
                "-m",
                "mgc_v05l.execution_core.track_b_agent_health",
                "--repo-root",
                str(repo_root),
                "--json",
            ],
        ),
        (
            "control_plane_snapshot",
            [
                python_bin,
                "-m",
                "mgc_v05l.execution_core.track_b_control_plane_snapshot",
                "--repo-root",
                str(repo_root),
                "--output-path",
                str(
                    repo_root
                    / "outputs"
                    / "track_b_execution_core"
                    / "control_plane"
                    / "latest_control_plane_snapshot.json"
                ),
                "--no-dashboard-projection",
                "--no-broker-lease-history",
                "--json",
            ],
        ),
    ]


def _command_result_succeeded(result: RefreshCommandResult) -> bool:
    if result.returncode == 0:
        return True
    if result.name == "canonical_readiness" and result.returncode in {1, 2}:
        return '"classification"' in result.stdout_tail
    if result.name in {"agent_health", "control_plane_snapshot"} and result.returncode == 2:
        return bool(result.stdout_tail)
    return False


def _run_safely(
    runner: Runner,
    command: Sequence[str],
    repo_root: Path,
    timeout_seconds: float,
) -> subprocess.CompletedProcess[str]:
    try:
        return runner(command, repo_root, timeout_seconds)
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else str(exc.stdout or "")
        stderr = exc.stderr if isinstance(exc.stderr, str) else str(exc.stderr or "")
        return subprocess.CompletedProcess(
            list(command),
            124,
            stdout=stdout,
            stderr=f"refresh command timed out after {timeout_seconds}s: {stderr}".strip(),
        )
    except Exception as exc:  # defensive: the service must report failure instead of dying silently.
        return subprocess.CompletedProcess(list(command), 1, stdout="", stderr=f"refresh command exception: {exc}")


def _run_command(command: Sequence[str], repo_root: Path, timeout_seconds: float) -> subprocess.CompletedProcess[str]:
    env = dict(os.environ)
    existing_pythonpath = env.get("PYTHONPATH", "")
    env["PYTHONPATH"] = f"{repo_root / 'src'}{':' + existing_pythonpath if existing_pythonpath else ''}"
    return subprocess.run(
        list(command),
        cwd=repo_root,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout_seconds,
        check=False,
    )


def _status_payload(
    *,
    config: RefreshConfig,
    started_at: datetime,
    command_results: Sequence[RefreshCommandResult],
    succeeded: bool,
) -> dict[str, Any]:
    now = _utc_now()
    authority_generation_id = f"track-b-operator-authority-refresh-{now.strftime('%Y%m%dT%H%M%S%fZ')}"
    broker_authority_ownership = _read_json(
        config.repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_authority_ownership.json"
    )
    return {
        "schema_version": "track_b_operator_readiness_refresher_status_v1",
        "generated_at": now.isoformat(),
        "authority_generation_id": authority_generation_id,
        "last_refresh_started_at": started_at.isoformat(),
        "last_refresh_finished_at": now.isoformat(),
        "last_success": succeeded,
        "last_success_at": now.isoformat() if succeeded else None,
        "last_failure": not succeeded,
        "last_failure_at": None if succeeded else now.isoformat(),
        "classification": (
            "TRACK_B_OPERATOR_READINESS_REFRESH_READY"
            if succeeded
            else "TRACK_B_OPERATOR_READINESS_REFRESH_FAILED"
        ),
        "repo_root": str(config.repo_root),
        "refresh_seconds": config.refresh_seconds,
        "preflight_mode": config.preflight_mode,
        "authority_refresh_orchestration": "TRACK_B_ACTIVE_RUNTIME_DEPENDENCY_CHAIN_V1",
        "authority_refresh_cadence_seconds": config.refresh_seconds,
        "broker_authority_ownership": _broker_authority_ownership_summary(broker_authority_ownership),
        "dependency_refresh_steps": [
            {
                "step": result.name,
                "authority_generation_id": authority_generation_id,
                "returncode": result.returncode,
                "succeeded": _command_result_succeeded(result),
                "duration_seconds": result.duration_seconds,
            }
            for result in command_results
        ],
        "dependency_refresh_failures": [
            {
                "step": result.name,
                "code": f"{result.name}_refresh_failed",
                "authority_generation_id": authority_generation_id,
                "returncode": result.returncode,
                "stderr_tail": result.stderr_tail,
            }
            for result in command_results
            if not _command_result_succeeded(result)
        ],
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "refreshed_artifacts": {
            "phase1_runtime_data_readiness": str(
                config.repo_root
                / "outputs"
                / "reports"
                / "phase1_runtime_data_readiness"
                / "latest_phase1_runtime_data_readiness.json"
            ),
            "phase1_ticker_readiness_matrix": str(
                config.repo_root
                / "outputs"
                / "reports"
                / "phase1_ticker_readiness_matrix"
                / "latest_phase1_ticker_readiness_matrix.json"
            ),
            "broker_truth_status": str(
                config.repo_root
                / "outputs"
                / "reports"
                / "ibkr_read_only_verification"
                / "ibkr_broker_truth_refresh_status.json"
            ),
            "broker_truth_lease": str(
                config.repo_root / "outputs" / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
            ),
            "broker_session_authority": str(
                config.repo_root
                / "outputs"
                / "operator_dashboard"
                / "runtime"
                / "latest_broker_session_authority.json"
            ),
            "broker_authority_ownership": str(
                config.repo_root
                / "outputs"
                / "operator_dashboard"
                / "runtime"
                / "latest_broker_authority_ownership.json"
            ),
            "track_b_paper_broker_reconciliation": str(
                config.repo_root
                / "outputs"
                / "reports"
                / "track_b_paper_broker_reconciliation"
                / "latest_track_b_paper_broker_reconciliation.json"
            ),
            "open_order_truth": str(
                config.repo_root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
            ),
            "managed_position_registry": str(
                config.repo_root
                / "outputs"
                / "track_b_execution_core"
                / "managed_positions"
                / "latest_managed_positions.json"
            ),
            "managed_order_registry": str(
                config.repo_root / "outputs" / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
            ),
            "shared_truth": str(
                config.repo_root
                / "outputs"
                / "track_b_execution_core"
                / "shared_truth"
                / "latest_track_b_shared_truth_refresh.json"
            ),
            "canonical_readiness": str(config.canonical_readiness_path),
            "canonical_readiness_summary": str(config.canonical_readiness_summary_path),
            "agent_health": str(
                config.repo_root / "outputs" / "track_b_execution_core" / "agent_health" / "latest_agent_health.json"
            ),
            "control_plane_snapshot": str(
                config.repo_root
                / "outputs"
                / "track_b_execution_core"
                / "control_plane"
                / "latest_control_plane_snapshot.json"
            ),
        },
        "commands": [
            {
                "name": result.name,
                "authority_generation_id": authority_generation_id,
                "command": result.command,
                "returncode": result.returncode,
                "succeeded": _command_result_succeeded(result),
                "duration_seconds": result.duration_seconds,
                "stdout_tail": result.stdout_tail,
                "stderr_tail": result.stderr_tail,
            }
            for result in command_results
        ],
    }


def _broker_authority_ownership_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "available": False,
            "classification": "BROKER_AUTHORITY_OWNERSHIP_STATUS_MISSING",
            "broker_authority_publisher_healthy": False,
            "lease_bsa_generation_aligned": False,
            "duplicate_hot_writer_detected": False,
            "non_owner_hot_write_attempt_count": 0,
            "running_writer_needs_reload": True,
            "next_safe_action": "INVESTIGATE_DUPLICATE_WRITER",
        }
    return {
        "available": True,
        "classification": payload.get("classification"),
        "authority_writer": payload.get("authority_writer"),
        "authority_generation_id": payload.get("authority_generation_id"),
        "writer_pid": payload.get("writer_pid"),
        "service_label": payload.get("service_label"),
        "source_commit": payload.get("source_commit"),
        "expected_min_commit": payload.get("expected_min_commit"),
        "broker_authority_publisher_healthy": payload.get("broker_authority_publisher_healthy") is True,
        "lease_bsa_generation_aligned": payload.get("lease_bsa_generation_aligned") is True,
        "duplicate_hot_writer_detected": payload.get("duplicate_hot_writer_detected") is True,
        "non_owner_hot_write_attempt_count": int(payload.get("non_owner_hot_write_attempt_count") or 0),
        "running_writer_needs_reload": payload.get("running_writer_needs_reload") is True,
        "next_safe_action": payload.get("next_safe_action"),
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _status_with_freshness(payload: dict[str, Any]) -> dict[str, Any]:
    generated_at = payload.get("generated_at")
    age_seconds = _age_seconds(generated_at)
    try:
        refresh_seconds = float(payload.get("refresh_seconds") or DEFAULT_REFRESH_SECONDS)
    except (TypeError, ValueError):
        refresh_seconds = DEFAULT_REFRESH_SECONDS
    threshold = max(refresh_seconds * 2.5, 180.0)
    fresh = bool(age_seconds is not None and age_seconds <= threshold)
    source_classification = str(payload.get("classification") or "TRACK_B_OPERATOR_READINESS_REFRESH_UNKNOWN")
    enriched = dict(payload)
    enriched["source_classification"] = source_classification
    enriched["age_seconds"] = age_seconds
    enriched["freshness_threshold_seconds"] = threshold
    enriched["fresh"] = fresh
    if not fresh and source_classification not in {
        "TRACK_B_OPERATOR_READINESS_REFRESH_STATUS_MISSING",
        "TRACK_B_OPERATOR_READINESS_REFRESH_RUNNING",
    }:
        enriched["classification"] = "TRACK_B_OPERATOR_READINESS_REFRESH_STALE"
    return enriched


def _write_heartbeat(*, config: RefreshConfig, payload: dict[str, Any], refresh_running: bool) -> None:
    if config.heartbeat_path is None:
        return
    heartbeat = _status_with_freshness(
        {
            "schema_version": "track_b_operator_readiness_refresher_heartbeat_v1",
            "generated_at": _utc_now().isoformat(),
            "authority_generation_id": payload.get("authority_generation_id"),
            "classification": payload.get("classification"),
            "repo_root": str(config.repo_root),
            "status_path": str(config.status_path),
            "refresh_seconds": config.refresh_seconds,
            "authority_refresh_cadence_seconds": config.refresh_seconds,
            "dependency_refresh_steps": payload.get("dependency_refresh_steps", []),
            "dependency_refresh_failures": payload.get("dependency_refresh_failures", []),
            "refreshed_artifacts": payload.get("refreshed_artifacts", {}),
            "preflight_mode": config.preflight_mode,
            "refresh_running": refresh_running,
            "last_success": payload.get("last_success"),
            "last_success_at": payload.get("last_success_at"),
            "last_failure": payload.get("last_failure"),
            "last_failure_at": payload.get("last_failure_at"),
            "submit_authority": False,
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        }
    )
    _write_json_atomic(config.heartbeat_path, heartbeat)


def _age_seconds(value: Any) -> float | None:
    if not value:
        return None
    try:
        timestamp = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=timezone.utc)
    return max((_utc_now() - timestamp.astimezone(timezone.utc)).total_seconds(), 0.0)


def _tail(value: str, *, max_chars: int = 2000) -> str:
    text = str(value or "").strip()
    if len(text) <= max_chars:
        return text
    return text[-max_chars:]


def _write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _utc_now() -> datetime:
    return datetime.now(timezone.utc)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh read-only Track B operator readiness artifacts.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--status-path", default=str(DEFAULT_STATUS_PATH))
    parser.add_argument("--heartbeat-path", default=str(DEFAULT_HEARTBEAT_PATH))
    parser.add_argument("--canonical-readiness-path", default=str(DEFAULT_CANONICAL_READINESS_PATH))
    parser.add_argument(
        "--canonical-readiness-summary-path",
        default=str(DEFAULT_CANONICAL_READINESS_SUMMARY_PATH),
    )
    parser.add_argument("--service-pid-path", default=str(DEFAULT_SERVICE_PID_PATH))
    parser.add_argument("--child-pid-path", default=str(DEFAULT_CHILD_PID_PATH))
    parser.add_argument("--supervisor-status-path", default=str(DEFAULT_SUPERVISOR_STATUS_PATH))
    parser.add_argument("--no-heartbeat", action="store_true")
    parser.add_argument("--refresh-seconds", type=float, default=float(os.environ.get("TRACK_B_OPERATOR_READINESS_REFRESH_SECONDS", DEFAULT_REFRESH_SECONDS)))
    parser.add_argument("--preflight-mode", choices=("monday-live", "weekend-static"), default=DEFAULT_PREFLIGHT_MODE)
    parser.add_argument("--timeout-seconds", type=float, default=120.0)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--once", action="store_true", help="Run one refresh pass and exit.")
    mode.add_argument("--service", action="store_true", help="Run refreshes until stopped.")
    mode.add_argument("--supervisor", action="store_true", help="Supervise the refresher service and restart it if it exits.")
    mode.add_argument("--status", action="store_true", help="Print latest refresher status.")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = RefreshConfig(
        repo_root=Path(args.repo_root),
        status_path=Path(args.status_path),
        heartbeat_path=None if args.no_heartbeat else Path(args.heartbeat_path),
        canonical_readiness_path=Path(args.canonical_readiness_path),
        canonical_readiness_summary_path=Path(args.canonical_readiness_summary_path),
        refresh_seconds=args.refresh_seconds,
        preflight_mode=args.preflight_mode,
        timeout_seconds=args.timeout_seconds,
    )
    if args.status:
        print(json.dumps(read_status(status_path=config.status_path), indent=2, sort_keys=True))
        return 0
    if args.service:
        return run_service(config=config)
    if args.supervisor:
        return run_supervisor(
            config=config,
            service_pid_path=Path(args.service_pid_path),
            child_pid_path=Path(args.child_pid_path),
            supervisor_status_path=Path(args.supervisor_status_path),
        )
    payload = refresh_once(config=config)
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0 if payload.get("last_success") is True else 1


if __name__ == "__main__":
    raise SystemExit(main())
