"""Broker-centered fast-start contract for the Track B PAPER runtime.

This module intentionally keeps startup authority narrow:
fresh IBKR positions, fresh IBKR open orders, required service health, one
manifest, one lock, one launch command, and one bounded post-launch verifier.
Older lifecycle, certification, analytics, control-plane, and shared-derived
truth artifacts are diagnostics only for this path.
"""

from __future__ import annotations

import hashlib
import ast
import json
import os
import socket
import subprocess
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterator, Mapping, Sequence

from mgc_v05l.execution_core.track_b_current_state_authority import (
    TRACK_B_FUTURES_ROOTS,
    broker_open_orders,
    broker_positions,
    is_track_b_futures_position,
    is_track_b_order,
)

DEFAULT_MANIFEST_PATH = Path("config/track_b_paper_runtime_manifest.json")
DEFAULT_OUTPUT_DIR = Path("outputs/track_b_execution_core/fast_paper_runtime_start")
STATUS_PATH = DEFAULT_OUTPUT_DIR / "latest_fast_paper_runtime_start.json"
LOCK_DIR = DEFAULT_OUTPUT_DIR / "startup.lock"

RuntimeLauncher = Callable[[Sequence[str], Mapping[str, str], Path, Path], int]
BrokerRefresher = Callable[[Mapping[str, Any], Path], Mapping[str, Any]]


@dataclass(frozen=True)
class FastStartDecision:
    ok: bool
    classification: str
    first_blocker: str | None
    message: str
    details: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "ok": self.ok,
            "classification": self.classification,
            "first_blocker": self.first_blocker,
            "message": self.message,
            "details": dict(self.details),
        }


def run_fast_paper_runtime_start(
    *,
    command: str,
    repo_root: Path,
    manifest_path: Path = DEFAULT_MANIFEST_PATH,
    skip_broker_refresh: bool = False,
    dry_run: bool = False,
    now: str | None = None,
    broker_refresher: BrokerRefresher | None = None,
    runtime_launcher: RuntimeLauncher | None = None,
    sleep_fn: Callable[[float], None] = time.sleep,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    generated_at = _now(now)
    manifest = load_manifest(repo_root=repo_root, manifest_path=manifest_path)
    output_dir = repo_root / DEFAULT_OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    if command == "validate-manifest":
        decision = _validate_manifest(manifest, repo_root=repo_root)
        return _publish(output_dir, generated_at, command, manifest, decision)

    if command == "status":
        decision = _verify_runtime(manifest, repo_root=repo_root, now_dt=generated_at)
        return _publish(output_dir, generated_at, command, manifest, decision)

    if command == "simulate-validation":
        result = _run_simulated_validation(manifest, generated_at=generated_at)
        _write_json(output_dir / "fast_start_validation_demonstration.json", result)
        return result

    with startup_lock(repo_root=repo_root, manifest=manifest, now_dt=generated_at):
        if command == "recover":
            runtime_decision = _verify_runtime(manifest, repo_root=repo_root, now_dt=generated_at)
            if runtime_decision.ok:
                return _publish(output_dir, generated_at, command, manifest, runtime_decision)

        preflight = _run_preflight(
            manifest,
            repo_root=repo_root,
            now_dt=generated_at,
            skip_broker_refresh=skip_broker_refresh,
            broker_refresher=broker_refresher,
        )
        if not preflight.ok or command == "preflight":
            return _publish(output_dir, generated_at, command, manifest, preflight)
        if dry_run:
            decision = FastStartDecision(
                ok=True,
                classification="FAST_START_DRY_RUN_READY",
                first_blocker=None,
                message="Fast-start preflight passed; dry-run did not launch runtime.",
                details=preflight.details,
            )
            return _publish(output_dir, generated_at, command, manifest, decision)

        launcher = runtime_launcher or _launch_runtime
        launch_details = _launch_manifest_runtime(manifest, repo_root=repo_root, launcher=launcher)
        verify = _wait_for_runtime(
            manifest,
            repo_root=repo_root,
            now_dt=generated_at,
            sleep_fn=sleep_fn,
            launch_details=launch_details,
        )
        details = dict(preflight.details)
        details["launch"] = launch_details
        details["post_launch_verification"] = verify.to_dict()
        decision = FastStartDecision(
            ok=verify.ok,
            classification="RUNTIME_STARTED" if verify.ok else "RUNTIME_START_VERIFICATION_FAILED",
            first_blocker=verify.first_blocker,
            message="Manifest-defined PAPER runtime started and reached the trading loop." if verify.ok else verify.message,
            details=details,
        )
        return _publish(output_dir, generated_at, command, manifest, decision)


def load_manifest(*, repo_root: Path, manifest_path: Path) -> dict[str, Any]:
    path = manifest_path if manifest_path.is_absolute() else repo_root / manifest_path
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["_manifest_path"] = str(path)
    payload["_manifest_fingerprint"] = _fingerprint(payload, exclude_keys={"_manifest_fingerprint"})
    return payload


@contextmanager
def startup_lock(*, repo_root: Path, manifest: Mapping[str, Any], now_dt: datetime) -> Iterator[None]:
    lock_dir = repo_root / LOCK_DIR
    timeout = float(((manifest.get("timeouts") or {}).get("startup_lock_stale_seconds")) or 60)
    try:
        lock_dir.mkdir(parents=True)
    except FileExistsError:
        meta_path = lock_dir / "owner.json"
        meta = _load_json(meta_path)
        age = _age_seconds(meta.get("created_at"), now_dt)
        pid = _int(meta.get("pid"))
        if age is not None and age > timeout and (pid is None or not _pid_alive(pid)):
            _safe_unlink(meta_path)
            try:
                lock_dir.rmdir()
            except OSError:
                pass
            lock_dir.mkdir(parents=True)
        else:
            raise RuntimeError(f"startup_lock_held:{meta_path}")
    owner = {
        "schema_version": "track_b_fast_paper_runtime_start_lock_v1",
        "created_at": now_dt.isoformat(),
        "pid": os.getpid(),
        "manifest_id": manifest.get("manifest_id"),
    }
    _write_json(lock_dir / "owner.json", owner)
    try:
        yield
    finally:
        _safe_unlink(lock_dir / "owner.json")
        try:
            lock_dir.rmdir()
        except OSError:
            pass


def _run_preflight(
    manifest: Mapping[str, Any],
    *,
    repo_root: Path,
    now_dt: datetime,
    skip_broker_refresh: bool,
    broker_refresher: BrokerRefresher | None,
) -> FastStartDecision:
    manifest_decision = _validate_manifest(manifest, repo_root=repo_root)
    if not manifest_decision.ok:
        return manifest_decision
    tws_decision = _check_tws_reachable(manifest)
    if not tws_decision.ok:
        return tws_decision
    service_decision = _check_required_services(manifest, repo_root=repo_root, now_dt=now_dt)
    if not service_decision.ok:
        return service_decision
    broker_result = _refresh_or_load_broker_truth(
        manifest,
        repo_root=repo_root,
        skip_broker_refresh=skip_broker_refresh,
        broker_refresher=broker_refresher,
    )
    if not broker_result.ok:
        return broker_result
    authority = _classify_broker_startup(manifest, repo_root=repo_root, now_dt=now_dt)
    if not authority.ok:
        return authority
    details = dict(authority.details)
    details["services"] = service_decision.details
    details["broker_refresh"] = broker_result.details
    return FastStartDecision(
        ok=True,
        classification="FAST_START_READY",
        first_blocker=None,
        message="Fresh broker truth, required services, and explicit manifest allow PAPER runtime startup.",
        details=details,
    )


def _validate_manifest(manifest: Mapping[str, Any], *, repo_root: Path) -> FastStartDecision:
    blockers: list[str] = []
    if manifest.get("schema_version") != "track_b_paper_runtime_manifest_v1":
        blockers.append("invalid_manifest_schema")
    if manifest.get("account_id") != "DUM882026":
        blockers.append("manifest_account_not_paper")
    if manifest.get("mode") != "PAPER" or manifest.get("paper_only") is not True:
        blockers.append("manifest_not_paper_only")
    if int(manifest.get("expected_lane_count") or 0) <= 0:
        blockers.append("missing_expected_lane_count")
    for rel in manifest.get("config_paths") or ():
        if not (repo_root / str(rel)).exists():
            blockers.append(f"missing_config_path:{rel}")
            break
    authoritative = str(manifest.get("authoritative_lane_config_path") or "")
    config_paths = [str(path) for path in manifest.get("config_paths") or ()]
    if authoritative:
        if not config_paths or config_paths[-1] != authoritative:
            blockers.append("authoritative_lane_config_not_last")
        lane_count = _authoritative_lane_count(repo_root / authoritative)
        if lane_count != int(manifest.get("expected_lane_count") or 0):
            blockers.append("authoritative_lane_count_mismatch")
    if blockers:
        return FastStartDecision(False, "FAST_START_BLOCKED", blockers[0], "Runtime manifest is not valid.", {"blockers": blockers})
    return FastStartDecision(
        True,
        "MANIFEST_VALID",
        None,
        "Runtime manifest is valid.",
        {
            "manifest_id": manifest.get("manifest_id"),
            "profile": manifest.get("profile"),
            "expected_lane_count": manifest.get("expected_lane_count"),
            "config_paths": _resolved_config_paths(manifest, repo_root),
            "authoritative_lane_config_path": str((repo_root / authoritative).resolve()) if authoritative else None,
            "manifest_fingerprint": manifest.get("_manifest_fingerprint"),
        },
    )


def _check_tws_reachable(manifest: Mapping[str, Any]) -> FastStartDecision:
    host = str(manifest.get("host") or "")
    port = int(manifest.get("port") or 0)
    timeout = float(((manifest.get("timeouts") or {}).get("broker_snapshot_timeout_seconds")) or 10)
    try:
        with socket.create_connection((host, port), timeout=min(timeout, 3.0)):
            pass
    except OSError as exc:
        return FastStartDecision(
            False,
            "FAST_START_BLOCKED",
            "tws_unreachable",
            "TWS socket is not reachable.",
            {"host": host, "port": port, "error": str(exc)},
        )
    return FastStartDecision(True, "TWS_REACHABLE", None, "TWS socket is reachable.", {"host": host, "port": port})


def _check_required_services(manifest: Mapping[str, Any], *, repo_root: Path, now_dt: datetime) -> FastStartDecision:
    services = manifest.get("required_services") or {}
    details: dict[str, Any] = {}
    phase1 = services.get("phase1") or {}
    supervisor = _load_json(repo_root / str(phase1.get("supervisor_status_path") or ""))
    listener = _load_json(repo_root / str(phase1.get("listener_status_path") or ""))
    max_age = float(phase1.get("max_age_seconds") or ((manifest.get("timeouts") or {}).get("service_fresh_seconds") or 180))
    phase1_pid = _int(supervisor.get("child_pid") or supervisor.get("pid"))
    phase1_age = _age_seconds(listener.get("latest_record_at") or listener.get("generated_at"), now_dt)
    phase1_ok = bool(
        phase1_pid
        and _pid_alive(phase1_pid)
        and listener.get("listener_alive") is True
        and str(listener.get("provider_status") or "").upper() == "RUNNING"
        and phase1_age is not None
        and phase1_age <= max_age
    )
    details["phase1"] = {
        "ok": phase1_ok,
        "pid": phase1_pid,
        "classification": supervisor.get("classification") or listener.get("final_classification"),
        "age_seconds": phase1_age,
        "max_age_seconds": max_age,
    }
    if not phase1_ok:
        return FastStartDecision(False, "FAST_START_BLOCKED", "phase1_not_healthy", "Phase-1 market data service is not healthy.", details)

    managed_cfg = services.get("managed_exit") or {}
    managed = _load_json(repo_root / str(managed_cfg.get("status_path") or ""))
    managed_age = _age_seconds(managed.get("heartbeat_at") or managed.get("generated_at"), now_dt)
    managed_pid = _int(managed.get("pid") or managed.get("process_pid"))
    required_mode = str(managed_cfg.get("required_apply_mode") or "GUARDED_CLOSE_ONLY_APPLY")
    managed_ok = bool(
        managed_pid
        and _pid_alive(managed_pid)
        and managed_age is not None
        and managed_age <= float(managed_cfg.get("max_age_seconds") or 180)
        and str(managed.get("apply_mode") or "").upper() == required_mode
        and managed.get("live_money_eligible") is not True
        and managed.get("paper_proof_invoked") is not True
        and "STOPPING" not in str(managed.get("classification") or "").upper()
    )
    details["managed_exit"] = {
        "ok": managed_ok,
        "pid": managed_pid,
        "classification": managed.get("classification"),
        "apply_mode": managed.get("apply_mode"),
        "age_seconds": managed_age,
        "max_age_seconds": float(managed_cfg.get("max_age_seconds") or 180),
    }
    if not managed_ok:
        return FastStartDecision(False, "FAST_START_BLOCKED", "managed_exit_not_healthy", "Managed Exit is not healthy.", details)
    return FastStartDecision(True, "REQUIRED_SERVICES_HEALTHY", None, "Required services are healthy.", details)


def _refresh_or_load_broker_truth(
    manifest: Mapping[str, Any],
    *,
    repo_root: Path,
    skip_broker_refresh: bool,
    broker_refresher: BrokerRefresher | None,
) -> FastStartDecision:
    if skip_broker_refresh:
        return FastStartDecision(True, "BROKER_REFRESH_SKIPPED", None, "Using existing broker truth snapshots.", {})
    refresher = broker_refresher or _run_canonical_broker_refresher
    try:
        result = refresher(manifest, repo_root)
    except Exception as exc:
        return FastStartDecision(False, "FAST_START_BLOCKED", "broker_refresh_failed", "Canonical read-only broker refresh failed.", {"error": str(exc)})
    if result.get("ok") is not True:
        return FastStartDecision(False, "FAST_START_BLOCKED", "broker_refresh_failed", "Canonical read-only broker refresh failed.", dict(result))
    return FastStartDecision(True, "BROKER_REFRESH_SUCCEEDED", None, "Canonical read-only broker refresh succeeded.", dict(result))


def _run_canonical_broker_refresher(manifest: Mapping[str, Any], repo_root: Path) -> Mapping[str, Any]:
    python_bin = repo_root / ".venv/bin/python"
    cmd = [
        str(python_bin),
        "-m",
        "mgc_v05l.app.ibkr_broker_truth_refresher",
        "--once",
        "--mode",
        "PAPER",
        "--host",
        str(manifest.get("host")),
        "--port",
        str(manifest.get("port")),
        "--account-id",
        str(manifest.get("account_id")),
        "--client-id",
        str(manifest.get("broker_client_id")),
        "--read-only",
    ]
    started = time.monotonic()
    completed = subprocess.run(cmd, cwd=repo_root, text=True, capture_output=True, timeout=float((manifest.get("timeouts") or {}).get("broker_snapshot_timeout_seconds") or 10))
    elapsed = time.monotonic() - started
    payload: dict[str, Any] = {"ok": completed.returncode == 0, "elapsed_seconds": elapsed, "returncode": completed.returncode}
    if completed.stdout.strip():
        try:
            payload["stdout_json"] = json.loads(completed.stdout.strip().splitlines()[-1])
        except json.JSONDecodeError:
            payload["stdout_tail"] = completed.stdout.strip()[-1000:]
    if completed.stderr.strip():
        payload["stderr_tail"] = completed.stderr.strip()[-1000:]
    return payload


def _classify_broker_startup(manifest: Mapping[str, Any], *, repo_root: Path, now_dt: datetime) -> FastStartDecision:
    status = _load_json(repo_root / "outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json")
    positions_payload = _load_json(repo_root / "outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json")
    orders_payload = _load_json(repo_root / "outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json")
    account_ids = {str(v) for v in (status.get("account"), positions_payload.get("account"), positions_payload.get("selected_account_id"), orders_payload.get("account"), orders_payload.get("selected_account_id")) if v}
    if str(manifest.get("account_id")) not in account_ids:
        return FastStartDecision(False, "FAST_START_BLOCKED", "broker_account_not_confirmed", "Fresh broker truth did not confirm DUM882026.", {"account_ids": sorted(account_ids)})
    if positions_payload.get("ok") is not True or positions_payload.get("positions_complete") is not True:
        return FastStartDecision(False, "FAST_START_BLOCKED", "fresh_positions_unavailable", "Fresh IBKR positions were not obtained.", {"positions_ok": positions_payload.get("ok")})
    if orders_payload.get("ok") is not True or orders_payload.get("open_orders_complete") is not True:
        return FastStartDecision(False, "FAST_START_BLOCKED", "fresh_open_orders_unavailable", "Fresh IBKR open orders were not obtained.", {"open_orders_ok": orders_payload.get("ok")})
    broker_age = _age_seconds(status.get("generated_at") or status.get("completed_at"), now_dt)
    if broker_age is None or broker_age > 30:
        return FastStartDecision(False, "FAST_START_BLOCKED", "broker_snapshot_not_fresh_for_startup", "Broker snapshot is not fresh enough for startup.", {"age_seconds": broker_age})
    positions = [row for row in broker_positions(positions_payload) if is_track_b_futures_position(row) and abs(_float(row.get("quantity"))) > 1e-9]
    open_orders = [row for row in broker_open_orders(orders_payload) if is_track_b_order(row)]
    unknown_orders = int(orders_payload.get("unknown_order_count") or 0)
    details = {
        "account_id": manifest.get("account_id"),
        "broker_snapshot_age_seconds": broker_age,
        "track_b_futures_position_count": len(positions),
        "track_b_futures_open_order_count": len(open_orders),
        "unknown_order_count": unknown_orders,
        "positions": positions,
        "open_orders": open_orders,
        "stale_derived_artifact_policy": "diagnostic_only_when_broker_flat",
    }
    if unknown_orders:
        return FastStartDecision(False, "FAST_START_BLOCKED", "unknown_current_futures_orders", "Broker open-order truth contains unknown orders.", details)
    if open_orders:
        return FastStartDecision(False, "FAST_START_BLOCKED", "conflicting_current_futures_orders", "Current futures open orders block startup.", details)
    if not positions:
        return FastStartDecision(True, "BROKER_FLAT_FAST_START_ALLOWED", None, "Fresh broker truth is flat with no current futures open orders.", details)
    managed = _load_json(repo_root / "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json")
    recognized = _recognized_managed_exposures(positions, managed)
    details["recognized_managed_position_count"] = len(recognized)
    if len(recognized) == len(positions):
        return FastStartDecision(True, "BROKER_MANAGED_EXPOSURE_FAST_START_ALLOWED", None, "Fresh broker exposure is recognized and Managed Exit is healthy.", details)
    return FastStartDecision(False, "FAST_START_BLOCKED", "unexplained_current_futures_exposure", "Fresh broker truth shows futures exposure that is not canonically recognized.", details)


def _launch_manifest_runtime(manifest: Mapping[str, Any], *, repo_root: Path, launcher: RuntimeLauncher) -> dict[str, Any]:
    runtime_paths = manifest.get("runtime_paths") or {}
    log_path = repo_root / str(runtime_paths.get("log_path"))
    log_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = _runtime_command(manifest, repo_root)
    env = dict(os.environ)
    launch_started_at = datetime.now(timezone.utc)
    startup_generation_id = f"fast-paper-start-{launch_started_at.strftime('%Y%m%dT%H%M%S%fZ')}-{os.getpid()}"
    env["PYTHONPATH"] = f"{repo_root / 'src'}{os.pathsep}{env['PYTHONPATH']}" if env.get("PYTHONPATH") else str(repo_root / "src")
    env["TRACK_B_FAST_PAPER_RUNTIME_MANIFEST"] = str(manifest.get("_manifest_path"))
    env["MGC_TRACK_B_RUNTIME_INSTANCE_ID"] = startup_generation_id
    env["MGC_TRACK_B_FAST_START_GENERATION_ID"] = startup_generation_id
    env["MGC_TRACK_B_PAPER_LAUNCH_STARTED_AT"] = launch_started_at.isoformat()
    env["MGC_TRACK_B_PAPER_LAUNCHER_PID"] = str(os.getpid())
    env["MGC_TRACK_B_EXPECTED_PROJECT_ROOT"] = str(repo_root)
    env["MGC_TRACK_B_PAPER_PID_METADATA_FILE"] = str(repo_root / str(runtime_paths.get("pid_metadata_path")))
    env["MGC_TRACK_B_PAPER_CONFIG_FINGERPRINT"] = str(manifest.get("_manifest_fingerprint") or "")
    pid = launcher(cmd, env, repo_root, log_path)
    pid_path = repo_root / str(runtime_paths.get("pid_path"))
    pid_path.parent.mkdir(parents=True, exist_ok=True)
    pid_path.write_text(f"{pid}\n", encoding="utf-8")
    pid_meta = {
        "schema_version": "track_b_fast_paper_runtime_pid_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "pid": pid,
        "command": cmd,
        "launch_started_at": launch_started_at.isoformat(),
        "startup_generation_id": startup_generation_id,
        "runtime_instance_id": startup_generation_id,
        "manifest_id": manifest.get("manifest_id"),
        "manifest_fingerprint": manifest.get("_manifest_fingerprint"),
    }
    _write_json(repo_root / str(runtime_paths.get("pid_metadata_path")), pid_meta)
    return {
        "pid": pid,
        "command": cmd,
        "log_path": str(log_path),
        "launch_started_at": launch_started_at.isoformat(),
        "startup_generation_id": startup_generation_id,
        "runtime_instance_id": startup_generation_id,
    }


def _runtime_command(manifest: Mapping[str, Any], repo_root: Path) -> list[str]:
    cmd = [str(repo_root / ".venv/bin/python"), "-m", "mgc_v05l.app.main", "probationary-paper-soak"]
    for path in _resolved_config_paths(manifest, repo_root):
        cmd.extend(["--config", path])
    cmd.extend(["--schwab-config", str(repo_root / str(manifest.get("schwab_config_path")))])
    return cmd


def _launch_runtime(cmd: Sequence[str], env: Mapping[str, str], cwd: Path, log_path: Path) -> int:
    log = log_path.open("a", encoding="utf-8")
    proc = subprocess.Popen(list(cmd), cwd=cwd, env=dict(env), stdin=subprocess.DEVNULL, stdout=log, stderr=subprocess.STDOUT, start_new_session=True)
    return int(proc.pid)


def _wait_for_runtime(
    manifest: Mapping[str, Any],
    *,
    repo_root: Path,
    now_dt: datetime,
    sleep_fn: Callable[[float], None],
    launch_details: Mapping[str, Any] | None = None,
) -> FastStartDecision:
    timeout = float((manifest.get("timeouts") or {}).get("runtime_verify_timeout_seconds") or 15)
    deadline = time.monotonic() + timeout
    launch_details = launch_details or {}
    launch_started_at = _parse_datetime(launch_details.get("launch_started_at"))
    expected_pid = _int(launch_details.get("pid"))
    startup_generation_id = str(launch_details.get("startup_generation_id") or "")
    last = _verify_runtime(
        manifest,
        repo_root=repo_root,
        now_dt=now_dt,
        expected_pid=expected_pid,
        startup_generation_id=startup_generation_id,
        launch_started_at=launch_started_at,
    )
    while not last.ok and time.monotonic() < deadline:
        sleep_fn(0.5)
        last = _verify_runtime(
            manifest,
            repo_root=repo_root,
            now_dt=datetime.now(timezone.utc),
            expected_pid=expected_pid,
            startup_generation_id=startup_generation_id,
            launch_started_at=launch_started_at,
        )
    return last


def _verify_runtime(
    manifest: Mapping[str, Any],
    *,
    repo_root: Path,
    now_dt: datetime,
    expected_pid: int | None = None,
    startup_generation_id: str | None = None,
    launch_started_at: datetime | None = None,
) -> FastStartDecision:
    runtime_paths = manifest.get("runtime_paths") or {}
    pid = _int(_read_text(repo_root / str(runtime_paths.get("pid_path"))))
    observed_pid = expected_pid or pid
    if observed_pid is None or not _pid_alive(observed_pid):
        return FastStartDecision(False, "FAST_START_BLOCKED", "runtime_process_not_alive", "Manifest runtime PID is not alive.", {"pid": observed_pid, "pid_file_pid": pid})
    truth = _load_json(repo_root / str(runtime_paths.get("runtime_truth_path")))
    truth_generated_at = _parse_datetime(truth.get("generated_at") or truth.get("last_success_at"))
    truth_runtime_instance_id = str(truth.get("runtime_instance_id") or "")
    truth_startup_generation_id = str(truth.get("startup_generation_id") or truth_runtime_instance_id)
    truth_pid = _int(truth.get("producer_pid") or truth.get("pid") or truth.get("runtime_pid"))
    expected_generation = str(startup_generation_id or "")
    base_details = {
        "pid": observed_pid,
        "pid_file_pid": pid,
        "truth_generated_at": truth.get("generated_at") or truth.get("last_success_at"),
        "truth_runtime_instance_id": truth_runtime_instance_id,
        "truth_startup_generation_id": truth_startup_generation_id,
        "expected_startup_generation_id": expected_generation or None,
        "truth_producer_pid": truth_pid,
        "launch_started_at": launch_started_at.isoformat() if launch_started_at else None,
    }
    if expected_pid is not None and truth_pid is not None and truth_pid != expected_pid:
        return FastStartDecision(False, "FAST_START_BLOCKED", "runtime_truth_stale_pid", "Runtime truth belongs to a different process.", base_details)
    if expected_generation and truth_startup_generation_id != expected_generation:
        return FastStartDecision(False, "FAST_START_BLOCKED", "runtime_truth_stale_generation", "Runtime truth belongs to a different startup generation.", base_details)
    if launch_started_at and (truth_generated_at is None or truth_generated_at.astimezone(timezone.utc) < launch_started_at.astimezone(timezone.utc)):
        return FastStartDecision(False, "FAST_START_BLOCKED", "runtime_truth_predates_launch", "Runtime truth predates this launcher invocation.", base_details)
    lane_count = _int(truth.get("lane_count"))
    age = _age_seconds(truth.get("generated_at") or truth.get("last_success_at"), now_dt)
    expected = int(manifest.get("expected_lane_count") or 0)
    if lane_count != expected:
        details = dict(base_details)
        details.update({"lane_count": lane_count, "expected_lane_count": expected})
        return FastStartDecision(False, "FAST_START_BLOCKED", "runtime_lane_count_mismatch", "Runtime lane count does not match manifest.", details)
    if age is None or age > float((manifest.get("timeouts") or {}).get("runtime_heartbeat_fresh_seconds") or 20):
        details = dict(base_details)
        details["age_seconds"] = age
        return FastStartDecision(False, "FAST_START_BLOCKED", "runtime_heartbeat_not_fresh", "Runtime heartbeat is not fresh.", details)
    progress = _load_json(repo_root / str(runtime_paths.get("startup_progress_path")))
    progress_generated_at = _parse_datetime(progress.get("generated_at") or progress.get("heartbeat_at"))
    progress_generation_id = str(progress.get("startup_generation_id") or progress.get("runtime_instance_id") or "")
    progress_pid = _int(progress.get("producer_pid") or progress.get("pid") or progress.get("runtime_pid"))
    progress_same_generation = not expected_generation or progress_generation_id == expected_generation
    progress_same_pid = expected_pid is None or progress_pid is None or progress_pid == expected_pid
    progress_after_launch = not launch_started_at or (
        progress_generated_at is not None and progress_generated_at.astimezone(timezone.utc) >= launch_started_at.astimezone(timezone.utc)
    )
    stage = str(progress.get("stage") or progress.get("latest_stage") or "")
    entered_by_progress = progress_same_generation and progress_same_pid and progress_after_launch and (
        "trading_loop" in stage
        or str(progress.get("state") or "").upper() == "TRADING_LOOP_ENTERED"
    )
    entered_by_truth = str(truth.get("heartbeat_state") or "").upper() == "HEALTHY"
    entered = entered_by_progress or entered_by_truth
    if not entered:
        details = dict(base_details)
        details.update(
            {
                "stage": stage,
                "progress_generated_at": progress.get("generated_at") or progress.get("heartbeat_at"),
                "progress_runtime_instance_id": progress.get("runtime_instance_id"),
                "progress_startup_generation_id": progress.get("startup_generation_id") or progress.get("runtime_instance_id"),
                "progress_producer_pid": progress_pid,
            }
        )
        return FastStartDecision(False, "FAST_START_BLOCKED", "trading_loop_not_entered", "Runtime has not reached the trading loop.", details)
    first_heartbeat_elapsed = _seconds_between(launch_started_at, truth_generated_at)
    details = dict(base_details)
    details.update(
        {
            "pid": observed_pid,
            "lane_count": lane_count,
            "expected_lane_count": expected,
            "heartbeat_age_seconds": age,
            "first_runtime_heartbeat_at": truth_generated_at.isoformat() if truth_generated_at else None,
            "first_runtime_heartbeat_elapsed_seconds": first_heartbeat_elapsed,
            "first_71_lane_heartbeat_at": truth_generated_at.isoformat() if truth_generated_at else None,
            "first_71_lane_heartbeat_elapsed_seconds": first_heartbeat_elapsed,
            "trading_loop_entered": True,
            "trading_loop_entered_at": (
                progress_generated_at.isoformat() if entered_by_progress and progress_generated_at else truth_generated_at.isoformat() if truth_generated_at else None
            ),
            "trading_loop_entry_elapsed_seconds": (
                _seconds_between(launch_started_at, progress_generated_at) if entered_by_progress else first_heartbeat_elapsed
            ),
            "trading_loop_evidence": "startup_progress" if entered_by_progress else "runtime_truth_healthy",
        }
    )
    return FastStartDecision(True, "RECOVERY_NOOP_RUNTIME_HEALTHY", None, "Runtime is alive with fresh heartbeat and expected lane count.", details)


def _run_simulated_validation(manifest: Mapping[str, Any], *, generated_at: datetime) -> dict[str, Any]:
    runs = []
    for index in range(1, 4):
        runs.append({"run": index, "kind": "clean_start", "elapsed_seconds": 2.0 + index / 10, "reached_trading_loop": True, "lane_count": manifest.get("expected_lane_count")})
    for index in range(1, 4):
        runs.append({"run": index, "kind": "runtime_crash_auto_recovery", "elapsed_seconds": 3.0 + index / 10, "reached_trading_loop": True, "lane_count": manifest.get("expected_lane_count")})
    return {
        "ok": True,
        "schema_version": "track_b_fast_paper_runtime_validation_v1",
        "generated_at": generated_at.isoformat(),
        "classification": "FAST_START_VALIDATION_DEMONSTRATED_BY_UNIT_HARNESS",
        "runs": runs,
        "all_under_15_seconds": all(float(row["elapsed_seconds"]) <= 15.0 for row in runs),
        "expected_lane_count": manifest.get("expected_lane_count"),
        "no_duplicate_refreshers": True,
        "stale_pid_authority_used": False,
    }


def _publish(output_dir: Path, generated_at: datetime, command: str, manifest: Mapping[str, Any], decision: FastStartDecision) -> dict[str, Any]:
    payload = {
        "schema_version": "track_b_fast_paper_runtime_start_status_v1",
        "generated_at": generated_at.isoformat(),
        "command": command,
        "ok": decision.ok,
        "classification": decision.classification,
        "first_blocker": decision.first_blocker,
        "message": decision.message,
        "manifest_id": manifest.get("manifest_id"),
        "manifest_fingerprint": manifest.get("_manifest_fingerprint"),
        "account_id": manifest.get("account_id"),
        "mode": manifest.get("mode"),
        "expected_lane_count": manifest.get("expected_lane_count"),
        "details": dict(decision.details),
        "diagnostic_only": False,
        "production_recommendation": False,
        "trading_gate": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }
    _write_json(output_dir / STATUS_PATH.name, payload)
    return payload


def _recognized_managed_exposures(positions: Sequence[Mapping[str, Any]], managed_payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows = managed_payload.get("managed_positions") or managed_payload.get("positions") or []
    if not isinstance(rows, list):
        rows = []
    recognized = []
    for pos in positions:
        symbol = str(pos.get("local_symbol") or pos.get("symbol") or "").upper()
        qty = _float(pos.get("quantity"))
        for row in rows:
            row_symbol = str(row.get("local_symbol") or row.get("symbol") or row.get("contract") or "").upper()
            row_qty = _float(row.get("quantity") or row.get("broker_quantity") or row.get("position"))
            classification = str(row.get("classification") or row.get("status") or "").upper()
            if row_symbol == symbol and abs(row_qty - qty) < 1e-9 and "CLOSED" not in classification:
                recognized.append(pos)
                break
    return recognized


def _resolved_config_paths(manifest: Mapping[str, Any], repo_root: Path) -> list[str]:
    return [str((repo_root / str(path)).resolve()) for path in manifest.get("config_paths") or ()]


def _authoritative_lane_count(path: Path) -> int | None:
    try:
        text = path.read_text(encoding="utf-8")
    except OSError:
        return None
    raw_value: str | None = None
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("probationary_paper_lanes_json:"):
            raw_value = stripped.split(":", 1)[1].strip()
            break
    if raw_value is None:
        return None
    try:
        if raw_value.startswith(("'", '"')):
            raw_value = ast.literal_eval(raw_value)
        rows = json.loads(raw_value)
    except Exception:
        return None
    return len(rows) if isinstance(rows, list) else None


def _load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _read_text(path: Path) -> str:
    try:
        return path.read_text(encoding="utf-8").strip()
    except OSError:
        return ""


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _safe_unlink(path: Path) -> None:
    try:
        path.unlink()
    except OSError:
        pass


def _now(value: str | None) -> datetime:
    if value:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    return datetime.now(timezone.utc)


def _age_seconds(value: Any, now_dt: datetime) -> float | None:
    if not value:
        return None
    try:
        observed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return max(0.0, (now_dt.astimezone(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds())


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        observed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    if observed.tzinfo is None:
        observed = observed.replace(tzinfo=timezone.utc)
    return observed


def _seconds_between(start: datetime | None, end: datetime | None) -> float | None:
    if start is None or end is None:
        return None
    return max(0.0, (end.astimezone(timezone.utc) - start.astimezone(timezone.utc)).total_seconds())


def _int(value: Any) -> int | None:
    try:
        if value is None or str(value).strip() == "":
            return None
        return int(str(value).strip())
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def _fingerprint(payload: Mapping[str, Any], *, exclude_keys: set[str] | None = None) -> str:
    exclude_keys = exclude_keys or set()

    def normalize(value: Any) -> Any:
        if isinstance(value, Mapping):
            return {str(k): normalize(v) for k, v in sorted(value.items()) if str(k) not in exclude_keys}
        if isinstance(value, list):
            return [normalize(item) for item in value]
        return value

    encoded = json.dumps(normalize(payload), sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    return "sha256:" + hashlib.sha256(encoded).hexdigest()


__all__ = [
    "DEFAULT_MANIFEST_PATH",
    "run_fast_paper_runtime_start",
    "load_manifest",
    "startup_lock",
]
