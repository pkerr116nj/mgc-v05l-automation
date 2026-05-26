"""Canonical Track B runtime authority resolution.

Runtime authority is execution-core state plus live process verification.  Legacy
operator/canonical PID artifacts are useful diagnostics, but they are not
authority when a current guarded PAPER loop is verified.
"""

from __future__ import annotations

import errno
import json
import os
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

RUNTIME_AUTHORITY_CURRENT = "RUNTIME_AUTHORITY_CURRENT"
RUNTIME_AUTHORITY_STALE_LEGACY_IGNORED = "RUNTIME_AUTHORITY_STALE_LEGACY_IGNORED"
RUNTIME_AUTHORITY_MISSING = "RUNTIME_AUTHORITY_MISSING"
RUNTIME_AUTHORITY_WRONG_ROOT = "RUNTIME_AUTHORITY_WRONG_ROOT"
RUNTIME_AUTHORITY_DUPLICATE_WRITER = "RUNTIME_AUTHORITY_DUPLICATE_WRITER"
RUNTIME_AUTHORITY_GENERATION_MISMATCH = "RUNTIME_AUTHORITY_GENERATION_MISMATCH"

CONTROL_PLANE_SNAPSHOT_PATH = Path("outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json")
RUNTIME_SAFE_STATE_PATH = Path("outputs/track_b_execution_core/safe_state/latest_runtime_safe_state_envelope.json")
GUARDED_PAPER_LOOP_PATH = Path("outputs/track_b_execution_core/p0_observe_only/latest_p0_observe_only_loop.json")
AGENT_HEALTH_PATH = Path("outputs/track_b_execution_core/agent_health/latest_agent_health.json")
LEGACY_CANONICAL_READINESS_PATH = Path("outputs/operator_dashboard/runtime/latest_canonical_readiness.json")

EXPECTED_GUARDED_LOOP_TOKEN = "track_b_p0_observe_only_loop"
EXPECTED_GUARDED_MODE_TOKEN = "--mode guarded-paper"
EXPECTED_GUARDED_MODE = "guarded-paper"

PidRunning = Callable[[int | None], bool]
ProcessRowsProvider = Callable[[Path], Sequence[Mapping[str, Any]]]


@dataclass(frozen=True)
class RuntimeAuthorityResolverConfig:
    repo_root: Path
    expected_mode: str = EXPECTED_GUARDED_MODE
    control_plane_path: Path = CONTROL_PLANE_SNAPSHOT_PATH
    safe_state_path: Path = RUNTIME_SAFE_STATE_PATH
    guarded_loop_path: Path = GUARDED_PAPER_LOOP_PATH
    agent_health_path: Path = AGENT_HEALTH_PATH
    legacy_canonical_readiness_path: Path = LEGACY_CANONICAL_READINESS_PATH


def resolve_track_b_runtime_authority(
    config: RuntimeAuthorityResolverConfig,
    *,
    legacy_operator_status: Mapping[str, Any] | None = None,
    process_rows_provider: ProcessRowsProvider | None = None,
    pid_running: PidRunning | None = None,
) -> dict[str, Any]:
    repo_root = config.repo_root.expanduser().resolve()
    pid_running = pid_running or _pid_is_running
    control_plane = _read_json(repo_root / config.control_plane_path)
    safe_state = _read_json(repo_root / config.safe_state_path)
    guarded_loop = _read_json(repo_root / config.guarded_loop_path)
    agent_health = _read_json(repo_root / config.agent_health_path)
    legacy_canonical = _read_json(repo_root / config.legacy_canonical_readiness_path)
    legacy_operator = dict(legacy_operator_status or {})

    process_rows = tuple(
        dict(row)
        for row in (
            process_rows_provider(repo_root)
            if process_rows_provider is not None
            else _guarded_loop_processes(repo_root=repo_root, expected_mode=config.expected_mode)
        )
        if isinstance(row, Mapping)
    )
    matching_rows = tuple(row for row in process_rows if bool(row.get("root_matches")) and not bool(row.get("wrong_root")))
    selected = dict(matching_rows[0]) if len(matching_rows) == 1 else {}
    selected_pid = _int_value(selected.get("pid")) or None
    selected_command = str(selected.get("command") or "").strip() or None
    selected_pid_active = bool(
        selected_pid
        and (
            selected.get("pid_active") is not False
            or pid_running(selected_pid)
        )
    )

    blockers: list[str] = []
    diagnostics: list[str] = []
    source_paths = {
        "guarded_loop": str(repo_root / config.guarded_loop_path),
        "control_plane": str(repo_root / config.control_plane_path),
        "safe_state": str(repo_root / config.safe_state_path),
        "agent_health": str(repo_root / config.agent_health_path),
        "legacy_canonical_readiness": str(repo_root / config.legacy_canonical_readiness_path),
    }

    _validate_execution_core_state(
        blockers=blockers,
        control_plane=control_plane,
        safe_state=safe_state,
        guarded_loop=guarded_loop,
        agent_health=agent_health,
        expected_mode=config.expected_mode,
    )
    if not process_rows:
        blockers.append("guarded_paper_loop_process_missing")
    if len(matching_rows) > 1:
        blockers.append("duplicate_runtime_submitters")
    if process_rows and not matching_rows:
        blockers.append("runtime_not_verified_from_dev_root")
    if any(bool(row.get("wrong_root")) for row in process_rows):
        blockers.append("runtime_from_documents_or_icloud")
        blockers.append("runtime_from_wrong_root")
    if selected_pid and not selected_pid_active:
        blockers.append("runtime_pid_not_active")
    if not selected_pid and matching_rows:
        blockers.append("runtime_pid_missing")

    loop_generation = _guarded_loop_runtime_generation(guarded_loop)
    current_generation = (
        safe_state.get("runtime_generation_id")
        or control_plane.get("safe_state_runtime_generation_id")
        or control_plane.get("runtime_generation_id")
    )
    if loop_generation and current_generation and str(loop_generation) != str(current_generation):
        blockers.append("runtime_generation_mismatch")

    legacy_pid = _legacy_runtime_pid(legacy_operator=legacy_operator, legacy_canonical=legacy_canonical)
    legacy_pid_active = pid_running(legacy_pid) if legacy_pid else False
    unique_blockers = tuple(dict.fromkeys(blockers))
    valid_current = not unique_blockers
    legacy_stale_ignored = bool(valid_current and legacy_pid and legacy_pid != selected_pid and not legacy_pid_active)
    if legacy_stale_ignored:
        diagnostics.append("stale_legacy_runtime_pid_ignored")

    if valid_current:
        classification = RUNTIME_AUTHORITY_STALE_LEGACY_IGNORED if legacy_stale_ignored else RUNTIME_AUTHORITY_CURRENT
    else:
        classification = _blocked_classification(unique_blockers)
        if legacy_pid and not legacy_pid_active and not selected_pid:
            diagnostics.append("stale_legacy_runtime_pid_blocks_without_current_authority")
            unique_blockers = tuple(dict.fromkeys((*unique_blockers, "runtime_pid_not_active", "runtime_not_verified_from_dev_root")))

    return {
        "classification": classification,
        "valid": bool(valid_current),
        "source": "guarded_paper_loop_control_plane",
        "authority_source_priority": (
            "current_guarded_paper_loop",
            "control_plane_snapshot_runtime_generation",
            "safe_state_runtime_evidence",
            "live_process_verification",
            "legacy_operator_canonical_status_diagnostic_only",
        ),
        "pid": selected_pid,
        "runtime_pid": selected_pid,
        "pid_active": bool(selected_pid_active),
        "runtime_pid_active": bool(selected_pid_active),
        "command": selected_command,
        "runtime_command": selected_command,
        "runtime_cwd": str(repo_root) if selected else None,
        "runtime_from_dev_root": bool(selected_pid_active and selected),
        "runtime_generation_id": str(current_generation or loop_generation or "") or None,
        "loop_runtime_generation_id": str(loop_generation or "") or None,
        "mode": str(guarded_loop.get("loop_mode") or guarded_loop.get("mode") or config.expected_mode),
        "duplicate_count": len(matching_rows),
        "process_count": len(process_rows),
        "matching_process_count": len(matching_rows),
        "blockers": unique_blockers,
        "diagnostics": tuple(dict.fromkeys(diagnostics)),
        "legacy_runtime_pid": legacy_pid,
        "legacy_runtime_pid_active": legacy_pid_active,
        "legacy_stale_ignored": legacy_stale_ignored,
        "legacy_stale_policy": RUNTIME_AUTHORITY_STALE_LEGACY_IGNORED if legacy_stale_ignored else None,
        "live_money_eligible": any(
            payload.get("live_money_eligible") is True for payload in (control_plane, safe_state, guarded_loop, agent_health)
        ),
        "paper_proof_invoked": any(
            payload.get("paper_proof_invoked") is True for payload in (control_plane, safe_state, guarded_loop, agent_health)
        ),
        "source_artifacts": source_paths,
        "dashboard_projection_consumed": False,
    }


def _validate_execution_core_state(
    *,
    blockers: list[str],
    control_plane: Mapping[str, Any],
    safe_state: Mapping[str, Any],
    guarded_loop: Mapping[str, Any],
    agent_health: Mapping[str, Any],
    expected_mode: str,
) -> None:
    cp_classification = str(control_plane.get("classification") or "").strip().upper()
    coherence = str(control_plane.get("shared_truth_coherence_status") or "").strip().upper()
    supervisor = str(control_plane.get("runtime_supervisor_classification") or "").strip().upper()
    safe_classification = str(safe_state.get("safe_state_classification") or safe_state.get("classification") or "").strip().upper()
    loop_mode = str(guarded_loop.get("loop_mode") or "").strip()

    if not control_plane:
        blockers.append("control_plane_missing")
    elif cp_classification != "CONTROL_PLANE_SNAPSHOT_READY":
        blockers.append("control_plane_not_ready")
    if control_plane and coherence != "COHERENT":
        blockers.append("control_plane_not_coherent")
    if supervisor and supervisor not in {
        "SUPERVISOR_RUNTIME_START_ALLOWED",
        "SUPERVISOR_RUNTIME_ACTIVE_MONITOR",
        "SUPERVISOR_NO_ACTION_NEEDED",
    }:
        blockers.append("runtime_supervisor_not_start_or_monitor_ready")
    if not safe_state:
        blockers.append("safe_state_missing")
    elif safe_classification != "SAFE_STATE_NORMAL":
        blockers.append("safe_state_not_normal")
    if not guarded_loop:
        blockers.append("guarded_paper_loop_artifact_missing")
    elif loop_mode != expected_mode:
        blockers.append("guarded_paper_loop_artifact_not_guarded_mode")

    if control_plane.get("agent_health_has_duplicate_writer") is True:
        blockers.append("duplicate_runtime_submitters")
    if agent_health.get("agent_health_has_duplicate_writer") is True or agent_health.get("duplicate_writer_count"):
        blockers.append("duplicate_runtime_submitters")
    for payload in (control_plane, safe_state, guarded_loop, agent_health):
        if payload.get("live_money_eligible") is True:
            blockers.append("live_money_eligible_true")
        if payload.get("paper_proof_invoked") is True:
            blockers.append("paper_proof_invoked_true")


def _blocked_classification(blockers: Sequence[str]) -> str:
    blocker_set = set(blockers)
    if "duplicate_runtime_submitters" in blocker_set:
        return RUNTIME_AUTHORITY_DUPLICATE_WRITER
    if "runtime_generation_mismatch" in blocker_set:
        return RUNTIME_AUTHORITY_GENERATION_MISMATCH
    if {"runtime_not_verified_from_dev_root", "runtime_from_documents_or_icloud", "runtime_from_wrong_root"} & blocker_set:
        return RUNTIME_AUTHORITY_WRONG_ROOT
    return RUNTIME_AUTHORITY_MISSING


def _guarded_loop_runtime_generation(payload: Mapping[str, Any]) -> str | None:
    latest_iteration = payload.get("latest_iteration")
    if isinstance(latest_iteration, Mapping):
        snapshot = latest_iteration.get("control_plane_snapshot")
        if isinstance(snapshot, Mapping):
            generation = snapshot.get("safe_state_runtime_generation_id") or snapshot.get("runtime_generation_id")
            if generation:
                return str(generation)
    for key in ("runtime_generation_id", "safe_state_runtime_generation_id"):
        if payload.get(key):
            return str(payload.get(key))
    return None


def _legacy_runtime_pid(*, legacy_operator: Mapping[str, Any], legacy_canonical: Mapping[str, Any]) -> int | None:
    for payload in (legacy_operator, legacy_canonical, legacy_canonical.get("runtime") if isinstance(legacy_canonical.get("runtime"), Mapping) else {}):
        if not isinstance(payload, Mapping):
            continue
        for key in ("source_runtime_pid", "runtime_pid", "pid"):
            value = _int_value(payload.get(key))
            if value > 0:
                return value
    return None


def _guarded_loop_processes(*, repo_root: Path, expected_mode: str) -> tuple[dict[str, Any], ...]:
    try:
        completed = subprocess.run(
            ["ps", "-efww"],
            check=False,
            capture_output=True,
            text=True,
            timeout=2,
        )
    except (OSError, subprocess.SubprocessError):
        return ()
    repo_text = str(repo_root.resolve())
    rows: list[dict[str, Any]] = []
    for line in completed.stdout.splitlines():
        if EXPECTED_GUARDED_LOOP_TOKEN not in line or f"--mode {expected_mode}" not in line:
            continue
        if (
            " egrep " in line
            or " grep " in line
            or "/bin/zsh -c" in line
            or "SCREEN -dmS" in line
            or " login -pflq " in line
        ):
            continue
        parts = line.split(None, 7)
        pid = _int_value(parts[1]) if len(parts) > 1 else 0
        if pid <= 0:
            continue
        command = parts[7] if len(parts) > 7 else line
        rows.append(
            {
                "pid": pid,
                "command": command,
                "root_matches": repo_text in line,
                "wrong_root": "Documents/MGC-v05l" in line or "Mobile Documents" in line,
                "pid_active": True,
            }
        )
    return tuple(rows)


def _pid_is_running(pid: int | None) -> bool:
    if pid is None or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.EPERM:
            return True
        return False
    return True


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _int_value(value: object) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0
