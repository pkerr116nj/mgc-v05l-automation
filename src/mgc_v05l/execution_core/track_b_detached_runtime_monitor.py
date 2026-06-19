"""Detached Track B PAPER runtime child-process observability.

The startup wrapper owns a shell child that becomes the paper runtime via exec.
This helper records that child's lifecycle in a small status artifact so the
launcher does not have to infer liveness from runtime truth alone.
"""

from __future__ import annotations

import argparse
import json
import os
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence


def build_detached_runtime_child_status(
    *,
    event: str,
    status_path: Path,
    pid: int,
    started_at: str | None = None,
    exit_code: int | None = None,
    observed_at: datetime | None = None,
    repo_root: Path | None = None,
    log_file: Path | None = None,
    pid_file: Path | None = None,
    config_paths_file: Path | None = None,
    runtime_truth_file: Path | None = None,
    post_truth_progress_file: Path | None = None,
    runtime_instance_id: str | None = None,
    source_commit: str | None = None,
    python_bin: str | None = None,
    child_command: str | None = None,
    parent_pid: int | None = None,
) -> dict[str, Any]:
    actual_observed_at = _ensure_utc(observed_at or datetime.now(UTC))
    existing = _read_json(status_path)
    truth = _read_json(runtime_truth_file)
    progress = _read_json(post_truth_progress_file)
    summary = _last_json_summary(log_file)
    truth_marker = _truth_marker(truth, pid)
    progress_marker = _progress_marker(progress, pid)
    runtime_cycle_marker = progress_marker if progress_marker.get("stage") == "runtime_cycle" else {}
    process_alive = _pid_alive(pid)

    exit_signal = _exit_signal(exit_code)
    classification, final_status, termination_reason = _classify(
        event=event,
        exit_code=exit_code,
        exit_signal=exit_signal,
        process_alive=process_alive,
        truth_marker=truth_marker,
        runtime_cycle_marker=runtime_cycle_marker,
        summary=summary,
    )

    payload: dict[str, Any] = {
        "schema_version": "track_b_detached_runtime_child_status_v1",
        "generated_at": actual_observed_at.isoformat(),
        "classification": classification,
        "child_pid": pid,
        "pid": pid,
        "child_started_at": started_at or existing.get("child_started_at"),
        "child_command": child_command or existing.get("child_command"),
        "supervisor_parent_pid": parent_pid if parent_pid is not None else existing.get("supervisor_parent_pid"),
        "child_final_status": final_status,
        "child_exit_code": exit_code,
        "child_exit_signal": exit_signal,
        "process_alive": process_alive,
        "termination_reason": termination_reason,
        "runtime_instance_id": runtime_instance_id,
        "source_commit": source_commit,
        "repo_root": str(repo_root) if repo_root is not None else None,
        "cwd": os.getcwd(),
        "python_bin": python_bin,
        "pid_file": str(pid_file) if pid_file is not None else None,
        "log_file": str(log_file) if log_file is not None else None,
        "config_paths_file": str(config_paths_file) if config_paths_file is not None else None,
        "runtime_truth_file": str(runtime_truth_file) if runtime_truth_file is not None else None,
        "post_truth_progress_file": str(post_truth_progress_file) if post_truth_progress_file is not None else None,
        "runtime_truth_marker": truth_marker or None,
        "last_post_truth_marker": progress_marker or None,
        "last_runtime_cycle_marker": runtime_cycle_marker or None,
        "runtime_cycle_marker_observed": bool(runtime_cycle_marker),
        "runtime_cycle_completed": runtime_cycle_marker.get("state") == "COMPLETED",
        "last_summary": summary or None,
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "submit_authority": False,
        "broker_mutation_allowed": False,
    }
    _write_json(status_path, payload)
    return payload


def _classify(
    *,
    event: str,
    exit_code: int | None,
    exit_signal: int | None,
    process_alive: bool,
    truth_marker: Mapping[str, Any],
    runtime_cycle_marker: Mapping[str, Any],
    summary: Mapping[str, Any],
) -> tuple[str, str, str | None]:
    normalized_event = event.strip().lower()
    if normalized_event != "exited":
        if process_alive and runtime_cycle_marker:
            if _completed_cycle_without_newer_truth(
                runtime_cycle_marker=runtime_cycle_marker,
                truth_marker=truth_marker,
            ):
                return "RUNTIME_CHILD_CYCLE_COMPLETED_WAITING_FOR_NEXT_TRUTH", "RUNNING", None
            return "RUNTIME_CHILD_RUNNING_CYCLE_OBSERVED", "RUNNING", None
        if process_alive and truth_marker:
            return "RUNTIME_CHILD_RUNNING_INITIAL_TRUTH", "RUNNING", None
        if process_alive:
            return "RUNTIME_CHILD_STARTED", "RUNNING", None
        return "RUNTIME_CHILD_NOT_ALIVE", "NOT_ALIVE", "child_not_alive"

    if exit_signal is not None:
        if runtime_cycle_marker:
            return "RUNTIME_CHILD_SIGNALED_AFTER_CYCLE_MARKER", "SIGNALED", f"signal_{exit_signal}"
        if truth_marker:
            return "RUNTIME_CHILD_SIGNALED_AFTER_INITIAL_TRUTH", "SIGNALED", f"signal_{exit_signal}"
        return "RUNTIME_CHILD_SIGNALED_BEFORE_RUNTIME_TRUTH", "SIGNALED", f"signal_{exit_signal}"

    summary_stop_reason = summary.get("stop_reason") if isinstance(summary, Mapping) else None
    reconciliation_clean = summary.get("reconciliation_clean") if isinstance(summary, Mapping) else None
    cycle_completed = runtime_cycle_marker.get("state") == "COMPLETED"
    if exit_code == 0 and cycle_completed and (summary_stop_reason in {None, ""}) and reconciliation_clean is not False:
        return "RUNTIME_CLEAN_EXIT_AFTER_CYCLE", "EXITED", "clean_runtime_exit_after_cycle"
    if runtime_cycle_marker:
        return "RUNTIME_EXITED_BEFORE_DURABLE_READY", "EXITED", "runtime_exited_after_runtime_cycle_marker"
    if truth_marker:
        return "RUNTIME_EXITED_BEFORE_DURABLE_READY", "EXITED", "runtime_exited_after_initial_truth"
    return "RUNTIME_EXITED_BEFORE_DURABLE_READY", "EXITED", "runtime_exited_before_runtime_truth"


def _completed_cycle_without_newer_truth(
    *,
    runtime_cycle_marker: Mapping[str, Any],
    truth_marker: Mapping[str, Any],
) -> bool:
    if runtime_cycle_marker.get("state") != "COMPLETED":
        return False
    marker_at = _parse_datetime(runtime_cycle_marker.get("generated_at") or runtime_cycle_marker.get("heartbeat_at"))
    truth_at = _parse_datetime(truth_marker.get("generated_at"))
    return marker_at is not None and truth_at is not None and truth_at <= marker_at


def _truth_marker(payload: Mapping[str, Any], pid: int) -> dict[str, Any]:
    if not payload:
        return {}
    if _optional_int(payload.get("producer_pid") or payload.get("pid")) != pid:
        return {}
    return {
        "generated_at": payload.get("generated_at"),
        "producer_pid": payload.get("producer_pid") or payload.get("pid"),
        "runtime_instance_id": payload.get("runtime_instance_id"),
        "source_commit": payload.get("source_commit"),
        "profile": payload.get("profile"),
        "lane_count": payload.get("lane_count"),
        "heartbeat_state": payload.get("heartbeat_state"),
        "freshness_state": payload.get("freshness_state"),
        "writer_authority": payload.get("writer_authority"),
    }


def _progress_marker(payload: Mapping[str, Any], pid: int) -> dict[str, Any]:
    if not payload:
        return {}
    if _optional_int(payload.get("producer_pid")) != pid:
        return {}
    return {
        "generated_at": payload.get("generated_at"),
        "heartbeat_at": payload.get("heartbeat_at"),
        "producer_pid": payload.get("producer_pid"),
        "runtime_instance_id": payload.get("runtime_instance_id"),
        "stage": payload.get("stage"),
        "state": payload.get("state"),
        "submit_authority": payload.get("submit_authority") is True,
        "broker_mutation_allowed": payload.get("broker_mutation_allowed") is True,
        "payload": payload.get("payload") if isinstance(payload.get("payload"), Mapping) else None,
    }


def _last_json_summary(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        rows = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return {}
    for row in reversed(rows[-200:]):
        text = row.strip()
        if not text.startswith("{"):
            continue
        try:
            payload = json.loads(text)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict) and {"stop_reason", "reconciliation_clean"} & set(payload):
            return payload
    return {}


def _read_json(path: Path | None) -> dict[str, Any]:
    if path is None:
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _pid_alive(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except OSError:
        return False
    return True


def _optional_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _exit_signal(exit_code: int | None) -> int | None:
    if exit_code is None:
        return None
    # POSIX shells report a signal-terminated child as 128 + signal.
    if 129 <= exit_code <= 192:
        return exit_code - 128
    # Python subprocess-style negative return codes are also accepted in tests.
    if exit_code < 0:
        return abs(exit_code)
    return None


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event", choices=("started", "heartbeat", "exited"), required=True)
    parser.add_argument("--status-path", type=Path, required=True)
    parser.add_argument("--pid", type=int, required=True)
    parser.add_argument("--started-at")
    parser.add_argument("--exit-code", type=int)
    parser.add_argument("--repo-root", type=Path)
    parser.add_argument("--log-file", type=Path)
    parser.add_argument("--pid-file", type=Path)
    parser.add_argument("--config-paths-file", type=Path)
    parser.add_argument("--runtime-truth-file", type=Path)
    parser.add_argument("--post-truth-progress-file", type=Path)
    parser.add_argument("--runtime-instance-id")
    parser.add_argument("--source-commit")
    parser.add_argument("--python-bin")
    parser.add_argument("--child-command")
    parser.add_argument("--parent-pid", type=int)
    args = parser.parse_args(argv)
    payload = build_detached_runtime_child_status(
        event=args.event,
        status_path=args.status_path,
        pid=args.pid,
        started_at=args.started_at,
        exit_code=args.exit_code,
        repo_root=args.repo_root,
        log_file=args.log_file,
        pid_file=args.pid_file,
        config_paths_file=args.config_paths_file,
        runtime_truth_file=args.runtime_truth_file,
        post_truth_progress_file=args.post_truth_progress_file,
        runtime_instance_id=args.runtime_instance_id,
        source_commit=args.source_commit,
        python_bin=args.python_bin,
        child_command=args.child_command,
        parent_pid=args.parent_pid,
    )
    print(json.dumps(payload, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
