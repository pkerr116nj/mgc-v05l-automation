"""Broker authority publisher ownership status for Track B PAPER.

This module only reads and writes local status artifacts.  It does not connect
to IBKR, submit orders, cancel orders, restart services, or mutate broker state.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic

DEFAULT_BROKER_AUTHORITY_OWNERSHIP_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_authority_ownership.json"
)
BROKER_AUTHORITY_WRITER = "ibkr_broker_truth_refresher"
BROKER_AUTHORITY_EXPECTED_MIN_COMMIT = ""


def build_broker_authority_ownership_status(
    *,
    repo_root: Path,
    lease: Mapping[str, Any],
    broker_session_authority: Mapping[str, Any],
    generated_at: str | None = None,
    writer_pid: int | None = None,
    service_label: str | None = None,
    expected_min_commit: str = BROKER_AUTHORITY_EXPECTED_MIN_COMMIT,
    source_commit: str | None = None,
    existing_status: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build the hot broker-authority ownership status artifact."""

    now = generated_at or datetime.now(timezone.utc).isoformat()
    existing = _mapping(existing_status)
    attempts = _list_of_mappings(existing.get("non_owner_hot_write_attempts"))
    lease_generation = lease.get("authority_generation_id")
    bsa_generation = broker_session_authority.get("authority_generation_id")
    lease_writer = str(lease.get("authority_writer") or "")
    bsa_writer = str(broker_session_authority.get("authority_writer") or "")
    aligned = bool(lease_generation and lease_generation == bsa_generation)
    writer_aligned = lease_writer == BROKER_AUTHORITY_WRITER and bsa_writer == BROKER_AUTHORITY_WRITER
    duplicate_attempt = bool(attempts)
    actual_source_commit = source_commit or _source_commit(repo_root)
    running_writer_needs_reload = _commit_predates(actual_source_commit, expected_min_commit)
    classification = _classification(
        aligned=aligned,
        writer_aligned=writer_aligned,
        duplicate_attempt=duplicate_attempt,
        running_writer_needs_reload=running_writer_needs_reload,
    )
    return {
        "schema_version": "track_b_broker_authority_ownership_v1",
        "generated_at": now,
        "classification": classification,
        "authority_writer": lease_writer or bsa_writer or None,
        "authority_generation_id": lease_generation if aligned else None,
        "lease_authority_generation_id": lease_generation,
        "bsa_authority_generation_id": bsa_generation,
        "lease_authority_writer": lease_writer or None,
        "bsa_authority_writer": bsa_writer or None,
        "writer_pid": writer_pid,
        "service_label": service_label,
        "source_commit": actual_source_commit,
        "expected_min_commit": expected_min_commit or None,
        "hot_artifact_paths_owned": {
            "broker_truth_lease": str(repo_root / "outputs/operator_dashboard/runtime/latest_broker_truth_lease.json"),
            "broker_session_authority": str(
                repo_root / "outputs/operator_dashboard/runtime/latest_broker_session_authority.json"
            ),
        },
        "lease_bsa_generation_aligned": aligned,
        "duplicate_hot_writer_detected": duplicate_attempt or not writer_aligned,
        "non_owner_hot_write_attempt_count": len(attempts),
        "non_owner_hot_write_attempts": attempts,
        "running_writer_needs_reload": running_writer_needs_reload,
        "broker_authority_publisher_healthy": bool(aligned and writer_aligned and not running_writer_needs_reload),
        "next_safe_action": _next_safe_action(
            aligned=aligned,
            writer_aligned=writer_aligned,
            duplicate_attempt=duplicate_attempt,
            running_writer_needs_reload=running_writer_needs_reload,
        ),
        "read_only": True,
        "broker_mutation_allowed": False,
        "submit_attempted": False,
        "close_attempted": False,
        "cancel_attempted": False,
        "global_cancel_allowed": False,
        "broad_flatten_allowed": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }


def write_broker_authority_ownership_status(
    *,
    repo_root: Path,
    status: Mapping[str, Any],
    output_path: Path | None = None,
) -> Path:
    path = output_path or repo_root / DEFAULT_BROKER_AUTHORITY_OWNERSHIP_ARTIFACT
    return write_json_atomic(path, status)


def load_broker_authority_ownership_status(repo_root: Path, path: Path | None = None) -> dict[str, Any]:
    return _read_json(path or repo_root / DEFAULT_BROKER_AUTHORITY_OWNERSHIP_ARTIFACT)


def record_non_owner_hot_write_attempt(
    *,
    path: Path,
    incoming_lease: Mapping[str, Any],
    existing_lease: Mapping[str, Any],
    generated_at: str | None = None,
) -> dict[str, Any]:
    """Record a quarantined non-owner hot write attempt."""

    repo_root = _infer_repo_root(path)
    status_path = repo_root / DEFAULT_BROKER_AUTHORITY_OWNERSHIP_ARTIFACT
    existing_status = _read_json(status_path)
    attempts = _list_of_mappings(existing_status.get("non_owner_hot_write_attempts"))
    now = generated_at or datetime.now(timezone.utc).isoformat()
    attempt = {
        "generated_at": now,
        "path": str(path),
        "incoming_authority_writer": incoming_lease.get("authority_writer"),
        "incoming_authority_generation_id": incoming_lease.get("authority_generation_id"),
        "existing_authority_writer": existing_lease.get("authority_writer"),
        "existing_authority_generation_id": existing_lease.get("authority_generation_id"),
        "classification": "NON_OWNER_HOT_WRITE_ATTEMPT_QUARANTINED",
        "write_skipped": True,
    }
    attempts = [*attempts, attempt][-20:]
    status = {
        **existing_status,
        "schema_version": "track_b_broker_authority_ownership_v1",
        "generated_at": now,
        "classification": "BROKER_AUTHORITY_DUPLICATE_HOT_WRITER_DETECTED",
        "authority_writer": existing_lease.get("authority_writer"),
        "authority_generation_id": existing_lease.get("authority_generation_id"),
        "lease_authority_generation_id": existing_lease.get("authority_generation_id"),
        "duplicate_hot_writer_detected": True,
        "non_owner_hot_write_attempt_count": len(attempts),
        "non_owner_hot_write_attempts": attempts,
        "running_writer_needs_reload": True,
        "next_safe_action": "RELOAD_AUTHORITY_REFRESHER_SERVICE",
        "expected_min_commit": existing_status.get("expected_min_commit") or BROKER_AUTHORITY_EXPECTED_MIN_COMMIT,
        "read_only": True,
        "broker_mutation_allowed": False,
        "submit_attempted": False,
        "close_attempted": False,
        "cancel_attempted": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    write_json_atomic(status_path, status)
    return status


def _classification(
    *,
    aligned: bool,
    writer_aligned: bool,
    duplicate_attempt: bool,
    running_writer_needs_reload: bool,
) -> str:
    if duplicate_attempt or not writer_aligned:
        return "BROKER_AUTHORITY_DUPLICATE_HOT_WRITER_DETECTED"
    if running_writer_needs_reload:
        return "BROKER_AUTHORITY_WRITER_RELOAD_REQUIRED"
    if aligned:
        return "BROKER_AUTHORITY_PUBLISHER_HEALTHY"
    return "BROKER_AUTHORITY_PUBLISHER_UNHEALTHY"


def _next_safe_action(
    *,
    aligned: bool,
    writer_aligned: bool,
    duplicate_attempt: bool,
    running_writer_needs_reload: bool,
) -> str:
    if duplicate_attempt or running_writer_needs_reload or not writer_aligned:
        return "RELOAD_AUTHORITY_REFRESHER_SERVICE"
    if not aligned:
        return "INVESTIGATE_DUPLICATE_WRITER"
    return "NO_ACTION"


def _source_commit(repo_root: Path) -> str | None:
    env_commit = str(os.environ.get("MGC_SOURCE_COMMIT") or os.environ.get("GIT_COMMIT") or "").strip()
    if env_commit:
        return env_commit
    git_dir = repo_root / ".git"
    head_path = git_dir / "HEAD"
    try:
        head = head_path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    if head.startswith("ref:"):
        ref = head.split(":", 1)[1].strip()
        try:
            return (git_dir / ref).read_text(encoding="utf-8").strip()
        except OSError:
            return None
    return head or None


def _commit_predates(source_commit: str | None, expected_min_commit: str) -> bool:
    if not expected_min_commit or not source_commit:
        return False
    return not str(source_commit).startswith(str(expected_min_commit))


def _infer_repo_root(path: Path) -> Path:
    current = Path(path).resolve()
    for parent in [current, *current.parents]:
        if (parent / ".git").exists():
            return parent
    marker = Path("outputs/operator_dashboard/runtime/latest_broker_truth_lease.json")
    text = current.as_posix()
    suffix = marker.as_posix()
    if text.endswith(suffix):
        return Path(text[: -len(suffix)]).resolve()
    return current.parent


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list_of_mappings(value: Any) -> list[dict[str, Any]]:
    if not isinstance(value, list):
        return []
    return [dict(row) for row in value if isinstance(row, Mapping)]


__all__ = [
    "BROKER_AUTHORITY_EXPECTED_MIN_COMMIT",
    "BROKER_AUTHORITY_WRITER",
    "DEFAULT_BROKER_AUTHORITY_OWNERSHIP_ARTIFACT",
    "build_broker_authority_ownership_status",
    "load_broker_authority_ownership_status",
    "record_non_owner_hot_write_attempt",
    "write_broker_authority_ownership_status",
]
