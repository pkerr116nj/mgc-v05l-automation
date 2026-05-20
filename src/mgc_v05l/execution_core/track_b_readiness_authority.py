"""Shared Track B PAPER readiness authority for submit/precheck gates.

Dashboard/operator snapshots are presentation artifacts. This module builds a
small fail-closed contract from canonical/root Track B truth so callers do not
accidentally treat stale presentation snapshots as submit authority.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_readiness_state import (
    DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT,
    DEFAULT_CANONICAL_READINESS_ARTIFACT,
    DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT,
)

CANONICAL_READINESS_SUMMARY_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness_summary.json"
)
PHASE1_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
CANONICAL_READINESS_FRESHNESS_SECONDS = 180.0
CANONICAL_SUMMARY_SIBLING_TOLERANCE_SECONDS = 5.0
READY_SUBMIT_CAPABLE = "READY_SUBMIT_CAPABLE"
RECONCILED_CLASSIFICATION = "TRACK_B_PAPER_BROKER_RECONCILED"
READY_BROKER_LEASE_STATES = {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"}


def build_track_b_readiness_authority(
    *,
    repo_root: Path,
    expected_root: Path | None = None,
    now: datetime | None = None,
    freshness_window_seconds: float = CANONICAL_READINESS_FRESHNESS_SECONDS,
    safety: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Return the generalized PAPER readiness authority decision.

    The decision is intentionally based on canonical/root artifacts only:
    canonical readiness, runtime root guard, broker truth lease, broker
    reconciliation, and the PAPER live-money guard. Presentation snapshots may
    appear elsewhere as diagnostics, but never make this authority ready.
    """

    repo_root = repo_root.expanduser().resolve()
    expected_root = (expected_root or DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT).expanduser().resolve()
    current = _ensure_utc(now or datetime.now(timezone.utc))
    safety = _mapping(safety)

    canonical_path = repo_root / DEFAULT_CANONICAL_READINESS_ARTIFACT
    summary_path = repo_root / CANONICAL_READINESS_SUMMARY_ARTIFACT
    broker_lease_path = repo_root / DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT
    reconciliation_path = repo_root / PHASE1_RECONCILIATION_ARTIFACT

    canonical = _read_json(canonical_path)
    summary = _read_json(summary_path)
    broker_lease_artifact = _read_json(broker_lease_path)
    reconciliation_artifact = _read_json(reconciliation_path)

    artifacts = {
        "canonical_readiness": _artifact_status(
            canonical_path,
            canonical,
            now=current,
            freshness_window_seconds=freshness_window_seconds,
            allow_mtime_fallback=False,
            required=True,
        ),
        "canonical_readiness_summary": _artifact_status(
            summary_path,
            summary,
            now=current,
            freshness_window_seconds=freshness_window_seconds,
            allow_mtime_fallback=True,
            required=False,
        ),
        "broker_truth_lease": _artifact_status(
            broker_lease_path,
            broker_lease_artifact,
            now=current,
            freshness_window_seconds=freshness_window_seconds,
            allow_mtime_fallback=False,
            required=False,
        ),
        "phase1_reconciliation": _artifact_status(
            reconciliation_path,
            reconciliation_artifact,
            now=current,
            freshness_window_seconds=freshness_window_seconds,
            allow_mtime_fallback=False,
            required=False,
        ),
    }

    blockers: list[str] = []
    warnings: list[str] = []

    canonical_present = bool(artifacts["canonical_readiness"]["present"])
    if not canonical_present:
        blockers.append("canonical_readiness_missing")
    elif not artifacts["canonical_readiness"]["fresh"]:
        blockers.append("canonical_readiness_stale")

    canonical_state = str(canonical.get("canonical_readiness") or canonical.get("state") or "").strip().upper()
    if canonical_present and canonical_state != READY_SUBMIT_CAPABLE:
        blockers.append("canonical_readiness_not_submit_capable")

    summary_state = str(
        summary.get("classification") or summary.get("canonical_readiness") or summary.get("state") or ""
    ).strip().upper()
    summary_fresh = bool(artifacts["canonical_readiness_summary"].get("fresh"))
    full_summary_sibling = _artifact_sibling_generated(
        artifacts["canonical_readiness"],
        artifacts["canonical_readiness_summary"],
    )
    summary_agrees = bool(summary_fresh and full_summary_sibling and summary_state == canonical_state)
    if summary:
        if not summary_fresh:
            warnings.append("canonical_summary_stale_diagnostic_only")
        elif not full_summary_sibling:
            warnings.append("canonical_summary_not_generated_with_full_artifact")
        elif summary_state != canonical_state:
            blockers.append("canonical_summary_disagrees_with_full")

    root_guard = _mapping(canonical.get("root_guard_summary"))
    runtime_process: dict[str, Any] = {}
    runtime_running = False
    runtime_root_match = False
    detected_runtime_root = None
    runtime: dict[str, Any] = {}
    if canonical_present:
        if root_guard.get("root_match") is not True:
            blockers.append("root_guard_not_matched")
        runtime_process = _runtime_process(root_guard)
        runtime_running = bool(runtime_process.get("running"))
        runtime_root_match = runtime_process.get("root_match") is True
        detected_runtime_root = (
            runtime_process.get("detected_root") or runtime_process.get("cwd") or runtime_process.get("artifact_root")
        )
        if detected_runtime_root and Path(str(detected_runtime_root)).expanduser() != expected_root:
            runtime_root_match = False
        if not runtime_running:
            blockers.append("runtime_not_running")
        if not runtime_root_match:
            blockers.append("runtime_not_from_expected_root")

        runtime = _mapping(canonical.get("runtime"))
        if runtime.get("healthy") is not True:
            blockers.append("runtime_not_healthy")
        if runtime.get("runtime_ingestion_fresh") is not True:
            blockers.append("runtime_ingestion_not_fresh")

    broker_lease = _mapping(canonical.get("broker_truth_lease")) or _mapping(broker_lease_artifact)
    broker_lease_state = str(broker_lease.get("lease_state") or broker_lease.get("state") or "").strip().upper()
    if broker_lease:
        if broker_lease_state not in READY_BROKER_LEASE_STATES:
            blockers.append("broker_truth_lease_not_active")
        if broker_lease.get("submit_entry_allowed") is False:
            blockers.append("broker_truth_lease_entry_not_allowed")
        if broker_lease.get("operator_action_required") is True:
            blockers.append("broker_truth_lease_operator_action_required")
    elif canonical_present:
        blockers.append("broker_truth_lease_not_active")

    reconciliation = _mapping(canonical.get("phase1_reconciliation")) or _mapping(reconciliation_artifact)
    reconciliation_state = str(reconciliation.get("classification") or "").strip().upper()
    if reconciliation:
        reconciliation_clean = bool(
            reconciliation_state == RECONCILED_CLASSIFICATION
            and reconciliation.get("broker_reconciled") is True
            and int(reconciliation.get("review_required_count") or 0) == 0
            and int(reconciliation.get("lifecycle_open_position_count") or 0) == 0
            and int(reconciliation.get("track_b_broker_open_order_count") or 0) == 0
        )
        if not reconciliation_clean:
            blockers.append("broker_reconciliation_not_clean")
    elif canonical_present:
        blockers.append("broker_reconciliation_not_clean")

    live_money_eligible = any(
        bool(source.get("live_money_eligible") is True)
        for source in (canonical, broker_lease, reconciliation, safety)
    )
    if live_money_eligible:
        blockers.append("live_money_eligible_true")

    if safety:
        if safety.get("broker_reconciled") is False or safety.get("classification") != RECONCILED_CLASSIFICATION:
            blockers.append("broker_lifecycle_not_reconciled")
        if int(safety.get("open_order_count") or 0):
            blockers.append("open_orders_present")
        if int(safety.get("review_required_count") or 0):
            blockers.append("review_required_nonzero")
        if safety.get("runtime_pid_active") is False:
            blockers.append("runtime_pid_not_active")
        if safety.get("runtime_from_dev_root") is False:
            blockers.append("runtime_not_verified_from_dev_root")
        if safety.get("paper_proof_invoked") is True:
            blockers.append("paper_proof_invoked_true")

    blockers = _dedupe(blockers)
    warnings = _dedupe(warnings)
    ready = not blockers
    return {
        "schema_version": "track_b_readiness_authority_v1",
        "generated_at": current.isoformat(),
        "classification": "TRACK_B_READINESS_AUTHORITY_READY" if ready else "TRACK_B_READINESS_AUTHORITY_BLOCKED",
        "ready": ready,
        "blockers": tuple(blockers),
        "warnings": tuple(warnings),
        "source": "canonical_track_b_runtime_readiness",
        "presentation_snapshots_authority": "DIAGNOSTIC_ONLY",
        "canonical_readiness": canonical_state or None,
        "canonical_readiness_fresh": bool(artifacts["canonical_readiness"]["fresh"]),
        "canonical_summary_fresh": summary_fresh,
        "canonical_summary_agrees": summary_agrees,
        "canonical_summary_generated_with_full": full_summary_sibling,
        "paper_trade_allowed": ready and canonical_state == READY_SUBMIT_CAPABLE,
        "paper_runtime_ready": bool(
            runtime_running and runtime.get("healthy") is True and runtime.get("runtime_ingestion_fresh") is True
        ),
        "runtime_running": runtime_running,
        "runtime_pid": runtime_process.get("pid"),
        "runtime_root": detected_runtime_root,
        "runtime_from_expected_root": runtime_root_match,
        "broker_truth_lease_state": broker_lease_state or None,
        "reconciliation_state": reconciliation_state or None,
        "live_money_eligible": live_money_eligible,
        "artifacts": artifacts,
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _artifact_status(
    path: Path,
    payload: Mapping[str, Any],
    *,
    now: datetime,
    freshness_window_seconds: float,
    allow_mtime_fallback: bool,
    required: bool,
) -> dict[str, Any]:
    generated_at = _parse_datetime(payload.get("generated_at")) if payload else None
    timestamp_source = "generated_at" if generated_at is not None else None
    if generated_at is None and payload and allow_mtime_fallback:
        try:
            generated_at = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
            timestamp_source = "mtime"
        except OSError:
            generated_at = None
    age_seconds = (
        None if generated_at is None else max(0.0, (now - generated_at.astimezone(timezone.utc)).total_seconds())
    )
    fresh = bool(payload and age_seconds is not None and age_seconds <= freshness_window_seconds)
    return {
        "label": path.stem,
        "path": str(path),
        "present": bool(payload),
        "required": required,
        "generated_at": generated_at.isoformat() if generated_at is not None else None,
        "timestamp_source": timestamp_source,
        "age_seconds": age_seconds,
        "freshness_window_seconds": freshness_window_seconds,
        "fresh": fresh,
    }


def _artifact_sibling_generated(full: Mapping[str, Any], summary: Mapping[str, Any]) -> bool:
    full_ts = _parse_datetime(full.get("generated_at"))
    summary_ts = _parse_datetime(summary.get("generated_at"))
    if full_ts is None or summary_ts is None:
        return False
    return abs((full_ts - summary_ts).total_seconds()) <= CANONICAL_SUMMARY_SIBLING_TOLERANCE_SECONDS


def _runtime_process(root_guard: Mapping[str, Any]) -> dict[str, Any]:
    for row in list(root_guard.get("processes") or []):
        if isinstance(row, Mapping) and row.get("name") == "paper_runtime":
            return dict(row)
    return {}


def _parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _dedupe(values: list[str]) -> list[str]:
    return list(dict.fromkeys(value for value in values if value))
