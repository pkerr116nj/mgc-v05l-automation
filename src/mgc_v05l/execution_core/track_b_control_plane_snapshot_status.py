"""Track B Control Plane Snapshot status helpers.

This module is pure classification. It does not refresh shared truth, start a
runtime, mutate broker/lifecycle state, or read dashboard projections as
authority.
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Mapping


CONTROL_PLANE_READY = "CONTROL_PLANE_READY"
CONTROL_PLANE_REQUIRED = "CONTROL_PLANE_REQUIRED"
CONTROL_PLANE_MISSING_DIAGNOSTIC_ONLY = "CONTROL_PLANE_MISSING_DIAGNOSTIC_ONLY"
CONTROL_PLANE_STALE_DIAGNOSTIC_ONLY = "CONTROL_PLANE_STALE_DIAGNOSTIC_ONLY"
CONTROL_PLANE_INCOHERENT_BLOCKED = "CONTROL_PLANE_INCOHERENT_BLOCKED"

DEFAULT_MAX_SNAPSHOT_AGE_SECONDS = 300.0


def classify_control_plane_snapshot_status(
    payload: Mapping[str, Any] | None,
    *,
    now: datetime | None = None,
    max_age_seconds: float = DEFAULT_MAX_SNAPSHOT_AGE_SECONDS,
    required_for_launch: bool = False,
) -> dict[str, Any]:
    """Classify whether a Control Plane Snapshot can be used as authority."""

    actual_now = _ensure_utc(now or datetime.now(UTC))
    snapshot = payload if isinstance(payload, Mapping) else {}
    if not snapshot:
        classification = CONTROL_PLANE_REQUIRED if required_for_launch else CONTROL_PLANE_MISSING_DIAGNOSTIC_ONLY
        return _status(
            classification,
            reason="Control Plane Snapshot is missing; scattered fallback artifacts are diagnostic only.",
            missing=True,
            required_for_launch=required_for_launch,
        )

    generated_at = _parse_time(snapshot.get("generated_at"))
    age_seconds = None
    if generated_at is not None:
        age_seconds = max(0.0, (actual_now - generated_at).total_seconds())
    if generated_at is None or age_seconds is None or age_seconds > max_age_seconds:
        return _status(
            CONTROL_PLANE_STALE_DIAGNOSTIC_ONLY,
            reason="Control Plane Snapshot is stale or missing generated_at; fallback display is diagnostic only.",
            stale=True,
            age_seconds=age_seconds,
            required_for_launch=required_for_launch,
        )

    if snapshot.get("shared_truth_coherence_status") != "COHERENT":
        return _status(
            CONTROL_PLANE_INCOHERENT_BLOCKED,
            reason="Control Plane Snapshot is not coherent; launch is blocked and component reads are diagnostic only.",
            incoherent=True,
            age_seconds=age_seconds,
            required_for_launch=required_for_launch,
        )

    ready = (
        snapshot.get("classification") == "CONTROL_PLANE_SNAPSHOT_READY"
        and snapshot.get("runtime_supervisor_classification") == "SUPERVISOR_RUNTIME_START_ALLOWED"
        and snapshot.get("supervisor_mode") == "READY_FOR_OPERATOR_START"
        and snapshot.get("safe_to_start_runtime") is True
        and not snapshot.get("blockers")
    )
    return _status(
        CONTROL_PLANE_READY,
        reason="Control Plane Snapshot is fresh and coherent.",
        age_seconds=age_seconds,
        required_for_launch=required_for_launch,
        safe_to_start_runtime=ready,
    )


def _status(
    classification: str,
    *,
    reason: str,
    missing: bool = False,
    stale: bool = False,
    incoherent: bool = False,
    age_seconds: float | None = None,
    required_for_launch: bool = False,
    safe_to_start_runtime: bool = False,
) -> dict[str, Any]:
    diagnostic_only = classification != CONTROL_PLANE_READY
    return {
        "classification": classification,
        "reason": reason,
        "required_for_launch": required_for_launch,
        "diagnostic_only": diagnostic_only,
        "not_routing_authority": True,
        "control_plane_snapshot_missing": missing,
        "control_plane_snapshot_stale": stale,
        "control_plane_snapshot_incoherent": incoherent,
        "control_plane_snapshot_age_seconds": age_seconds,
        "control_plane_snapshot_missing_or_stale": missing or stale,
        "control_plane_snapshot_blocked": classification != CONTROL_PLANE_READY or not safe_to_start_runtime,
        "safe_to_start_runtime": bool(safe_to_start_runtime and classification == CONTROL_PLANE_READY),
    }


def _parse_time(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    if text.endswith("Z"):
        text = f"{text[:-1]}+00:00"
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
