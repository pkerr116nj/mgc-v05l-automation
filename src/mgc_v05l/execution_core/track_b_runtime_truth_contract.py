"""Shared Track B runtime truth contract primitives.

Slice 1 is intentionally pure: it defines common freshness/heartbeat/runtime
ownership language without changing launcher, readiness, broker, or lifecycle
behavior.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

SCHEMA_VERSION = "track_b_runtime_truth_contract_v1"

FRESHNESS_FRESH = "FRESH"
FRESHNESS_STALE = "STALE"
FRESHNESS_MISSING_TIMESTAMP = "MISSING_TIMESTAMP"
FRESHNESS_FUTURE_TIMESTAMP = "FUTURE_TIMESTAMP"
FRESHNESS_MISSING_ARTIFACT = "MISSING_ARTIFACT"

HEARTBEAT_HEALTHY = "HEALTHY"
HEARTBEAT_PROCESS_DOWN = "PROCESS_DOWN"
HEARTBEAT_ARTIFACT_MISSING = "ARTIFACT_MISSING"
HEARTBEAT_ARTIFACT_STALE = "ARTIFACT_STALE"
HEARTBEAT_WRONG_ROOT = "WRONG_ROOT"
HEARTBEAT_COMMAND_MISMATCH = "COMMAND_MISMATCH"
HEARTBEAT_UNKNOWN = "UNKNOWN"

WRITER_SINGLE = "SINGLE_WRITER"
WRITER_DUPLICATE = "DUPLICATE_WRITER_DETECTED"
WRITER_MISSING = "NO_ACTIVE_WRITER"

RECOVERY_OBSERVE_ONLY = "OBSERVE_ONLY"
RECOVERY_RESTART_ELIGIBLE = "RESTART_ELIGIBLE"
RECOVERY_BLOCKED = "RECOVERY_BLOCKED"
RECOVERY_OPERATOR_REQUIRED = "OPERATOR_REQUIRED"

REQUIRED_CONTRACT_FIELDS = {
    "schema_version",
    "runtime_instance_id",
    "service_name",
    "producer_pid",
    "producer_root",
    "generated_at",
    "last_success_at",
    "freshness_ttl_seconds",
    "freshness_state",
    "heartbeat_state",
    "writer_authority",
    "source_commit",
    "config_fingerprint",
    "runtime_mode",
    "restart_generation",
    "duplicate_writer_detection",
    "stale_reason",
    "recovery_state",
}


@dataclass(frozen=True)
class FreshnessResult:
    freshness_state: str
    age_seconds: float | None
    stale_reason: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "freshness_state": self.freshness_state,
            "age_seconds": self.age_seconds,
            "stale_reason": self.stale_reason,
        }


def classify_freshness(
    *,
    generated_at: str | datetime | None,
    freshness_ttl_seconds: float,
    now: datetime | None = None,
    artifact_present: bool = True,
) -> FreshnessResult:
    """Classify artifact freshness using one clock and one TTL."""

    current = _ensure_utc(now or datetime.now(timezone.utc))
    ttl = max(float(freshness_ttl_seconds), 0.0)
    if not artifact_present:
        return FreshnessResult(FRESHNESS_MISSING_ARTIFACT, None, "artifact_missing")
    timestamp = _parse_timestamp(generated_at)
    if timestamp is None:
        return FreshnessResult(FRESHNESS_MISSING_TIMESTAMP, None, "timestamp_missing")
    age_seconds = (current - timestamp).total_seconds()
    if age_seconds < -1.0:
        return FreshnessResult(FRESHNESS_FUTURE_TIMESTAMP, age_seconds, "timestamp_in_future")
    bounded_age = max(age_seconds, 0.0)
    if bounded_age > ttl:
        return FreshnessResult(FRESHNESS_STALE, bounded_age, f"age_seconds>{ttl:g}")
    return FreshnessResult(FRESHNESS_FRESH, bounded_age, None)


def classify_heartbeat(
    *,
    process_running: bool | None,
    root_ok: bool | None,
    command_ok: bool | None,
    freshness_state: str | None,
) -> str:
    """Classify heartbeat truth after process and artifact checks."""

    if root_ok is False:
        return HEARTBEAT_WRONG_ROOT
    if command_ok is False:
        return HEARTBEAT_COMMAND_MISMATCH
    if process_running is False:
        return HEARTBEAT_PROCESS_DOWN
    if freshness_state == FRESHNESS_MISSING_ARTIFACT:
        return HEARTBEAT_ARTIFACT_MISSING
    if freshness_state in {FRESHNESS_STALE, FRESHNESS_MISSING_TIMESTAMP, FRESHNESS_FUTURE_TIMESTAMP}:
        return HEARTBEAT_ARTIFACT_STALE
    if process_running is True and freshness_state == FRESHNESS_FRESH:
        return HEARTBEAT_HEALTHY
    return HEARTBEAT_UNKNOWN


def classify_writer_authority(instances: Sequence[Mapping[str, Any]]) -> str:
    """Classify whether an artifact/service has zero, one, or many live writers."""

    active = [
        row
        for row in instances
        if bool(row.get("process_running")) and str(row.get("heartbeat_state") or "") == HEARTBEAT_HEALTHY
    ]
    if not active:
        return WRITER_MISSING
    if len(active) > 1:
        return WRITER_DUPLICATE
    return WRITER_SINGLE


def build_runtime_truth_contract(
    *,
    runtime_instance_id: str,
    service_name: str,
    producer_pid: int | None,
    producer_root: str | None,
    generated_at: str | datetime,
    freshness_ttl_seconds: float,
    heartbeat_state: str,
    writer_authority: str,
    runtime_mode: str = "PAPER",
    recovery_state: str = RECOVERY_OBSERVE_ONLY,
    last_success_at: str | datetime | None = None,
    source_commit: str | None = None,
    config_fingerprint: str | None = None,
    restart_generation: int | None = None,
    duplicate_writer_detection: Mapping[str, Any] | None = None,
    stale_reason: str | None = None,
    extra: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    """Build a normalized Track B runtime truth payload."""

    freshness = classify_freshness(
        generated_at=last_success_at or generated_at,
        freshness_ttl_seconds=freshness_ttl_seconds,
        now=_parse_timestamp(generated_at) or datetime.now(timezone.utc),
    )
    payload = {
        "schema_version": SCHEMA_VERSION,
        "runtime_instance_id": runtime_instance_id,
        "service_name": service_name,
        "producer_pid": producer_pid,
        "producer_root": producer_root,
        "generated_at": _iso(generated_at),
        "last_success_at": _iso(last_success_at),
        "freshness_ttl_seconds": float(freshness_ttl_seconds),
        "freshness_state": freshness.freshness_state,
        "heartbeat_state": heartbeat_state,
        "writer_authority": writer_authority,
        "source_commit": source_commit,
        "config_fingerprint": config_fingerprint,
        "runtime_mode": runtime_mode,
        "restart_generation": restart_generation,
        "duplicate_writer_detection": dict(duplicate_writer_detection or {}),
        "stale_reason": stale_reason or freshness.stale_reason,
        "recovery_state": recovery_state,
    }
    if extra:
        payload.update(dict(extra))
    validate_runtime_truth_contract(payload)
    return payload


def validate_runtime_truth_contract(payload: Mapping[str, Any]) -> tuple[str, ...]:
    """Return validation errors for a normalized runtime truth payload."""

    errors: list[str] = []
    missing = sorted(field for field in REQUIRED_CONTRACT_FIELDS if field not in payload)
    errors.extend(f"missing:{field}" for field in missing)
    if payload.get("schema_version") != SCHEMA_VERSION:
        errors.append("schema_version_mismatch")
    if str(payload.get("runtime_mode") or "").upper() != "PAPER":
        errors.append("runtime_mode_not_paper")
    if payload.get("producer_pid") is not None and not isinstance(payload.get("producer_pid"), int):
        errors.append("producer_pid_not_int")
    if _parse_timestamp(payload.get("generated_at")) is None:
        errors.append("generated_at_invalid")
    try:
        if float(payload.get("freshness_ttl_seconds")) < 0:
            errors.append("freshness_ttl_negative")
    except (TypeError, ValueError):
        errors.append("freshness_ttl_invalid")
    if payload.get("freshness_state") not in {
        FRESHNESS_FRESH,
        FRESHNESS_STALE,
        FRESHNESS_MISSING_TIMESTAMP,
        FRESHNESS_FUTURE_TIMESTAMP,
        FRESHNESS_MISSING_ARTIFACT,
    }:
        errors.append("freshness_state_unknown")
    if payload.get("heartbeat_state") not in {
        HEARTBEAT_HEALTHY,
        HEARTBEAT_PROCESS_DOWN,
        HEARTBEAT_ARTIFACT_MISSING,
        HEARTBEAT_ARTIFACT_STALE,
        HEARTBEAT_WRONG_ROOT,
        HEARTBEAT_COMMAND_MISMATCH,
        HEARTBEAT_UNKNOWN,
    }:
        errors.append("heartbeat_state_unknown")
    if payload.get("writer_authority") not in {WRITER_SINGLE, WRITER_DUPLICATE, WRITER_MISSING}:
        errors.append("writer_authority_unknown")
    if errors:
        raise ValueError(",".join(errors))
    return ()


def _parse_timestamp(value: str | datetime | object | None) -> datetime | None:
    if isinstance(value, datetime):
        return _ensure_utc(value)
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _iso(value: str | datetime | None) -> str | None:
    parsed = _parse_timestamp(value)
    return None if parsed is None else parsed.isoformat()
