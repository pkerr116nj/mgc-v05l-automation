"""Fresh Truth Contract helpers for Track B authority artifacts.

Authority consumers should not make submit, close, ownership, recovery, or
readiness decisions from stale rows.  Stale rows can remain visible, but only as
diagnostic/full-audit evidence.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from typing import Any, Callable, Iterable, Mapping, Sequence, TypeVar


FRESH_AUTHORITY = "FRESH_AUTHORITY"
EXPIRED_DIAGNOSTIC_ONLY = "EXPIRED_DIAGNOSTIC_ONLY"
FRESHNESS_UNKNOWN_FAIL_CLOSED = "FRESHNESS_UNKNOWN_FAIL_CLOSED"
RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE = "RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE"

DEFAULT_AUTHORITY_TTL_SECONDS = 180


@dataclass(frozen=True)
class FreshTruthDecision:
    classification: str
    fresh: bool
    reason_codes: tuple[str, ...]
    generated_at: str | None = None
    observed_at: str | None = None
    source_pid: int | None = None
    source_started_at: str | None = None
    runtime_epoch: str | None = None
    sequence_id: str | None = None
    ttl_seconds: float | None = None
    fresh_until: str | None = None
    authority_scope: str | None = None
    age_seconds: float | None = None

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "fresh": self.fresh,
            "reason_codes": list(self.reason_codes),
            "generated_at": self.generated_at,
            "observed_at": self.observed_at,
            "source_pid": self.source_pid,
            "source_started_at": self.source_started_at,
            "runtime_epoch": self.runtime_epoch,
            "sequence_id": self.sequence_id,
            "ttl_seconds": self.ttl_seconds,
            "fresh_until": self.fresh_until,
            "authority_scope": self.authority_scope,
            "age_seconds": self.age_seconds,
        }


def build_authority_freshness_metadata(
    *,
    generated_at: datetime,
    observed_at: datetime | None = None,
    source_pid: int | None = None,
    source_started_at: str | None = None,
    runtime_epoch: str | None = None,
    sequence_id: str | None = None,
    ttl_seconds: float = DEFAULT_AUTHORITY_TTL_SECONDS,
    authority_scope: str,
) -> dict[str, Any]:
    generated = _ensure_utc(generated_at)
    observed = _ensure_utc(observed_at) if observed_at is not None else generated
    fresh_until = generated + timedelta(seconds=float(ttl_seconds))
    return {
        "generated_at": generated.isoformat(),
        "observed_at": observed.isoformat(),
        "source_pid": source_pid,
        "source_started_at": source_started_at,
        "runtime_epoch": runtime_epoch,
        "sequence_id": sequence_id,
        "ttl_seconds": float(ttl_seconds),
        "fresh_until": fresh_until.isoformat(),
        "authority_scope": authority_scope,
    }


def validate_authority_freshness(
    payload: Mapping[str, Any],
    *,
    now: datetime,
    default_ttl_seconds: float = DEFAULT_AUTHORITY_TTL_SECONDS,
    authority_scope: str | None = None,
    pid_running: Callable[[int], bool] | None = None,
    stale_classification: str = EXPIRED_DIAGNOSTIC_ONLY,
) -> FreshTruthDecision:
    actual_now = _ensure_utc(now)
    generated_text = _first_nonempty(payload.get("generated_at"), payload.get("observed_at"))
    observed_text = _first_nonempty(payload.get("observed_at"), payload.get("generated_at"))
    generated_at = _parse_datetime(generated_text)
    observed_at = _parse_datetime(observed_text)
    ttl = _float(payload.get("ttl_seconds"), _float(payload.get("freshness_ttl_seconds"), default_ttl_seconds))
    scope = str(payload.get("authority_scope") or authority_scope or "").strip() or None
    fresh_until_dt = _parse_datetime(payload.get("fresh_until"))
    if fresh_until_dt is None and generated_at is not None:
        fresh_until_dt = generated_at + timedelta(seconds=ttl)
    source_pid = _int_or_none(payload.get("source_pid") or payload.get("producer_pid"))
    reason_codes: list[str] = []
    if generated_at is None:
        reason_codes.append("MISSING_GENERATED_AT")
    if generated_at is not None and fresh_until_dt is not None and actual_now > fresh_until_dt:
        reason_codes.append("AUTHORITY_TTL_EXPIRED")
    if pid_running is not None and source_pid is not None and not pid_running(source_pid):
        reason_codes.append("SOURCE_PID_ABSENT")
    if reason_codes:
        return FreshTruthDecision(
            classification=stale_classification,
            fresh=False,
            reason_codes=tuple(reason_codes),
            generated_at=generated_text,
            observed_at=observed_text,
            source_pid=source_pid,
            source_started_at=_str_or_none(payload.get("source_started_at")),
            runtime_epoch=_str_or_none(payload.get("runtime_epoch")),
            sequence_id=_str_or_none(payload.get("sequence_id")),
            ttl_seconds=ttl,
            fresh_until=fresh_until_dt.isoformat() if fresh_until_dt is not None else None,
            authority_scope=scope,
            age_seconds=_age_seconds(generated_at, actual_now),
        )
    return FreshTruthDecision(
        classification=FRESH_AUTHORITY,
        fresh=True,
        reason_codes=("FRESH_AUTHORITY_METADATA_VALID",),
        generated_at=generated_text,
        observed_at=observed_text,
        source_pid=source_pid,
        source_started_at=_str_or_none(payload.get("source_started_at")),
        runtime_epoch=_str_or_none(payload.get("runtime_epoch")),
        sequence_id=_str_or_none(payload.get("sequence_id")),
        ttl_seconds=ttl,
        fresh_until=fresh_until_dt.isoformat() if fresh_until_dt is not None else None,
        authority_scope=scope,
        age_seconds=_age_seconds(generated_at, actual_now),
    )


T = TypeVar("T")


def expire_superseded_same_scope_candidates(
    candidates: Sequence[T],
    *,
    scope_key: Callable[[T], str],
    observed_at: Callable[[T], datetime | None],
    stale_reason: str = "SUPERSEDED_BY_FRESHER_SAME_SCOPE_AUTHORITY",
) -> tuple[list[T], list[dict[str, Any]]]:
    """Keep the newest candidate per scope and mark older candidates diagnostic.

    This is not an age-based timeout.  It is the Fresh Truth Contract for
    competing current-owner claims: when a newer exact same-contract authority
    exists, older same-scope claims may remain in audit, but they must not
    participate in ambiguity resolution.
    """

    grouped: dict[str, list[T]] = {}
    for candidate in candidates:
        key = str(scope_key(candidate) or "").strip()
        if not key:
            key = "__missing_scope__"
        grouped.setdefault(key, []).append(candidate)
    active: list[T] = []
    expired: list[dict[str, Any]] = []
    for key, rows in grouped.items():
        if len(rows) <= 1:
            active.extend(rows)
            continue
        ranked = sorted(rows, key=lambda item: observed_at(item) or datetime.min.replace(tzinfo=UTC), reverse=True)
        newest_time = observed_at(ranked[0])
        if newest_time is None:
            active.extend(rows)
            continue
        newest = [item for item in ranked if observed_at(item) == newest_time]
        if len(newest) != 1:
            active.extend(rows)
            continue
        winner = newest[0]
        active.append(winner)
        for item in ranked:
            if item is winner:
                continue
            expired.append(
                {
                    "classification": EXPIRED_DIAGNOSTIC_ONLY,
                    "reason_codes": [stale_reason],
                    "authority_scope": key,
                    "winner_observed_at": newest_time.isoformat(),
                    "candidate_observed_at": (
                        observed_at(item).isoformat() if observed_at(item) is not None else None
                    ),
                    "candidate": _candidate_display(item),
                }
            )
    return active, expired


def annotate_expired_diagnostic_only(
    row: Mapping[str, Any],
    *,
    decision: FreshTruthDecision,
) -> dict[str, Any]:
    return {
        **dict(row),
        "classification": EXPIRED_DIAGNOSTIC_ONLY,
        "fresh_truth_decision": decision.to_dict(),
        "current_scope_authority": False,
        "diagnostic_only": True,
    }


def _candidate_display(candidate: object) -> dict[str, Any]:
    display: dict[str, Any] = {}
    for attr in ("trade_id", "current_state", "broker_backed_entry", "broker_backed_exit", "open_qty"):
        if hasattr(candidate, attr):
            value = getattr(candidate, attr)
            display[attr] = str(value) if attr == "open_qty" else value
    owner = getattr(candidate, "ownership_identity", None)
    if owner is not None:
        for attr in ("lifecycle_id", "account_id", "local_symbol", "con_id", "side", "qty"):
            value = getattr(owner, attr, None)
            display[attr] = str(value) if attr == "qty" else value
    return display


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _parse_datetime(value: object) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _age_seconds(generated_at: datetime | None, now: datetime) -> float | None:
    if generated_at is None:
        return None
    return max(0.0, (_ensure_utc(now) - _ensure_utc(generated_at)).total_seconds())


def _float(value: object, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _int_or_none(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _first_nonempty(*values: object) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _str_or_none(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None
