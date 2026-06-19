"""Track B PAPER recovery budget ledger authority.

The recovery budget ledger is read-only accounting for future autonomous PAPER
recovery executors. It records attempts and computes per-agent/action/target
budget state, but it never starts processes or mutates broker, order, lifecycle,
or runtime state.
"""

from __future__ import annotations

import argparse
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.bounded_jsonl import append_bounded_jsonl
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


RECOVERY_BUDGET_AVAILABLE = "RECOVERY_BUDGET_AVAILABLE"
RECOVERY_BUDGET_EXHAUSTED = "RECOVERY_BUDGET_EXHAUSTED"
RECOVERY_BUDGET_UNKNOWN = "RECOVERY_BUDGET_UNKNOWN"

BUDGET_ATTEMPT_RESERVED = "BUDGET_ATTEMPT_RESERVED"
BUDGET_ATTEMPT_RELEASED = "BUDGET_ATTEMPT_RELEASED"
BUDGET_ATTEMPT_CONSUMED_SUCCESS = "BUDGET_ATTEMPT_CONSUMED_SUCCESS"
BUDGET_ATTEMPT_CONSUMED_FAILURE = "BUDGET_ATTEMPT_CONSUMED_FAILURE"
BUDGET_ATTEMPT_EXPIRED = "BUDGET_ATTEMPT_EXPIRED"
RECOVERY_ATTEMPT_RECORDED = "RECOVERY_ATTEMPT_RECORDED"

BUDGET_EVENT_VALID = "BUDGET_EVENT_VALID"
BUDGET_EVENT_INVALID_MISSING_SNAPSHOT = "BUDGET_EVENT_INVALID_MISSING_SNAPSHOT"
BUDGET_EVENT_INVALID_DUPLICATE_ACTIVE_RESERVATION = "BUDGET_EVENT_INVALID_DUPLICATE_ACTIVE_RESERVATION"
BUDGET_EVENT_INVALID_MISSING_RESERVATION = "BUDGET_EVENT_INVALID_MISSING_RESERVATION"
BUDGET_EVENT_INVALID_SCHEMA = "BUDGET_EVENT_INVALID_SCHEMA"

DEFAULT_AGENT_ID = "track_b_paper_runtime"
DEFAULT_ACTION_TYPE = "RUNTIME_RETRY"
DEFAULT_FAILURE_CLASSIFICATION = "runtime_retry"

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "recovery_budget" / "latest_recovery_budget_ledger.json"
)
DEFAULT_RECOVERY_BUDGET_EVENTS = (
    Path("outputs") / "track_b_execution_core" / "recovery_budget" / "recovery_budget_events.jsonl"
)


@dataclass(frozen=True)
class TrackBRecoveryBudgetLedgerConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT
    event_log_path: Path = DEFAULT_RECOVERY_BUDGET_EVENTS
    budget_window_seconds: int = 3600
    max_attempts_per_target: int = 2
    cooldown_seconds: int = 900

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_recovery_budget_ledger(
    *,
    config: TrackBRecoveryBudgetLedgerConfig,
    now: datetime | None = None,
    events: Sequence[Mapping[str, Any]] | None = None,
    budget_requests: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    rows = [dict(row) for row in (events if events is not None else _read_jsonl(config.resolve(config.event_log_path)))]
    requests = [dict(row) for row in (budget_requests or [_default_budget_request()])]
    window_start = actual_now - timedelta(seconds=config.budget_window_seconds)
    keyed_events = _group_events(events=rows, window_start=window_start)
    requested_keys = {_budget_key_from_request(request) for request in requests}
    keys = sorted(set(keyed_events) | requested_keys)
    entries = [
        _entry_for_key(
            key=key,
            events=keyed_events.get(key, []),
            request=_request_for_key(key=key, requests=requests),
            config=config,
            now=actual_now,
        )
        for key in keys
    ]
    exhausted = [entry for entry in entries if entry["budget_exhausted"] is True]
    classification = RECOVERY_BUDGET_EXHAUSTED if exhausted else RECOVERY_BUDGET_AVAILABLE
    return {
        "schema_version": "track_b_recovery_budget_ledger_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "accounting_only": True,
        "submit_authority": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "classification": classification,
        "budget_window_seconds": config.budget_window_seconds,
        "max_attempts_per_target": config.max_attempts_per_target,
        "cooldown_seconds": config.cooldown_seconds,
        "budget_exhausted": bool(exhausted),
        "quarantine_required": bool(exhausted),
        "entries": entries,
        "summary": {
            "entry_count": len(entries),
            "exhausted_entry_count": len(exhausted),
            "budget_exhausted": bool(exhausted),
            "quarantine_required": bool(exhausted),
            "minimum_attempts_remaining": min((int(entry["attempts_remaining"]) for entry in entries), default=config.max_attempts_per_target),
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
        },
        "prohibited_actions": [
            "runtime_restart",
            "broker_mutation",
            "order_submit",
            "order_cancel",
            "order_replace",
            "order_modify",
            "lifecycle_mutation",
            "dashboard_projection_authority",
            "live_money_route",
            "broad_cancel",
            "broad_flatten",
        ],
    }


def write_track_b_recovery_budget_ledger(
    *,
    config: TrackBRecoveryBudgetLedgerConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    _write_json_atomic(output_path, dict(payload))
    return output_path


def append_recovery_budget_event(
    *,
    config: TrackBRecoveryBudgetLedgerConfig,
    event: Mapping[str, Any],
) -> Path:
    path = config.resolve(config.event_log_path)
    append_bounded_jsonl(path, dict(event))
    return path


def build_recovery_budget_event(
    *,
    agent_id: str = DEFAULT_AGENT_ID,
    action_type: str = DEFAULT_ACTION_TYPE,
    target_identity: Mapping[str, Any] | None = None,
    runtime_generation_id: str | None = None,
    stop_reason: str | None = None,
    failure_classification: str | None = None,
    attempt_counted: bool = True,
    dry_run: bool = False,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    actual_time = _ensure_utc(occurred_at or datetime.now(UTC))
    normalized_identity = _normalize_identity(target_identity or {})
    return {
        "schema_version": "track_b_recovery_budget_event_v1",
        "event_type": RECOVERY_ATTEMPT_RECORDED,
        "generated_at": actual_time.isoformat(),
        "occurred_at": actual_time.isoformat(),
        "mode": "PAPER",
        "read_only_accounting": True,
        "dry_run": bool(dry_run),
        "attempt_counted": bool(attempt_counted),
        "agent_id": str(agent_id or DEFAULT_AGENT_ID),
        "action_type": str(action_type or DEFAULT_ACTION_TYPE),
        "target_identity": normalized_identity,
        "target_identity_hash": target_identity_hash(normalized_identity),
        "runtime_generation_id": runtime_generation_id,
        "stop_reason": stop_reason,
        "failure_classification": failure_classification or stop_reason or DEFAULT_FAILURE_CLASSIFICATION,
        "broker_mutation": False,
        "runtime_restart_executed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def build_budget_reservation_event(
    *,
    recovery_attempt_id: str,
    control_plane_snapshot_id: str,
    shared_truth_generation_id: str,
    action_type: str = DEFAULT_ACTION_TYPE,
    budget_key: str | None = None,
    agent_id: str = DEFAULT_AGENT_ID,
    target_identity: Mapping[str, Any] | None = None,
    runtime_generation_id: str | None = None,
    created_at: datetime | None = None,
) -> dict[str, Any]:
    actual_time = _ensure_utc(created_at or datetime.now(UTC))
    normalized_identity = _normalize_identity(target_identity or {})
    resolved_budget_key = budget_key or _public_budget_key(
        agent_id=agent_id,
        action_type=action_type,
        target_identity=normalized_identity,
        runtime_generation_id=runtime_generation_id,
    )
    return {
        "schema_version": "track_b_recovery_budget_event_v2",
        "event_type": BUDGET_ATTEMPT_RESERVED,
        "reservation_id": f"budget-reservation-{_safe_slug(recovery_attempt_id)}",
        "recovery_attempt_id": recovery_attempt_id,
        "control_plane_snapshot_id": control_plane_snapshot_id,
        "shared_truth_generation_id": shared_truth_generation_id,
        "agent_id": str(agent_id or DEFAULT_AGENT_ID),
        "action_type": str(action_type or DEFAULT_ACTION_TYPE),
        "budget_key": resolved_budget_key,
        "target_identity": normalized_identity,
        "target_identity_hash": target_identity_hash(normalized_identity),
        "runtime_generation_id": runtime_generation_id,
        "created_at": actual_time.isoformat(),
        "generated_at": actual_time.isoformat(),
        "occurred_at": actual_time.isoformat(),
        "attempt_counted": True,
        "dry_run": False,
        "apply_enabled_required": True,
        "read_only_accounting": True,
        "broker_mutation": False,
        "runtime_restart_executed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def build_budget_release_event(
    *,
    reservation_id: str,
    recovery_attempt_id: str,
    released_at: datetime | None = None,
    reason: str = "reservation_released_before_apply",
) -> dict[str, Any]:
    actual_time = _ensure_utc(released_at or datetime.now(UTC))
    return {
        "schema_version": "track_b_recovery_budget_event_v2",
        "event_type": BUDGET_ATTEMPT_RELEASED,
        "reservation_id": reservation_id,
        "recovery_attempt_id": recovery_attempt_id,
        "reason": reason,
        "generated_at": actual_time.isoformat(),
        "occurred_at": actual_time.isoformat(),
        "attempt_counted": False,
        "read_only_accounting": True,
        "broker_mutation": False,
        "runtime_restart_executed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def build_budget_consumed_event(
    *,
    reservation_id: str,
    recovery_attempt_id: str,
    success: bool,
    consumed_at: datetime | None = None,
    failure_classification: str | None = None,
    stop_reason: str | None = None,
) -> dict[str, Any]:
    actual_time = _ensure_utc(consumed_at or datetime.now(UTC))
    return {
        "schema_version": "track_b_recovery_budget_event_v2",
        "event_type": BUDGET_ATTEMPT_CONSUMED_SUCCESS if success else BUDGET_ATTEMPT_CONSUMED_FAILURE,
        "reservation_id": reservation_id,
        "recovery_attempt_id": recovery_attempt_id,
        "success": bool(success),
        "failure_classification": None if success else failure_classification,
        "stop_reason": None if success else stop_reason,
        "generated_at": actual_time.isoformat(),
        "occurred_at": actual_time.isoformat(),
        "attempt_counted": True,
        "read_only_accounting": True,
        "broker_mutation": False,
        "runtime_restart_executed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
    }


def build_budget_expired_event(
    *,
    reservation_id: str,
    recovery_attempt_id: str,
    expired_at: datetime | None = None,
    reason: str = "reservation_expired_before_apply",
) -> dict[str, Any]:
    event = build_budget_release_event(
        reservation_id=reservation_id,
        recovery_attempt_id=recovery_attempt_id,
        released_at=expired_at,
        reason=reason,
    )
    event["event_type"] = BUDGET_ATTEMPT_EXPIRED
    return event


def validate_budget_event(
    event: Mapping[str, Any],
    *,
    existing_events: Sequence[Mapping[str, Any]] | None = None,
) -> dict[str, Any]:
    event_type = str(event.get("event_type") or "")
    existing = list(existing_events or [])
    if event_type not in {
        RECOVERY_ATTEMPT_RECORDED,
        BUDGET_ATTEMPT_RESERVED,
        BUDGET_ATTEMPT_RELEASED,
        BUDGET_ATTEMPT_CONSUMED_SUCCESS,
        BUDGET_ATTEMPT_CONSUMED_FAILURE,
        BUDGET_ATTEMPT_EXPIRED,
    }:
        return _validation(BUDGET_EVENT_INVALID_SCHEMA, "Unknown recovery budget event type.")
    if event_type == BUDGET_ATTEMPT_RESERVED:
        missing = [
            field
            for field in (
                "recovery_attempt_id",
                "control_plane_snapshot_id",
                "shared_truth_generation_id",
                "action_type",
                "budget_key",
                "agent_id",
                "created_at",
            )
            if not event.get(field)
        ]
        if missing:
            return _validation(BUDGET_EVENT_INVALID_MISSING_SNAPSHOT, f"Reservation missing fields: {', '.join(missing)}.")
        if _active_reservation(event.get("recovery_attempt_id"), existing) is not None:
            return _validation(
                BUDGET_EVENT_INVALID_DUPLICATE_ACTIVE_RESERVATION,
                "Duplicate active reservation for recovery_attempt_id.",
            )
        return _validation(BUDGET_EVENT_VALID, "Budget reservation event is valid.")
    if event_type in {
        BUDGET_ATTEMPT_CONSUMED_SUCCESS,
        BUDGET_ATTEMPT_CONSUMED_FAILURE,
        BUDGET_ATTEMPT_RELEASED,
        BUDGET_ATTEMPT_EXPIRED,
    }:
        if not event.get("reservation_id") or not event.get("recovery_attempt_id"):
            return _validation(BUDGET_EVENT_INVALID_SCHEMA, "Reservation-linked event is missing reservation identity.")
        if _reservation_by_id(event.get("reservation_id"), existing) is None:
            return _validation(BUDGET_EVENT_INVALID_MISSING_RESERVATION, "Reservation-linked event has no prior reservation.")
    return _validation(BUDGET_EVENT_VALID, "Recovery budget event is valid.")


def target_identity_hash(identity: Mapping[str, Any] | None) -> str:
    normalized = _normalize_identity(identity or {})
    encoded = json.dumps(normalized, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(encoded.encode("utf-8")).hexdigest()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write Track B PAPER recovery budget ledger authority.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT)
    parser.add_argument("--event-log-path", type=Path, default=DEFAULT_RECOVERY_BUDGET_EVENTS)
    parser.add_argument("--budget-window-seconds", type=int, default=3600)
    parser.add_argument("--max-attempts-per-target", type=int, default=2)
    parser.add_argument("--cooldown-seconds", type=int, default=900)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBRecoveryBudgetLedgerConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
        event_log_path=Path(args.event_log_path),
        budget_window_seconds=int(args.budget_window_seconds),
        max_attempts_per_target=int(args.max_attempts_per_target),
        cooldown_seconds=int(args.cooldown_seconds),
    )
    payload = build_track_b_recovery_budget_ledger(config=config)
    authority_path = write_track_b_recovery_budget_ledger(config=config, payload=payload)
    if bool(args.json):
        print(json.dumps(payload, indent=2, sort_keys=True))
    else:
        print(
            json.dumps(
                {
                    "classification": payload.get("classification"),
                    "budget_exhausted": payload.get("budget_exhausted"),
                    "quarantine_required": payload.get("quarantine_required"),
                    "authority_path": str(authority_path),
                    "read_only": True,
                    "broker_mutation": False,
                    "runtime_restart_authority": False,
                },
                indent=2,
                sort_keys=True,
            )
        )
    return 0


def budget_summary_for_action(
    ledger: Mapping[str, Any],
    *,
    agent_id: str = DEFAULT_AGENT_ID,
    action_type: str = DEFAULT_ACTION_TYPE,
    target_identity: Mapping[str, Any] | None = None,
    failure_classification: str | None = None,
) -> dict[str, Any]:
    request = {
        "agent_id": agent_id,
        "action_type": action_type,
        "target_identity": _normalize_identity(target_identity or {}),
        "failure_classification": failure_classification or DEFAULT_FAILURE_CLASSIFICATION,
    }
    key = _budget_key_from_request(request)
    for entry in _list(ledger.get("entries")):
        if _mapping(entry).get("budget_key") == key:
            return dict(_mapping(entry))
    return {
        "budget_key": key,
        "agent_id": agent_id,
        "action_type": action_type,
        "target_identity_hash": request["target_identity_hash"] if "target_identity_hash" in request else target_identity_hash(request["target_identity"]),
        "attempts_used": 0,
        "attempts_remaining": int(ledger.get("max_attempts_per_target") or 0),
        "budget_exhausted": False,
        "quarantine_required": False,
    }


def _default_budget_request() -> dict[str, Any]:
    return {
        "agent_id": DEFAULT_AGENT_ID,
        "action_type": DEFAULT_ACTION_TYPE,
        "target_identity": {},
        "failure_classification": DEFAULT_FAILURE_CLASSIFICATION,
    }


def _group_events(*, events: Sequence[Mapping[str, Any]], window_start: datetime) -> dict[str, list[dict[str, Any]]]:
    grouped: dict[str, list[dict[str, Any]]] = {}
    for event in _budget_consuming_events(events):
        if event.get("attempt_counted") is False:
            continue
        event_time = _parse_datetime(event.get("occurred_at") or event.get("generated_at"))
        if event_time is not None and event_time < window_start:
            continue
        key = _budget_key_from_event(event)
        grouped.setdefault(key, []).append(dict(event))
    return grouped


def _budget_consuming_events(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    active: dict[str, dict[str, Any]] = {}
    consumed: list[dict[str, Any]] = []
    for event in events:
        event_type = str(event.get("event_type") or RECOVERY_ATTEMPT_RECORDED)
        reservation_id = str(event.get("reservation_id") or "")
        if event_type == BUDGET_ATTEMPT_RESERVED:
            if reservation_id:
                active[reservation_id] = dict(event)
            continue
        if event_type in {BUDGET_ATTEMPT_RELEASED, BUDGET_ATTEMPT_EXPIRED}:
            active.pop(reservation_id, None)
            continue
        if event_type in {BUDGET_ATTEMPT_CONSUMED_SUCCESS, BUDGET_ATTEMPT_CONSUMED_FAILURE}:
            reservation = active.pop(reservation_id, None)
            consumed.append(_merge_consumed_event(reservation=reservation, event=event))
            continue
        consumed.append(dict(event))
    return consumed + list(active.values())


def _merge_consumed_event(*, reservation: Mapping[str, Any] | None, event: Mapping[str, Any]) -> dict[str, Any]:
    merged = dict(reservation or {})
    merged.update(dict(event))
    if reservation:
        for key in ("agent_id", "action_type", "target_identity", "target_identity_hash", "runtime_generation_id", "budget_key"):
            if key not in merged or merged.get(key) in (None, ""):
                merged[key] = reservation.get(key)
        if not merged.get("failure_classification"):
            merged["failure_classification"] = reservation.get("failure_classification") or DEFAULT_FAILURE_CLASSIFICATION
    return merged


def _entry_for_key(
    *,
    key: str,
    events: list[dict[str, Any]],
    request: Mapping[str, Any] | None,
    config: TrackBRecoveryBudgetLedgerConfig,
    now: datetime,
) -> dict[str, Any]:
    basis = request or (events[-1] if events else _default_budget_request())
    latest_time = _latest_event_time(events)
    attempts_used = len(events)
    attempts_remaining = max(0, config.max_attempts_per_target - attempts_used)
    exhausted = attempts_remaining <= 0
    cooldown_until = None
    if exhausted:
        cooldown_base = latest_time or now
        cooldown_until = (cooldown_base + timedelta(seconds=config.cooldown_seconds)).isoformat()
    target_identity = _normalize_identity(_mapping(basis.get("target_identity")))
    return {
        "budget_key": key,
        "agent_id": str(basis.get("agent_id") or DEFAULT_AGENT_ID),
        "action_type": str(basis.get("action_type") or DEFAULT_ACTION_TYPE),
        "target_identity_hash": str(basis.get("target_identity_hash") or target_identity_hash(target_identity)),
        "target_identity": target_identity,
        "runtime_generation_id": basis.get("runtime_generation_id"),
        "stop_reason": basis.get("stop_reason"),
        "failure_classification": str(basis.get("failure_classification") or basis.get("stop_reason") or DEFAULT_FAILURE_CLASSIFICATION),
        "time_window_seconds": config.budget_window_seconds,
        "attempts_used": attempts_used,
        "attempts_remaining": attempts_remaining,
        "max_attempts": config.max_attempts_per_target,
        "budget_exhausted": exhausted,
        "cooldown_until": cooldown_until,
        "quarantine_required": exhausted,
        "latest_attempt_at": None if latest_time is None else latest_time.isoformat(),
        "event_count": len(events),
    }


def _request_for_key(*, key: str, requests: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for request in requests:
        if _budget_key_from_request(request) == key:
            return request
    return None


def _budget_key_from_request(request: Mapping[str, Any]) -> str:
    identity_hash = str(request.get("target_identity_hash") or target_identity_hash(_mapping(request.get("target_identity"))))
    return "|".join(
        [
            str(request.get("agent_id") or DEFAULT_AGENT_ID),
            str(request.get("action_type") or DEFAULT_ACTION_TYPE),
            identity_hash,
            str(request.get("runtime_generation_id") or "*"),
            str(request.get("failure_classification") or request.get("stop_reason") or DEFAULT_FAILURE_CLASSIFICATION),
        ]
    )


def _public_budget_key(
    *,
    agent_id: str,
    action_type: str,
    target_identity: Mapping[str, Any],
    runtime_generation_id: str | None = None,
    failure_classification: str | None = None,
) -> str:
    return _budget_key_from_request(
        {
            "agent_id": agent_id,
            "action_type": action_type,
            "target_identity": target_identity,
            "runtime_generation_id": runtime_generation_id,
            "failure_classification": failure_classification or DEFAULT_FAILURE_CLASSIFICATION,
        }
    )


def _budget_key_from_event(event: Mapping[str, Any]) -> str:
    return _budget_key_from_request(event)


def _latest_event_time(events: Sequence[Mapping[str, Any]]) -> datetime | None:
    parsed = [_parse_datetime(event.get("occurred_at") or event.get("generated_at")) for event in events]
    valid = [item for item in parsed if item is not None]
    return max(valid) if valid else None


def _normalize_identity(identity: Mapping[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in sorted(identity.items()) if value not in (None, "")}


def _reservation_by_id(reservation_id: Any, events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    for event in events:
        if event.get("reservation_id") == reservation_id and event.get("event_type") == BUDGET_ATTEMPT_RESERVED:
            return event
    return None


def _active_reservation(recovery_attempt_id: Any, events: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    active: dict[str, Mapping[str, Any]] = {}
    for event in events:
        event_type = str(event.get("event_type") or "")
        reservation_id = str(event.get("reservation_id") or "")
        if event_type == BUDGET_ATTEMPT_RESERVED and event.get("recovery_attempt_id") == recovery_attempt_id:
            active[reservation_id] = event
        elif event_type in {
            BUDGET_ATTEMPT_RELEASED,
            BUDGET_ATTEMPT_CONSUMED_SUCCESS,
            BUDGET_ATTEMPT_CONSUMED_FAILURE,
            BUDGET_ATTEMPT_EXPIRED,
        }:
            active.pop(reservation_id, None)
    for reservation in active.values():
        return reservation
    return None


def _validation(classification: str, reason: str) -> dict[str, Any]:
    return {
        "classification": classification,
        "valid": classification == BUDGET_EVENT_VALID,
        "reason": reason,
        "read_only": True,
        "broker_mutation": False,
        "runtime_restart_authority": False,
    }


def _safe_slug(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in str(value)).strip("_") or "unknown"


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        if not line.strip():
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload)


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


if __name__ == "__main__":
    raise SystemExit(main())
