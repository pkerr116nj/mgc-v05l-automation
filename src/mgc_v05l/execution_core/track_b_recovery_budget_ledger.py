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


RECOVERY_BUDGET_AVAILABLE = "RECOVERY_BUDGET_AVAILABLE"
RECOVERY_BUDGET_EXHAUSTED = "RECOVERY_BUDGET_EXHAUSTED"
RECOVERY_BUDGET_UNKNOWN = "RECOVERY_BUDGET_UNKNOWN"

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
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(dict(event), sort_keys=True) + "\n")
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
        "event_type": "RECOVERY_ATTEMPT_RECORDED",
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
    for event in events:
        if event.get("attempt_counted") is False:
            continue
        event_time = _parse_datetime(event.get("occurred_at") or event.get("generated_at"))
        if event_time is not None and event_time < window_start:
            continue
        key = _budget_key_from_event(event)
        grouped.setdefault(key, []).append(dict(event))
    return grouped


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


def _budget_key_from_event(event: Mapping[str, Any]) -> str:
    return _budget_key_from_request(event)


def _latest_event_time(events: Sequence[Mapping[str, Any]]) -> datetime | None:
    parsed = [_parse_datetime(event.get("occurred_at") or event.get("generated_at")) for event in events]
    valid = [item for item in parsed if item is not None]
    return max(valid) if valid else None


def _normalize_identity(identity: Mapping[str, Any]) -> dict[str, str]:
    return {str(key): str(value) for key, value in sorted(identity.items()) if value not in (None, "")}


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
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


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
