from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any
from uuid import uuid4

DEFAULT_LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS = 28800

INCREASE_RISK = "INCREASE_RISK"
REDUCE_RISK = "REDUCE_RISK"
OPERATOR_CONTROL = "OPERATOR_CONTROL"


def local_operator_auth_ttl_seconds() -> int:
    raw_value = str(os.environ.get("MGC_LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS") or "").strip()
    try:
        parsed = int(raw_value) if raw_value else DEFAULT_LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS
    except ValueError:
        parsed = DEFAULT_LOCAL_OPERATOR_AUTH_SESSION_TTL_SECONDS
    return max(60, parsed)


def _read_json_file(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_jsonl_rows(path: Path, limit: int = 40) -> list[dict[str, Any]]:
    try:
        rows = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    parsed_rows: list[dict[str, Any]] = []
    for line in rows[-limit:]:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            parsed_rows.append(payload)
    return parsed_rows


def _iso_datetime(raw_value: Any) -> datetime | None:
    if raw_value in (None, ""):
        return None
    try:
        return datetime.fromisoformat(str(raw_value).replace("Z", "+00:00"))
    except ValueError:
        return None


def _format_remaining_label(seconds: int | None) -> str:
    if seconds is None or seconds <= 0:
        return "expired"
    minutes, remaining_seconds = divmod(seconds, 60)
    hours, remaining_minutes = divmod(minutes, 60)
    if hours:
        return f"{hours}h {remaining_minutes:02d}m {remaining_seconds:02d}s"
    if minutes:
        return f"{minutes}m {remaining_seconds:02d}s"
    return f"{remaining_seconds}s"


def production_action_risk_bucket(action: str, payload: dict[str, Any] | None = None) -> str:
    normalized_action = str(action or "").strip().lower()
    normalized_payload = payload or {}
    intent_type = str(normalized_payload.get("intent_type") or "").strip().upper()
    if normalized_action == "preview-order":
        return REDUCE_RISK if intent_type == "FLATTEN" else INCREASE_RISK
    if normalized_action == "submit-order":
        return REDUCE_RISK if intent_type == "FLATTEN" else INCREASE_RISK
    if normalized_action == "flatten-position":
        return REDUCE_RISK
    if normalized_action == "cancel-order":
        return REDUCE_RISK
    if normalized_action == "replace-order":
        if (
            intent_type == "FLATTEN"
            or bool(normalized_payload.get("reduce_only"))
            or bool(normalized_payload.get("replace_reduces_risk"))
            or bool(normalized_payload.get("operator_reduce_only"))
        ):
            return REDUCE_RISK
        return INCREASE_RISK
    return OPERATOR_CONTROL


def append_local_operator_auth_event(repo_root: Path, payload: dict[str, Any]) -> dict[str, Any]:
    events_path = repo_root / "outputs" / "operator_dashboard" / "local_operator_auth_events.jsonl"
    events_path.parent.mkdir(parents=True, exist_ok=True)
    event = {
        "event_id": str(payload.get("event_id") or uuid4()),
        "occurred_at": str(payload.get("occurred_at") or datetime.now(timezone.utc).isoformat()),
        **payload,
    }
    with events_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(event, sort_keys=True))
        handle.write("\n")
    return event


def local_operator_auth_surface(repo_root: Path, *, now: datetime | None = None) -> dict[str, Any]:
    current_time = now or datetime.now(timezone.utc)
    state_path = repo_root / "outputs" / "operator_dashboard" / "local_operator_auth_state.json"
    events_path = repo_root / "outputs" / "operator_dashboard" / "local_operator_auth_events.jsonl"
    if not state_path.exists():
        entry_blocked_reason = "Local operator auth state artifact is not present in this workspace."
        return {
            "known": False,
            "available": False,
            "ready": None,
            "auth_session_active": False,
            "label": "LOCAL OPERATOR AUTH UNKNOWN",
            "detail": "Local operator auth state artifact is not present in this workspace.",
            "required_for_live_submit": True,
            "blocker": entry_blocked_reason,
            "auth_method": None,
            "authenticated_at": None,
            "auth_session_expires_at": None,
            "auth_session_ttl_seconds": local_operator_auth_ttl_seconds(),
            "ttl_seconds": local_operator_auth_ttl_seconds(),
            "time_remaining_seconds": None,
            "time_remaining_label": "unknown",
            "entry_allowed": False,
            "entry_blocked_reason": entry_blocked_reason,
            "flatten_allowed": True,
            "flatten_blocked_reason": None,
            "cancel_allowed": True,
            "cancel_blocked_reason": None,
            "replace_allowed": False,
            "replace_blocked_reason": "Replace order requires an active local operator auth session unless it is explicitly marked reduce-only.",
            "action_risk_policy": {
                "submit_order_open": INCREASE_RISK,
                "submit_order_flatten": REDUCE_RISK,
                "flatten_position": REDUCE_RISK,
                "cancel_order": REDUCE_RISK,
                "replace_order_default": INCREASE_RISK,
                "replace_order_reduce_only": REDUCE_RISK,
            },
            "next_action_label": "Authenticate Now",
            "next_action_detail": "Prime local operator auth before preview, submit, or flatten actions.",
            "local_operator_identity": None,
            "auth_session_id": None,
            "artifacts": {
                "state_path": str(state_path),
                "events_path": str(events_path),
            },
            "prime_action": {
                "action": "prime-local-auth",
                "endpoint": "/api/production-link/prime-local-auth",
                "command": "bash scripts/run_local_operator_auth.sh",
            },
            "source_of_truth": "shared_local_auth_artifact",
        }
    payload = _read_json_file(state_path)
    events = _read_jsonl_rows(Path(str((payload.get("artifacts") or {}).get("events_path") or events_path)))
    ttl_seconds = max(60, int(payload.get("auth_session_ttl_seconds") or local_operator_auth_ttl_seconds()))
    latest_success: dict[str, Any] | None = None
    latest_clear_at: datetime | None = None
    for event in events:
        event_type = str(event.get("event_type") or "").strip()
        occurred_at = _iso_datetime(event.get("occurred_at"))
        if event_type == "local_operator_auth_session_cleared":
            if occurred_at is not None and (latest_clear_at is None or occurred_at > latest_clear_at):
                latest_clear_at = occurred_at
            continue
        if event_type not in {"local_operator_auth_succeeded", "sensitive_action_authorized"}:
            continue
        if latest_success is None:
            latest_success = event
            continue
        previous_occurred_at = _iso_datetime(latest_success.get("occurred_at"))
        if occurred_at is not None and (previous_occurred_at is None or occurred_at > previous_occurred_at):
            latest_success = event
    authenticated_at = _iso_datetime(payload.get("last_authenticated_at"))
    auth_session_expires_at = _iso_datetime(payload.get("auth_session_expires_at"))
    auth_session_id = str(payload.get("auth_session_id") or "").strip() or None
    local_operator_identity = str(payload.get("local_operator_identity") or "").strip() or None
    auth_method = str(payload.get("auth_method") or "").strip() or None
    last_auth_result = str(payload.get("last_auth_result") or "").strip() or None
    if latest_success is not None:
        event_authenticated_at = _iso_datetime(latest_success.get("authenticated_at") or latest_success.get("occurred_at"))
        if event_authenticated_at is not None and (authenticated_at is None or event_authenticated_at > authenticated_at):
            authenticated_at = event_authenticated_at
            auth_session_id = str(latest_success.get("auth_session_id") or auth_session_id or "").strip() or None
            local_operator_identity = str(latest_success.get("local_operator_identity") or local_operator_identity or "").strip() or None
            auth_method = str(latest_success.get("auth_method") or auth_method or "").strip() or None
            auth_session_expires_at = event_authenticated_at + timedelta(seconds=ttl_seconds)
            last_auth_result = "SUCCEEDED"
    if authenticated_at is not None and auth_session_expires_at is None:
        auth_session_expires_at = authenticated_at + timedelta(seconds=ttl_seconds)
    if latest_clear_at is not None and authenticated_at is not None and latest_clear_at >= authenticated_at:
        auth_session_expires_at = None
    auth_available = bool(payload.get("auth_available"))
    touch_id_available = bool(payload.get("touch_id_available"))
    ready = bool(
        auth_available
        and touch_id_available
        and authenticated_at is not None
        and auth_session_expires_at is not None
        and auth_session_expires_at > current_time
        and local_operator_identity
        and auth_method
    )
    time_remaining_seconds = None
    if auth_session_expires_at is not None:
        time_remaining_seconds = max(int((auth_session_expires_at - current_time).total_seconds()), 0)
    detail = str(payload.get("last_auth_detail") or "").strip()
    if ready:
        detail = detail or "Local operator auth session is active for live broker actions."
    elif auth_available and touch_id_available and auth_session_expires_at is not None and auth_session_expires_at <= current_time:
        detail = f"Local operator auth session expired at {auth_session_expires_at.isoformat()}."
        last_auth_result = "EXPIRED"
    elif not detail:
        detail = "Local operator auth session is not active."
    entry_blocked_reason = None if ready else detail or "Local operator auth session is not active."
    blocker = entry_blocked_reason
    return {
        "known": True,
        "available": auth_available,
        "touch_id_available": touch_id_available,
        "ready": ready,
        "auth_session_active": ready,
        "label": "LOCAL OPERATOR AUTH READY" if ready else "LOCAL OPERATOR AUTH REQUIRED",
        "detail": detail,
        "required_for_live_submit": True,
        "blocker": blocker,
        "entry_allowed": ready,
        "entry_blocked_reason": entry_blocked_reason,
        "flatten_allowed": True,
        "flatten_blocked_reason": None,
        "cancel_allowed": True,
        "cancel_blocked_reason": None,
        "replace_allowed": ready,
        "replace_blocked_reason": (
            None if ready else "Replace order requires an active local operator auth session unless it is explicitly marked reduce-only."
        ),
        "action_risk_policy": {
            "submit_order_open": INCREASE_RISK,
            "submit_order_flatten": REDUCE_RISK,
            "flatten_position": REDUCE_RISK,
            "cancel_order": REDUCE_RISK,
            "replace_order_default": INCREASE_RISK,
            "replace_order_reduce_only": REDUCE_RISK,
        },
        "auth_method": auth_method,
        "authenticated_at": authenticated_at.isoformat() if authenticated_at is not None else None,
        "auth_session_expires_at": auth_session_expires_at.isoformat() if auth_session_expires_at is not None else None,
        "auth_session_ttl_seconds": ttl_seconds,
        "ttl_seconds": ttl_seconds,
        "time_remaining_seconds": time_remaining_seconds,
        "time_remaining_label": _format_remaining_label(time_remaining_seconds),
        "next_action_label": "Ready" if ready else "Authenticate Now",
        "next_action_detail": (
            "Reuse the active Touch ID session across preview, submit, and flatten actions."
            if ready
            else "Prime local operator auth before preview, submit, or flatten actions."
        ),
        "local_operator_identity": local_operator_identity,
        "auth_session_id": auth_session_id,
        "last_auth_result": last_auth_result,
        "artifacts": dict(payload.get("artifacts") or {}) or {"state_path": str(state_path), "events_path": str(events_path)},
        "prime_action": {
            "action": "prime-local-auth",
            "endpoint": "/api/production-link/prime-local-auth",
            "command": "bash scripts/run_local_operator_auth.sh",
        },
        "source_of_truth": "shared_local_auth_artifact",
    }
