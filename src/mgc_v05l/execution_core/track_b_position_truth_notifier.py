"""Read-only Track B Position Truth notification sidecar.

The sidecar consumes the execution_core Position Truth event log only. Dashboard
projection artifacts are display-only and must not become notification authority.
"""

from __future__ import annotations

import json
import shutil
import subprocess
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping

from mgc_v05l.execution_core.track_b_position_truth_monitor import DEFAULT_TRADE_OUTCOME_EVENTS

DEFAULT_NOTIFICATION_EVENTS = (
    Path("outputs") / "operator_dashboard" / "runtime" / "track_b_notifications.jsonl"
)
DEFAULT_NOTIFICATION_STATE = (
    Path("outputs") / "operator_dashboard" / "runtime" / "track_b_notification_sidecar_state.json"
)

NOTIFY_EVENT_TYPES = {
    "ADOPTED_OPEN_MANAGED",
    "ENTRY_FILLED",
    "LIFECYCLE_CLOSED_FLAT",
    "RECONCILIATION_BLOCKED",
    "RECONCILIATION_CLEAN",
    "RUNTIME_STOPPED_WITH_BROKER_EXPOSURE",
    "SUSPICIOUS_ORDER_DETECTED",
}
NOTIFY_CLASSIFICATIONS = {
    "BROKER_POSITION_REQUIRES_ADOPTION",
    "CLOSE_ORDER_SUSPICIOUS",
    "CLOSE_ORDER_WORKING_TOO_LONG",
    "OPEN_MANAGED_METADATA_INCOMPLETE",
    "RECONCILIATION_BLOCKED",
    "REVIEW_REQUIRED",
}


@dataclass(frozen=True)
class TrackBPositionTruthNotificationConfig:
    repo_root: Path
    event_log_path: Path = DEFAULT_TRADE_OUTCOME_EVENTS
    notification_log_path: Path = DEFAULT_NOTIFICATION_EVENTS
    state_path: Path = DEFAULT_NOTIFICATION_STATE
    rate_limit_seconds: float = 300.0
    enable_macos_notifications: bool = True

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


Notifier = Callable[[Mapping[str, Any]], bool]


def process_position_truth_notifications(
    *,
    config: TrackBPositionTruthNotificationConfig,
    now: datetime | None = None,
    notifier: Notifier | None = None,
) -> list[dict[str, Any]]:
    """Process new Position Truth events and emit deduplicated notifications."""

    actual_now = _ensure_utc(now or datetime.now(UTC))
    event_log_path = config.resolve(config.event_log_path)
    state_path = config.resolve(config.state_path)
    state = _read_json(state_path)
    offset = int(state.get("event_log_offset") or 0)
    raw_events, next_offset = _read_events_since(event_log_path, offset)
    if next_offset < offset:
        offset = 0
        raw_events, next_offset = _read_events_since(event_log_path, offset)

    recent_signatures = _recent_signatures(state, now=actual_now, ttl_seconds=config.rate_limit_seconds)
    deliveries: list[dict[str, Any]] = []
    for event in raw_events:
        notification = build_notification_payload(event=event, now=actual_now)
        if notification is None:
            continue
        signature = str(notification["signature"])
        if signature in recent_signatures:
            continue
        delivered = _deliver_notification(
            notification=notification,
            config=config,
            notifier=notifier,
        )
        notification["delivered"] = delivered
        notification["delivery_method"] = notification.get("delivery_method") or (
            "macos_osascript" if delivered and config.enable_macos_notifications else "jsonl_only"
        )
        deliveries.append(notification)
        recent_signatures[signature] = actual_now.isoformat()

    if deliveries:
        _append_jsonl(config.resolve(config.notification_log_path), deliveries)
    state_payload = {
        "schema_version": "track_b_position_truth_notification_state_v1",
        "updated_at": actual_now.isoformat(),
        "event_log_path": str(event_log_path),
        "event_log_offset": next_offset,
        "rate_limit_seconds": config.rate_limit_seconds,
        "recent_signatures": recent_signatures,
        "read_only": True,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }
    _write_json_atomic(state_path, state_payload)
    return deliveries


def build_notification_payload(*, event: Mapping[str, Any], now: datetime | None = None) -> dict[str, Any] | None:
    event_type = str(event.get("event_type") or "")
    classification = str(event.get("classification") or "")
    if event_type not in NOTIFY_EVENT_TYPES and classification not in NOTIFY_CLASSIFICATIONS:
        return None
    actual_now = _ensure_utc(now or datetime.now(UTC))
    symbol = event.get("symbol")
    title = _notification_title(event_type=event_type, classification=classification, symbol=symbol)
    detail = str(event.get("detail") or classification or event_type)
    signature = "|".join(
        [
            event_type,
            str(symbol or ""),
            classification,
            str(event.get("generated_at") or ""),
            _state_signature(event.get("state")),
        ]
    )
    return {
        "schema_version": "track_b_position_truth_notification_v1",
        "generated_at": actual_now.isoformat(),
        "source_event_generated_at": event.get("generated_at"),
        "source_event_type": event_type,
        "symbol": symbol,
        "classification": classification,
        "title": title,
        "message": detail,
        "signature": signature,
        "source_event": dict(event),
        "read_only": True,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
    }


def _deliver_notification(
    *,
    notification: dict[str, Any],
    config: TrackBPositionTruthNotificationConfig,
    notifier: Notifier | None,
) -> bool:
    if notifier is not None:
        return bool(notifier(notification))
    if not config.enable_macos_notifications or shutil.which("osascript") is None:
        notification["delivery_method"] = "jsonl_only"
        return False
    script = (
        f'display notification "{_escape_osascript(str(notification["message"]))}" '
        f'with title "{_escape_osascript(str(notification["title"]))}"'
    )
    try:
        result = subprocess.run(
            ["osascript", "-e", script],
            check=False,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            timeout=5,
        )
    except (OSError, subprocess.TimeoutExpired):
        notification["delivery_method"] = "jsonl_only"
        return False
    notification["delivery_method"] = "macos_osascript" if result.returncode == 0 else "jsonl_only"
    return result.returncode == 0


def _read_events_since(path: Path, offset: int) -> tuple[list[dict[str, Any]], int]:
    try:
        size = path.stat().st_size
    except OSError:
        return [], 0
    if offset > size:
        return [], 0
    events: list[dict[str, Any]] = []
    with path.open("rb") as handle:
        handle.seek(offset)
        for raw_line in handle:
            try:
                decoded = raw_line.decode("utf-8")
                payload = json.loads(decoded)
            except (UnicodeDecodeError, json.JSONDecodeError):
                continue
            if isinstance(payload, Mapping):
                events.append(dict(payload))
        next_offset = handle.tell()
    return events, next_offset


def _recent_signatures(state: Mapping[str, Any], *, now: datetime, ttl_seconds: float) -> dict[str, str]:
    raw = state.get("recent_signatures")
    if not isinstance(raw, Mapping):
        return {}
    kept: dict[str, str] = {}
    for signature, generated_at in raw.items():
        parsed = _parse_time(generated_at)
        if parsed is None or (now - parsed).total_seconds() > ttl_seconds:
            continue
        kept[str(signature)] = parsed.isoformat()
    return kept


def _notification_title(*, event_type: str, classification: str, symbol: Any) -> str:
    prefix = "Track B PAPER"
    subject = f" {symbol}" if symbol else ""
    label = event_type or classification or "Position Truth"
    return f"{prefix}{subject}: {label}"


def _state_signature(state: Any) -> str:
    if not isinstance(state, Mapping):
        return ""
    keys = ("broker_quantity", "open_order_ids", "lifecycle_ids", "suspicious_order_reasons")
    return json.dumps({key: state.get(key) for key in keys if key in state}, sort_keys=True)


def _append_jsonl(path: Path, rows: list[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(dict(row), sort_keys=True) + "\n")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.tmp")
    tmp.write_text(json.dumps(dict(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _parse_time(value: Any) -> datetime | None:
    if value in {None, ""}:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _escape_osascript(value: str) -> str:
    return value.replace("\\", "\\\\").replace('"', '\\"')
