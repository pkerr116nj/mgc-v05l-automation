"""Real operator alert dispatching with persistence and deduplicated active-state tracking."""

from __future__ import annotations

import hashlib
import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy.exc import OperationalError

from .logger import StructuredLogger, get_logger

DEFAULT_THROTTLE_SECONDS = 300
SEVERITY_INFO = "INFO"
SEVERITY_ACTION = "ACTION"
SEVERITY_BLOCKING = "BLOCKING"
SEVERITY_RECOVERY = "RECOVERY"
SEVERITY_AUDIT_ONLY = "AUDIT_ONLY"
ACTIVE_SEVERITIES = {SEVERITY_ACTION, SEVERITY_BLOCKING}
TERMINAL_SEVERITIES = {SEVERITY_RECOVERY, SEVERITY_AUDIT_ONLY}


class AlertDispatcher:
    """Routes operator-relevant alerts into persisted audit and active state."""

    def __init__(
        self,
        structured_logger: Optional[StructuredLogger] = None,
        alert_repository: Any | None = None,
        *,
        source_subsystem: str = "runtime",
    ) -> None:
        self._structured_logger = structured_logger
        self._alert_repository = alert_repository
        self._source_subsystem = source_subsystem
        self._logger = get_logger(__name__)
        self._state_path: Path | None = (
            structured_logger.artifact_dir / "alerts_state.json" if structured_logger is not None else None
        )

    def emit(
        self,
        severity: str,
        code: str,
        message: str,
        payload: Optional[dict[str, Any]] = None,
        *,
        category: str | None = None,
        title: str | None = None,
        source_subsystem: str | None = None,
        dedup_key: str | None = None,
        recommended_action: str | None = None,
        active: bool | None = None,
        throttle_seconds: int | None = None,
        coalesce: bool = True,
        occurred_at: datetime | None = None,
        acknowledged: bool = False,
    ) -> dict[str, Any] | None:
        """Emit a one-shot or stateful alert event.

        Existing call sites may still pass legacy severities like warning/error/info.
        """

        normalized_severity = _normalize_severity(severity)
        occurred = occurred_at or _payload_datetime(payload, "occurred_at") or datetime.now(timezone.utc)
        normalized_category = category or _infer_category(code, normalized_severity)
        normalized_title = title or _default_title(code, normalized_category, normalized_severity)
        normalized_active = bool(active) if active is not None else normalized_severity in ACTIVE_SEVERITIES
        normalized_dedup_key = dedup_key or _default_dedup_key(code, normalized_category, payload)
        action_text = recommended_action or _default_recommended_action(normalized_category, normalized_severity, payload)
        throttle = max(int(throttle_seconds or _default_throttle_seconds(normalized_category, normalized_severity)), 0)
        source = str(source_subsystem or (payload.get("source_subsystem") if payload else None) or self._source_subsystem)
        detail_payload = dict(payload or {})

        record = {
            "event_type": "alert_event",
            "alert_id": _build_alert_id(
                normalized_dedup_key or code,
                occurred,
                normalized_severity,
                normalized_category,
                normalized_active,
            ),
            "occurred_at": occurred.isoformat(),
            "category": normalized_category,
            "severity": normalized_severity,
            "code": code,
            "title": normalized_title,
            "message": message,
            "detail": detail_payload,
            "source_subsystem": source,
            "dedup_key": normalized_dedup_key,
            "recommended_action": action_text,
            "acknowledged": bool(acknowledged),
            "active": normalized_active,
        }

        if coalesce and normalized_dedup_key and self._state_path is not None:
            state = self._load_state()
            state_record = dict(state["by_key"].get(normalized_dedup_key) or {})
            should_log, transition, occurrence_count = self._update_state_for_emit(
                state,
                state_record=state_record,
                dedup_key=normalized_dedup_key,
                record=record,
                occurred_at=occurred,
                throttle_seconds=throttle,
            )
            record["state_transition"] = transition
            record["occurrence_count"] = occurrence_count
            self._write_state(state)
            if not should_log:
                return None
        else:
            record["state_transition"] = "event"
            record["occurrence_count"] = 1

        self._log(record)
        return record

    def sync_condition(
        self,
        *,
        code: str,
        active: bool,
        severity: str,
        category: str,
        title: str,
        message: str,
        payload: Optional[dict[str, Any]] = None,
        dedup_key: str,
        recommended_action: str | None = None,
        occurred_at: datetime | None = None,
        source_subsystem: str | None = None,
        throttle_seconds: int | None = None,
    ) -> dict[str, Any] | None:
        """Track an ongoing condition and emit only when it opens, materially repeats, or resolves."""

        emitted = self.emit(
            severity=severity,
            code=code,
            message=message,
            payload=payload,
            category=category,
            title=title,
            source_subsystem=source_subsystem,
            dedup_key=dedup_key,
            recommended_action=recommended_action,
            active=active,
            throttle_seconds=throttle_seconds,
            coalesce=True,
            occurred_at=occurred_at,
        )
        if emitted is not None or not active:
            return emitted
        return None

    def _load_state(self) -> dict[str, Any]:
        if self._state_path is None or not self._state_path.exists():
            return {"updated_at": None, "by_key": {}, "active_alerts": []}
        try:
            payload = json.loads(self._state_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"updated_at": None, "by_key": {}, "active_alerts": []}
        by_key = payload.get("by_key")
        if not isinstance(by_key, dict):
            by_key = {}
        active_alerts = payload.get("active_alerts")
        if not isinstance(active_alerts, list):
            active_alerts = []
        return {
            "updated_at": payload.get("updated_at"),
            "by_key": by_key,
            "active_alerts": active_alerts,
        }

    def _write_state(self, state: dict[str, Any]) -> None:
        if self._structured_logger is None:
            return
        active_rows = [
            row
            for row in state["by_key"].values()
            if isinstance(row, dict) and row.get("active") is True
        ]
        active_rows.sort(key=lambda row: str(row.get("last_seen_at") or row.get("occurred_at") or ""), reverse=True)
        state["active_alerts"] = active_rows
        state["updated_at"] = datetime.now(timezone.utc).isoformat()
        self._structured_logger.write_alert_state(state)

    def _update_state_for_emit(
        self,
        state: dict[str, Any],
        *,
        state_record: dict[str, Any],
        dedup_key: str,
        record: dict[str, Any],
        occurred_at: datetime,
        throttle_seconds: int,
    ) -> tuple[bool, str, int]:
        previous_active = bool(state_record.get("active"))
        last_emitted_at = _parse_iso_datetime(state_record.get("last_emitted_at"))
        last_severity = str(state_record.get("severity") or "")
        last_message = str(state_record.get("message") or "")
        last_title = str(state_record.get("title") or "")
        occurrence_count = int(state_record.get("occurrence_count") or 0) + 1
        should_log = True
        transition = "event"

        if record["active"]:
            if previous_active:
                transition = "repeated"
                if (
                    throttle_seconds > 0
                    and last_emitted_at is not None
                    and occurred_at - last_emitted_at < timedelta(seconds=throttle_seconds)
                    and last_severity == record["severity"]
                    and last_message == record["message"]
                    and last_title == record["title"]
                ):
                    should_log = False
            else:
                transition = "opened"
        else:
            if previous_active:
                transition = "resolved"
            else:
                transition = "event"
                if throttle_seconds > 0 and last_emitted_at is not None and occurred_at - last_emitted_at < timedelta(seconds=throttle_seconds):
                    should_log = False

        state["by_key"][dedup_key] = {
            **state_record,
            "dedup_key": dedup_key,
            "category": record["category"],
            "severity": record["severity"],
            "title": record["title"],
            "message": record["message"],
            "recommended_action": record["recommended_action"],
            "source_subsystem": record["source_subsystem"],
            "active": bool(record["active"]),
            "acknowledged": bool(record["acknowledged"]),
            "detail": record["detail"],
            "occurred_at": state_record.get("occurred_at") or record["occurred_at"],
            "last_seen_at": record["occurred_at"],
            "last_emitted_at": record["occurred_at"] if should_log else state_record.get("last_emitted_at"),
            "occurrence_count": occurrence_count,
            "state_transition": transition,
        }
        return should_log, transition, occurrence_count

    def _log(self, record: dict[str, Any]) -> None:
        severity = str(record.get("severity") or SEVERITY_INFO)
        if severity == SEVERITY_BLOCKING:
            self._logger.error("%s: %s", record["code"], record["message"])
        elif severity == SEVERITY_ACTION:
            self._logger.warning("%s: %s", record["code"], record["message"])
        else:
            self._logger.info("%s: %s", record["code"], record["message"])

        if self._structured_logger is not None:
            self._structured_logger.log_alert(record)
        if self._alert_repository is not None:
            try:
                self._alert_repository.save(record, occurred_at=datetime.fromisoformat(record["occurred_at"]))
            except OperationalError as exc:
                if not _is_transient_sqlite_lock_error(exc):
                    raise
                self._logger.warning("alert_persistence_degraded: %s", exc)


def _build_alert_id(dedup_key: str, occurred_at: datetime, severity: str, category: str, active: bool) -> str:
    return hashlib.sha256(
        json.dumps(
            {
                "dedup_key": dedup_key,
                "occurred_at": occurred_at.isoformat(),
                "severity": severity,
                "category": category,
                "active": active,
            },
            sort_keys=True,
        ).encode("utf-8")
    ).hexdigest()


def _parse_iso_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text)
    except ValueError:
        return None


def _payload_datetime(payload: Optional[dict[str, Any]], key: str) -> datetime | None:
    if not payload:
        return None
    return _parse_iso_datetime(payload.get(key))


def _normalize_severity(severity: str) -> str:
    normalized = str(severity or "").strip().upper()
    if normalized in {SEVERITY_INFO, SEVERITY_ACTION, SEVERITY_BLOCKING, SEVERITY_RECOVERY, SEVERITY_AUDIT_ONLY}:
        return normalized
    if normalized in {"WARNING", "WARN"}:
        return SEVERITY_ACTION
    if normalized in {"ERROR", "CRITICAL"}:
        return SEVERITY_BLOCKING
    if normalized in {"SUCCESS", "RECOVERED"}:
        return SEVERITY_RECOVERY
    return SEVERITY_INFO


def _is_transient_sqlite_lock_error(error: OperationalError) -> bool:
    return "database is locked" in str(error).lower()


def _infer_category(code: str, severity: str) -> str:
    lowered = str(code).lower()
    if "reconcil" in lowered:
        return "reconciliation_mismatch"
    if "restart" in lowered or "recovery" in lowered:
        return "runtime_recovery"
    if "health" in lowered or "market" in lowered or "stale" in lowered:
        return "market_data"
    if "disconnect" in lowered or "broker" in lowered:
        return "broker_connectivity"
    if "fault" in lowered:
        return "persistent_fault"
    if "fill" in lowered:
        return "fill_event"
    if "intent" in lowered or "order" in lowered:
        return "order_lifecycle"
    if severity == SEVERITY_RECOVERY:
        return "recovery"
    return "runtime"


def _default_title(code: str, category: str, severity: str) -> str:
    if code:
        return code.replace("_", " ").upper()
    return f"{severity} {category.replace('_', ' ').upper()}"


def _default_dedup_key(code: str, category: str, payload: Optional[dict[str, Any]]) -> str:
    scoped = {
        "category": category,
        "code": code,
        "lane_id": payload.get("lane_id") if payload else None,
        "instrument": payload.get("instrument") if payload else None,
        "standalone_strategy_id": payload.get("standalone_strategy_id") if payload else None,
        "runtime_name": payload.get("runtime_name") if payload else None,
    }
    return hashlib.sha256(json.dumps(scoped, sort_keys=True).encode("utf-8")).hexdigest()[:24]


def _default_recommended_action(category: str, severity: str, payload: Optional[dict[str, Any]]) -> str | None:
    if severity == SEVERITY_AUDIT_ONLY:
        return None
    if category == "reconciliation_mismatch":
        return str(payload.get("recommended_action") if payload else "") or "Inspect the mismatch and rerun reconciliation once broker and internal state are understood."
    if category == "runtime_recovery":
        return str(payload.get("next_action") if payload else "") or "Watch the recovery result; intervene only if automatic recovery fails or is marked unsafe."
    if category == "market_data":
        return "Verify live polling/transport health if the condition does not self-resolve."
    if category == "broker_connectivity":
        return "Verify broker connectivity/auth before allowing new entries to continue."
    if category == "persistent_fault":
        return "Review the fault detail before clearing or resuming entries."
    return None


def _default_throttle_seconds(category: str, severity: str) -> int:
    if category in {"market_data", "broker_connectivity", "runtime_recovery", "reconciliation_mismatch", "persistent_fault"}:
        return DEFAULT_THROTTLE_SECONDS
    if severity == SEVERITY_AUDIT_ONLY:
        return 0
    return 60
