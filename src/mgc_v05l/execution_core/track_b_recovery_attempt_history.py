"""Read-only Track B PAPER recovery attempt history rollup."""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_paper_autonomous_recovery_executor import (
    DEFAULT_EXECUTOR_EVENT_LOG,
    DEFAULT_LATEST_EXECUTOR_REPORT,
)
from mgc_v05l.execution_core.track_b_recovery_budget_ledger import (
    BUDGET_ATTEMPT_CONSUMED_FAILURE,
    BUDGET_ATTEMPT_CONSUMED_SUCCESS,
    BUDGET_ATTEMPT_EXPIRED,
    BUDGET_ATTEMPT_RELEASED,
    BUDGET_ATTEMPT_RESERVED,
    DEFAULT_RECOVERY_BUDGET_EVENTS,
    DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_autonomous_recovery"
    / "latest_recovery_attempt_history.json"
)
DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "control_plane"
    / "latest_control_plane_snapshot.json"
)


@dataclass(frozen=True)
class TrackBRecoveryAttemptHistoryConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT
    executor_event_log_path: Path = DEFAULT_EXECUTOR_EVENT_LOG
    latest_executor_report_path: Path = DEFAULT_LATEST_EXECUTOR_REPORT
    recovery_budget_ledger_path: Path = DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT
    recovery_budget_event_log_path: Path = DEFAULT_RECOVERY_BUDGET_EVENTS
    control_plane_snapshot_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    recent_attempt_limit: int = 10
    incomplete_attempt_stale_seconds: int = 900

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_recovery_attempt_history(
    *,
    config: TrackBRecoveryAttemptHistoryConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    event_rows = _read_jsonl(config.resolve(config.executor_event_log_path))
    latest_report = _read_json(config.resolve(config.latest_executor_report_path))
    budget_ledger = _read_json(config.resolve(config.recovery_budget_ledger_path))
    budget_events = _read_jsonl(config.resolve(config.recovery_budget_event_log_path))
    snapshot = _read_json(config.resolve(config.control_plane_snapshot_path))

    latest = _latest_attempt(latest_report=latest_report, event_rows=event_rows)
    self_recover = _self_recover_fields(latest=latest, snapshot=snapshot, budget_ledger=budget_ledger)
    recent_attempts = _recent_attempts(event_rows=event_rows, latest_report=latest_report, limit=config.recent_attempt_limit)
    active_reservations = _active_budget_reservations(budget_events)
    stale_or_incomplete = _stale_or_incomplete_attempts(
        event_rows=event_rows,
        latest_report=latest_report,
        active_reservations=active_reservations,
        now=actual_now,
        stale_after=timedelta(seconds=config.incomplete_attempt_stale_seconds),
    )
    last_success_at, last_failure_at = _last_budget_outcomes(budget_events)

    return {
        "schema_version": "track_b_recovery_attempt_history_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "status_only": True,
        "submit_authority": False,
        "broker_mutation": False,
        "lifecycle_mutation": False,
        "runtime_restart_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "latest_recovery_attempt_id": latest.get("recovery_attempt_id") or "",
        "latest_action_type": latest.get("action_type") or "",
        "latest_classification": latest.get("classification") or "",
        **self_recover,
        "last_success_at": last_success_at,
        "last_failure_at": last_failure_at,
        "recent_attempts": recent_attempts,
        "active_budget_reservations": active_reservations,
        "stale_or_incomplete_attempts": stale_or_incomplete,
        "operator_explanation": _operator_explanation(latest=latest, snapshot=snapshot, self_recover=self_recover),
        "source_artifact_paths": {
            "executor_event_log": str(config.resolve(config.executor_event_log_path)),
            "latest_executor_report": str(config.resolve(config.latest_executor_report_path)),
            "recovery_budget_ledger": str(config.resolve(config.recovery_budget_ledger_path)),
            "recovery_budget_events": str(config.resolve(config.recovery_budget_event_log_path)),
            "control_plane_snapshot": str(config.resolve(config.control_plane_snapshot_path)),
            "recovery_attempt_history": str(config.resolve(config.output_path)),
        },
        "dashboard_projection_consumed": False,
        "not_routing_authority": True,
    }


def write_track_b_recovery_attempt_history(
    *,
    config: TrackBRecoveryAttemptHistoryConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    write_json_atomic(output_path, dict(payload))
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write a read-only Track B PAPER recovery attempt history rollup.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_RECOVERY_ATTEMPT_HISTORY_ARTIFACT)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBRecoveryAttemptHistoryConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    payload = build_track_b_recovery_attempt_history(config=config)
    authority_path = write_track_b_recovery_attempt_history(config=config, payload=payload)
    summary = {
        "latest_recovery_attempt_id": payload.get("latest_recovery_attempt_id"),
        "latest_action_type": payload.get("latest_action_type"),
        "latest_classification": payload.get("latest_classification"),
        "recommended_recovery_action": payload.get("recommended_recovery_action"),
        "recovery_budget_key": payload.get("recovery_budget_key"),
        "attempts_remaining": payload.get("attempts_remaining"),
        "quarantine_required": payload.get("quarantine_required"),
        "active_budget_reservations": len(payload.get("active_budget_reservations") or []),
        "stale_or_incomplete_attempts": len(payload.get("stale_or_incomplete_attempts") or []),
        "authority_path": str(authority_path),
        "read_only": True,
    }
    print(json.dumps(payload if bool(args.json) else summary, indent=2, sort_keys=True))
    return 0


def _latest_attempt(*, latest_report: Mapping[str, Any], event_rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    if latest_report:
        return dict(latest_report)
    rows = [dict(row) for row in event_rows]
    return rows[-1] if rows else {}


def _self_recover_fields(
    *,
    latest: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    budget_ledger: Mapping[str, Any],
) -> dict[str, Any]:
    summary = _mapping(budget_ledger.get("summary"))
    return {
        "recommended_recovery_action": latest.get("recommended_recovery_action")
        or snapshot.get("recommended_recovery_action")
        or "",
        "paper_action_policy": latest.get("self_recover_paper_action_policy")
        or snapshot.get("paper_action_policy")
        or "",
        "autonomous_recovery_plan_classification": latest.get("autonomous_recovery_plan_classification")
        or snapshot.get("self_recover_autonomous_recovery_plan_classification")
        or snapshot.get("autonomous_recovery_plan_classification")
        or "",
        "recovery_budget_key": latest.get("recovery_budget_key") or snapshot.get("recovery_budget_key") or "",
        "attempts_remaining": latest.get("self_recover_attempts_remaining")
        if latest.get("self_recover_attempts_remaining") is not None
        else snapshot.get("attempts_remaining")
        if snapshot.get("attempts_remaining") is not None
        else summary.get("minimum_attempts_remaining"),
        "cooldown_until": latest.get("self_recover_cooldown_until")
        or snapshot.get("cooldown_until")
        or _first_entry_value(budget_ledger, "cooldown_until"),
        "quarantine_required": latest.get("quarantine_required") is True
        or snapshot.get("quarantine_required") is True
        or budget_ledger.get("quarantine_required") is True
        or summary.get("quarantine_required") is True,
    }


def _recent_attempts(
    *,
    event_rows: Sequence[Mapping[str, Any]],
    latest_report: Mapping[str, Any],
    limit: int,
) -> list[dict[str, Any]]:
    rows = [dict(row) for row in event_rows[-max(0, limit) :]]
    if latest_report and not any(row.get("recovery_attempt_id") == latest_report.get("recovery_attempt_id") for row in rows):
        rows.append(_attempt_row(latest_report))
    return [_attempt_row(row) for row in rows][-max(0, limit) :]


def _attempt_row(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": row.get("generated_at"),
        "recovery_attempt_id": row.get("recovery_attempt_id") or "",
        "action_type": row.get("action_type") or "",
        "classification": row.get("classification") or "",
        "recommended_recovery_action": row.get("recommended_recovery_action") or "",
        "recovery_budget_key": row.get("recovery_budget_key") or row.get("budget_key") or "",
        "quarantine_required": row.get("quarantine_required") is True,
        "execution_enabled": row.get("execution_enabled") is True,
    }


def _active_budget_reservations(events: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    reservations: dict[str, dict[str, Any]] = {}
    terminal_reservations: set[str] = set()
    for event in events:
        event_type = str(event.get("event_type") or "")
        reservation_id = str(event.get("reservation_id") or "")
        if event_type == BUDGET_ATTEMPT_RESERVED and reservation_id:
            reservations[reservation_id] = {
                "reservation_id": reservation_id,
                "recovery_attempt_id": event.get("recovery_attempt_id") or "",
                "action_type": event.get("action_type") or "",
                "budget_key": event.get("budget_key") or "",
                "created_at": event.get("created_at") or event.get("generated_at"),
                "control_plane_snapshot_id": event.get("control_plane_snapshot_id") or "",
                "shared_truth_generation_id": event.get("shared_truth_generation_id") or "",
            }
        elif event_type in {
            BUDGET_ATTEMPT_RELEASED,
            BUDGET_ATTEMPT_CONSUMED_SUCCESS,
            BUDGET_ATTEMPT_CONSUMED_FAILURE,
            BUDGET_ATTEMPT_EXPIRED,
        } and reservation_id:
            terminal_reservations.add(reservation_id)
    return [row for reservation_id, row in sorted(reservations.items()) if reservation_id not in terminal_reservations]


def _stale_or_incomplete_attempts(
    *,
    event_rows: Sequence[Mapping[str, Any]],
    latest_report: Mapping[str, Any],
    active_reservations: Sequence[Mapping[str, Any]],
    now: datetime,
    stale_after: timedelta,
) -> list[dict[str, Any]]:
    incomplete: list[dict[str, Any]] = []
    for row in event_rows:
        if not row.get("recovery_attempt_id") or not row.get("classification") or not row.get("control_plane_snapshot_id"):
            incomplete.append({**_attempt_row(row), "reason": "executor_event_row_incomplete"})
    latest_id = str(latest_report.get("recovery_attempt_id") or "")
    if latest_report and latest_id and not any(row.get("recovery_attempt_id") == latest_id for row in event_rows):
        incomplete.append({**_attempt_row(latest_report), "reason": "latest_report_not_present_in_event_log"})
    for row in active_reservations:
        created_at = _parse_datetime(row.get("created_at"))
        if created_at is None or now - created_at > stale_after:
            incomplete.append(
                {
                    "recovery_attempt_id": row.get("recovery_attempt_id") or "",
                    "action_type": row.get("action_type") or "",
                    "classification": "ACTIVE_BUDGET_RESERVATION_STALE",
                    "reason": "active_budget_reservation_stale_or_missing_timestamp",
                    "reservation_id": row.get("reservation_id") or "",
                }
            )
    return incomplete


def _last_budget_outcomes(events: Sequence[Mapping[str, Any]]) -> tuple[str | None, str | None]:
    last_success_at: str | None = None
    last_failure_at: str | None = None
    for event in events:
        event_type = str(event.get("event_type") or "")
        timestamp = str(event.get("consumed_at") or event.get("generated_at") or event.get("created_at") or "")
        if event_type == BUDGET_ATTEMPT_CONSUMED_SUCCESS:
            last_success_at = timestamp
        elif event_type == BUDGET_ATTEMPT_CONSUMED_FAILURE:
            last_failure_at = timestamp
    return last_success_at, last_failure_at


def _operator_explanation(
    *,
    latest: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    self_recover: Mapping[str, Any],
) -> str:
    for value in (
        latest.get("operator_explanation"),
        _mapping(latest.get("pre_action_evidence")).get("operator_explanation"),
        snapshot.get("operator_explanation"),
    ):
        if value:
            return str(value)
    recommended = str(self_recover.get("recommended_recovery_action") or "")
    if recommended:
        return f"Latest PAPER recovery posture is {recommended}; rollup is read-only status."
    return "No PAPER autonomous recovery attempts have been recorded."


def _first_entry_value(payload: Mapping[str, Any], key: str) -> Any:
    for row in payload.get("entries") or []:
        if isinstance(row, Mapping) and row.get(key) not in (None, ""):
            return row.get(key)
    return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
    except OSError:
        return []
    rows: list[dict[str, Any]] = []
    for line in lines:
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, Mapping):
            rows.append(dict(payload))
    return rows


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


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


if __name__ == "__main__":
    raise SystemExit(main())
