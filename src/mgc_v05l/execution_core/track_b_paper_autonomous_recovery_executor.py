"""Dry-run Track B PAPER autonomous recovery executor framework.

This module is the shared envelope for future PAPER recovery executors. Version
1 does not execute recovery actions. It validates one coherent Control Plane
Snapshot, checks a dry-run budget ledger, and writes audit artifacts only.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import (
    PRE_ACTION_SNAPSHOT_VALID,
    TrackBPreActionSnapshotValidatorConfig,
    validate_track_b_pre_action_snapshot,
)


EXECUTOR_DRY_RUN_READY = "EXECUTOR_DRY_RUN_READY"
EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION = "EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION"
EXECUTOR_BLOCKED_BUDGET_EXHAUSTED = "EXECUTOR_BLOCKED_BUDGET_EXHAUSTED"
EXECUTOR_BLOCKED_UNSUPPORTED_ACTION = "EXECUTOR_BLOCKED_UNSUPPORTED_ACTION"

SUPPORTED_ACTION_TYPES = {
    "REFRESH_EVIDENCE",
    "RUNTIME_RETRY",
    "MARKET_DATA_RESTART",
    "SCOPED_POSITION_CLEANUP",
    "MANAGED_ORDER_MODIFY",
    "TARGETED_CANCEL_REPLACE",
    "QUARANTINE_OBSERVE_ONLY",
}

REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_EXECUTOR_ATTEMPTS_DIR = (
    Path("outputs") / "track_b_execution_core" / "paper_autonomous_recovery" / "executor_attempts"
)
DEFAULT_EXECUTOR_EVENT_LOG = DEFAULT_EXECUTOR_ATTEMPTS_DIR / "paper_autonomous_recovery_executor_events.jsonl"
DEFAULT_LATEST_EXECUTOR_REPORT = DEFAULT_EXECUTOR_ATTEMPTS_DIR / "latest_paper_autonomous_recovery_executor_attempt.json"


@dataclass(frozen=True)
class TrackBPaperAutonomousRecoveryExecutorConfig:
    repo_root: Path = REPO_ROOT
    attempts_dir: Path = DEFAULT_EXECUTOR_ATTEMPTS_DIR
    event_log_path: Path = DEFAULT_EXECUTOR_EVENT_LOG
    latest_report_path: Path = DEFAULT_LATEST_EXECUTOR_REPORT
    max_attempts_per_target: int = 1
    max_attempts_per_window: int = 3
    budget_window_seconds: int = 3600
    pre_action_validator_config: TrackBPreActionSnapshotValidatorConfig | None = None

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path

    def validator_config(self) -> TrackBPreActionSnapshotValidatorConfig:
        return self.pre_action_validator_config or TrackBPreActionSnapshotValidatorConfig(repo_root=self.repo_root)


def build_track_b_paper_autonomous_recovery_executor_attempt(
    *,
    config: TrackBPaperAutonomousRecoveryExecutorConfig,
    expected_plan_classification: str,
    action_type: str,
    target_identity: Mapping[str, Any] | None = None,
    max_snapshot_age_seconds: int = 300,
    expected_snapshot_id: str | None = None,
    expected_shared_truth_generation_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    normalized_target = _normalize_identity(target_identity or {})
    budget_key = _budget_key(action_type=action_type, target_identity=normalized_target)
    recovery_attempt_id = _attempt_id(action_type=action_type, budget_key=budget_key, now=actual_now)

    validation = validate_track_b_pre_action_snapshot(
        config=config.validator_config(),
        expected_plan_classification=expected_plan_classification,
        expected_action_type=action_type,
        expected_target_identity=normalized_target,
        max_snapshot_age_seconds=max_snapshot_age_seconds,
        expected_snapshot_id=expected_snapshot_id,
        expected_shared_truth_generation_id=expected_shared_truth_generation_id,
        now=actual_now,
    )
    budget = _budget_summary(
        config=config,
        action_type=action_type,
        budget_key=budget_key,
        now=actual_now,
    )
    would_mutate_runtime = action_type in {"RUNTIME_RETRY", "MARKET_DATA_RESTART"}
    would_mutate_broker = action_type in {"SCOPED_POSITION_CLEANUP", "MANAGED_ORDER_MODIFY", "TARGETED_CANCEL_REPLACE"}
    would_mutate_lifecycle = action_type in {"SCOPED_POSITION_CLEANUP"}

    classification = EXECUTOR_DRY_RUN_READY
    blockers: list[dict[str, str]] = []
    reason = "Dry-run executor envelope is ready; execution remains disabled."
    if action_type not in SUPPORTED_ACTION_TYPES:
        classification = EXECUTOR_BLOCKED_UNSUPPORTED_ACTION
        reason = "Action type is not supported by the dry-run PAPER executor framework."
        blockers.append({"code": "unsupported_action_type", "detail": action_type})
    elif validation.get("classification") != PRE_ACTION_SNAPSHOT_VALID:
        classification = EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION
        reason = "Pre-action Control Plane Snapshot validation blocked the attempt."
        blockers.append({"code": "pre_action_validation", "detail": str(validation.get("classification") or "")})
    elif budget["budget_exhausted"] is True:
        classification = EXECUTOR_BLOCKED_BUDGET_EXHAUSTED
        reason = "Dry-run recovery budget is exhausted for this action/target window."
        blockers.append({"code": "budget_exhausted", "detail": budget_key})

    return {
        "schema_version": "track_b_paper_autonomous_recovery_executor_attempt_v1",
        "recovery_attempt_id": recovery_attempt_id,
        "generated_at": actual_now.isoformat(),
        "policy_mode": "PAPER",
        "dry_run": True,
        "framework_only": True,
        "execution_enabled": False,
        "control_plane_snapshot_id": validation.get("control_plane_snapshot_id") or "",
        "shared_truth_generation_id": validation.get("shared_truth_refresh_generation_id") or "",
        "action_type": action_type,
        "expected_plan_classification": expected_plan_classification,
        "target_identity": normalized_target,
        "budget_key": budget_key,
        "budget": budget,
        "pre_action_validation": validation,
        "pre_action_evidence": {
            "control_plane_snapshot_id": validation.get("control_plane_snapshot_id") or "",
            "shared_truth_refresh_generation_id": validation.get("shared_truth_refresh_generation_id") or "",
            "snapshot_coherence_status": validation.get("snapshot_coherence_status") or "",
            "supervisor_decision_id": validation.get("supervisor_decision_id") or "",
            "supervisor_classification": validation.get("supervisor_classification") or "",
            "planner_classification": validation.get("planner_classification") or "",
            "planner_action_type": validation.get("planner_action_type") or "",
        },
        "post_action_evidence": {
            "placeholder": True,
            "reason": "v1 is dry-run/framework only; no post-action mutation or verification is performed.",
        },
        "would_mutate_runtime": would_mutate_runtime,
        "would_mutate_broker": would_mutate_broker,
        "would_mutate_lifecycle": would_mutate_lifecycle,
        "broker_mutation_allowed": False,
        "lifecycle_mutation_allowed": False,
        "runtime_restart_allowed": False,
        "submit_authority": False,
        "cancel_authority": False,
        "replace_authority": False,
        "modify_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "prohibited_actions": [
            "live_money_route",
            "broad_cancel",
            "broad_flatten",
            "submit_order",
            "cancel_order",
            "replace_order",
            "modify_order",
            "runtime_restart",
            "hidden_recovery",
            "dashboard_projection_authority",
            "paper_proof_bypass",
        ],
        "classification": classification,
        "result_classification": classification,
        "reason": reason,
        "blockers": blockers,
        "source_artifact_paths": {
            "attempt_report": str(_attempt_path(config=config, recovery_attempt_id=recovery_attempt_id)),
            "latest_report": str(config.resolve(config.latest_report_path)),
            "event_log": str(config.resolve(config.event_log_path)),
        },
    }


def write_track_b_paper_autonomous_recovery_executor_attempt(
    *,
    config: TrackBPaperAutonomousRecoveryExecutorConfig,
    payload: Mapping[str, Any],
) -> dict[str, Path]:
    attempts_dir = config.resolve(config.attempts_dir)
    event_log_path = config.resolve(config.event_log_path)
    latest_report_path = config.resolve(config.latest_report_path)
    attempt_path = _attempt_path(config=config, recovery_attempt_id=str(payload["recovery_attempt_id"]))
    attempts_dir.mkdir(parents=True, exist_ok=True)
    _write_json_atomic(attempt_path, payload)
    _write_json_atomic(latest_report_path, payload)
    _append_jsonl(event_log_path, _event_row(payload))
    return {"attempt": attempt_path, "latest": latest_report_path, "event_log": event_log_path}


def run_track_b_paper_autonomous_recovery_executor_dry_run(
    *,
    config: TrackBPaperAutonomousRecoveryExecutorConfig,
    expected_plan_classification: str,
    action_type: str,
    target_identity: Mapping[str, Any] | None = None,
    max_snapshot_age_seconds: int = 300,
    expected_snapshot_id: str | None = None,
    expected_shared_truth_generation_id: str | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    payload = build_track_b_paper_autonomous_recovery_executor_attempt(
        config=config,
        expected_plan_classification=expected_plan_classification,
        action_type=action_type,
        target_identity=target_identity,
        max_snapshot_age_seconds=max_snapshot_age_seconds,
        expected_snapshot_id=expected_snapshot_id,
        expected_shared_truth_generation_id=expected_shared_truth_generation_id,
        now=now,
    )
    paths = write_track_b_paper_autonomous_recovery_executor_attempt(config=config, payload=payload)
    result = dict(payload)
    result["written_artifacts"] = {key: str(path) for key, path in paths.items()}
    return result


def _budget_summary(
    *,
    config: TrackBPaperAutonomousRecoveryExecutorConfig,
    action_type: str,
    budget_key: str,
    now: datetime,
) -> dict[str, Any]:
    window_start = now - timedelta(seconds=config.budget_window_seconds)
    rows = _event_rows(config.resolve(config.event_log_path))
    target_attempts = 0
    window_attempts = 0
    for row in rows:
        row_time = _parse_datetime(row.get("generated_at"))
        if row_time is None or row_time < window_start:
            continue
        if row.get("action_type") == action_type:
            window_attempts += 1
        if row.get("budget_key") == budget_key:
            target_attempts += 1
    target_remaining = max(0, config.max_attempts_per_target - target_attempts)
    window_remaining = max(0, config.max_attempts_per_window - window_attempts)
    return {
        "ledger_path": str(config.resolve(config.event_log_path)),
        "max_attempts_per_target": config.max_attempts_per_target,
        "max_attempts_per_window": config.max_attempts_per_window,
        "budget_window_seconds": config.budget_window_seconds,
        "attempts_for_target_in_window": target_attempts,
        "attempts_for_action_in_window": window_attempts,
        "remaining_attempts_for_target": target_remaining,
        "remaining_attempts_for_window": window_remaining,
        "budget_exhausted": target_remaining <= 0 or window_remaining <= 0,
    }


def _event_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    rows: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(payload, dict):
            rows.append(payload)
    return rows


def _event_row(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "generated_at": payload.get("generated_at"),
        "recovery_attempt_id": payload.get("recovery_attempt_id"),
        "classification": payload.get("classification"),
        "action_type": payload.get("action_type"),
        "budget_key": payload.get("budget_key"),
        "control_plane_snapshot_id": payload.get("control_plane_snapshot_id"),
        "shared_truth_generation_id": payload.get("shared_truth_generation_id"),
        "execution_enabled": False,
    }


def _attempt_id(*, action_type: str, budget_key: str, now: datetime) -> str:
    compact_time = now.strftime("%Y%m%dT%H%M%SZ")
    safe_action = _safe_slug(action_type)
    safe_budget = _safe_slug(budget_key)[0:48]
    return f"paper-recovery-{compact_time}-{safe_action}-{safe_budget}"


def _attempt_path(*, config: TrackBPaperAutonomousRecoveryExecutorConfig, recovery_attempt_id: str) -> Path:
    return config.resolve(config.attempts_dir) / f"{_safe_slug(recovery_attempt_id)}.json"


def _budget_key(*, action_type: str, target_identity: Mapping[str, str]) -> str:
    if not target_identity:
        return action_type
    parts = [action_type]
    for key in sorted(target_identity):
        parts.append(f"{key}={target_identity[key]}")
    return "|".join(parts)


def _normalize_identity(identity: Mapping[str, Any]) -> dict[str, str]:
    normalized: dict[str, str] = {}
    for key, value in identity.items():
        if value is None or value == "":
            continue
        normalized[str(key)] = str(value)
    return normalized


def _safe_slug(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value).strip("_") or "unknown"


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(dict(payload), sort_keys=True) + "\n")


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
