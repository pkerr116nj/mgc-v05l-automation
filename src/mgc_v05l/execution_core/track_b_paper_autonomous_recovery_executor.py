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

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import (
    PRE_ACTION_SNAPSHOT_VALID,
    TrackBPreActionSnapshotValidatorConfig,
    validate_track_b_pre_action_snapshot,
)
from mgc_v05l.execution_core.track_b_recovery_budget_ledger import (
    BUDGET_ATTEMPT_CONSUMED_FAILURE,
    BUDGET_ATTEMPT_CONSUMED_SUCCESS,
    DEFAULT_AGENT_ID,
    DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT,
    build_budget_reservation_event,
    target_identity_hash,
    validate_budget_event,
)
from mgc_v05l.execution_core.track_b_recovery_budget_transaction import simulate_recovery_budget_transaction


EXECUTOR_DRY_RUN_READY = "EXECUTOR_DRY_RUN_READY"
EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION = "EXECUTOR_BLOCKED_PRE_ACTION_VALIDATION"
EXECUTOR_BLOCKED_BUDGET_EXHAUSTED = "EXECUTOR_BLOCKED_BUDGET_EXHAUSTED"
EXECUTOR_BLOCKED_UNSUPPORTED_ACTION = "EXECUTOR_BLOCKED_UNSUPPORTED_ACTION"

BUDGET_GATE_PASS = "BUDGET_GATE_PASS"
BUDGET_GATE_BLOCKED_EXHAUSTED = "BUDGET_GATE_BLOCKED_EXHAUSTED"
BUDGET_GATE_BLOCKED_COOLDOWN = "BUDGET_GATE_BLOCKED_COOLDOWN"
BUDGET_GATE_BLOCKED_QUARANTINE = "BUDGET_GATE_BLOCKED_QUARANTINE"
BUDGET_GATE_BLOCKED_MISSING = "BUDGET_GATE_BLOCKED_MISSING"
BUDGET_GATE_NOT_APPLICABLE = "BUDGET_GATE_NOT_APPLICABLE"
BUDGET_GATE_NOT_EVALUATED = "BUDGET_GATE_NOT_EVALUATED"

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
    recovery_budget_ledger_path: Path = DEFAULT_RECOVERY_BUDGET_LEDGER_ARTIFACT
    enable_runtime_retry_adapter: bool = False
    runtime_retry_launcher_path: Path = Path("scripts") / "run_headless_supervised_paper_service.sh"
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
        target_identity=normalized_target,
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
    elif action_type == "RUNTIME_RETRY" and budget["budget_gate_classification"] != BUDGET_GATE_PASS:
        classification = EXECUTOR_BLOCKED_BUDGET_EXHAUSTED
        reason = "Recovery Budget Ledger blocked RUNTIME_RETRY before the disabled adapter boundary."
        blockers.append({"code": "recovery_budget_gate", "detail": str(budget["budget_gate_classification"])})
    elif budget["budget_exhausted"] is True:
        classification = EXECUTOR_BLOCKED_BUDGET_EXHAUSTED
        reason = "Dry-run executor audit budget is exhausted for this action/target window."
        blockers.append({"code": "executor_audit_budget_exhausted", "detail": budget_key})
    adapter_result = _adapter_result(
        config=config,
        action_type=action_type,
        validation=validation,
        budget=budget,
        classification=classification,
        normalized_target=normalized_target,
        recovery_attempt_id=recovery_attempt_id,
        now=actual_now,
    )

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
        "recovery_budget_ledger": {
            "classification": budget.get("ledger_classification"),
            "source_authority_path": budget.get("ledger_authority_path"),
            "attempts_remaining": budget.get("ledger_attempts_remaining"),
            "attempts_used": budget.get("ledger_attempts_used"),
            "budget_exhausted": budget.get("ledger_budget_exhausted"),
            "cooldown_until": budget.get("ledger_cooldown_until"),
            "quarantine_required": budget.get("ledger_quarantine_required"),
            "budget_gate_classification": budget.get("budget_gate_classification"),
        },
        "pre_action_validation": validation,
        "agent_health_top_blockers": list(validation.get("agent_health_top_blockers") or []),
        "action_adapter": adapter_result,
        "pre_action_evidence": {
            "control_plane_snapshot_id": validation.get("control_plane_snapshot_id") or "",
            "shared_truth_refresh_generation_id": validation.get("shared_truth_refresh_generation_id") or "",
            "snapshot_coherence_status": validation.get("snapshot_coherence_status") or "",
            "supervisor_decision_id": validation.get("supervisor_decision_id") or "",
            "supervisor_classification": validation.get("supervisor_classification") or "",
            "planner_classification": validation.get("planner_classification") or "",
            "planner_action_type": validation.get("planner_action_type") or "",
            "agent_health_top_blockers": list(validation.get("agent_health_top_blockers") or []),
            "agent_health_has_duplicate_writer": validation.get("agent_health_has_duplicate_writer") is True,
        },
        "post_action_evidence": {
            "placeholder": True,
            "adapter_apply_result": adapter_result.get("apply_result"),
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
    target_identity: Mapping[str, str],
    now: datetime,
) -> dict[str, Any]:
    window_start = now - timedelta(seconds=config.budget_window_seconds)
    rows = _event_rows(config.resolve(config.event_log_path))
    ledger = _read_json(config.resolve(config.recovery_budget_ledger_path))
    ledger_budget = _ledger_budget_for_action(ledger, action_type=action_type, target_identity=target_identity)
    budget_gate = _budget_gate(ledger=ledger, ledger_budget=ledger_budget, action_type=action_type, now=now)
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
        "recovery_budget_ledger_path": str(config.resolve(config.recovery_budget_ledger_path)),
        "ledger_classification": ledger.get("classification") or "MISSING",
        "ledger_budget_exhausted": ledger_budget.get("budget_exhausted") is True,
        "ledger_attempts_remaining": ledger_budget.get("attempts_remaining"),
        "ledger_attempts_used": ledger_budget.get("attempts_used"),
        "ledger_cooldown_until": ledger_budget.get("cooldown_until"),
        "ledger_quarantine_required": ledger_budget.get("quarantine_required") is True,
        "ledger_authority_path": _mapping(ledger.get("artifact_paths")).get("authority"),
        "budget_gate_classification": budget_gate["classification"],
        "budget_gate_reason": budget_gate["reason"],
        "max_attempts_per_target": config.max_attempts_per_target,
        "max_attempts_per_window": config.max_attempts_per_window,
        "budget_window_seconds": config.budget_window_seconds,
        "attempts_for_target_in_window": target_attempts,
        "attempts_for_action_in_window": window_attempts,
        "remaining_attempts_for_target": target_remaining,
        "remaining_attempts_for_window": window_remaining,
        "budget_exhausted": target_remaining <= 0 or window_remaining <= 0 or ledger_budget.get("budget_exhausted") is True,
    }


def _adapter_result(
    *,
    config: TrackBPaperAutonomousRecoveryExecutorConfig,
    action_type: str,
    validation: Mapping[str, Any],
    budget: Mapping[str, Any],
    classification: str,
    normalized_target: Mapping[str, str],
    recovery_attempt_id: str,
    now: datetime,
) -> dict[str, Any]:
    if action_type != "RUNTIME_RETRY":
        return {
            "action_type": action_type,
            "adapter_name": "NO_ADAPTER_BOUNDARY_V1",
            "adapter_enabled": False,
            "execution_enabled": False,
            "blocked_reason": "ADAPTER_NOT_IMPLEMENTED_FOR_ACTION_TYPE",
            "dry_run_result": {"classification": classification},
            "apply_result": {"placeholder": True, "executed": False},
        }
    command = _runtime_retry_command(config)
    launcher_exists = len(command) >= 2 and Path(command[1]).exists()
    blocked_reason = "ADAPTER_DISABLED"
    if validation.get("classification") != PRE_ACTION_SNAPSHOT_VALID:
        blocked_reason = "PRE_ACTION_VALIDATION_BLOCKED"
    elif budget.get("budget_gate_classification") != BUDGET_GATE_PASS:
        blocked_reason = str(budget.get("budget_gate_classification") or BUDGET_GATE_BLOCKED_MISSING)
    elif validation.get("planner_action_type") != "RUNTIME_RETRY":
        blocked_reason = "PLAN_ACTION_TYPE_MISMATCH"
    elif validation.get("snapshot_safe_to_start_runtime") is not True:
        blocked_reason = "SNAPSHOT_START_NOT_ALLOWED"
    elif not launcher_exists:
        blocked_reason = "LAUNCH_COMMAND_NOT_FOUND"
    elif config.enable_runtime_retry_adapter:
        blocked_reason = "ADAPTER_DISABLED_PENDING_SECOND_ENABLE_FLAG"
    return {
        "action_type": "RUNTIME_RETRY",
        "adapter_name": "RUNTIME_RETRY_DISABLED_V1",
        "adapter_enabled": False,
        "execution_enabled": False,
        "enable_runtime_retry_adapter_requested": config.enable_runtime_retry_adapter,
        "blocked_reason": blocked_reason,
        "would_execute_command": command,
        "launch_command_exists": launcher_exists,
        "control_plane_snapshot_id": validation.get("control_plane_snapshot_id") or "",
        "shared_truth_generation_id": validation.get("shared_truth_refresh_generation_id") or "",
        "budget_key": _budget_key(action_type=action_type, target_identity=normalized_target),
        "would_record_budget_event": _budget_event_preview(
            recovery_attempt_id=recovery_attempt_id,
            validation=validation,
            budget_key=_budget_key(action_type=action_type, target_identity=normalized_target),
            normalized_target=normalized_target,
        ),
        "recovery_budget_transaction_preview": _budget_transaction_preview(
            config=config,
            recovery_attempt_id=recovery_attempt_id,
            validation=validation,
            budget_key=_budget_key(action_type=action_type, target_identity=normalized_target),
            normalized_target=normalized_target,
            now=now,
        ),
        "recovery_budget_classification": budget.get("ledger_classification"),
        "attempts_remaining": budget.get("ledger_attempts_remaining"),
        "attempts_used": budget.get("ledger_attempts_used"),
        "budget_exhausted": budget.get("ledger_budget_exhausted") is True,
        "cooldown_until": budget.get("ledger_cooldown_until"),
        "quarantine_required": budget.get("ledger_quarantine_required") is True,
        "budget_gate_classification": budget.get("budget_gate_classification"),
        "dry_run_result": {
            "classification": classification,
            "validated_runtime_start_allowed": validation.get("snapshot_safe_to_start_runtime") is True,
            "validated_plan_action_type": validation.get("planner_action_type") == "RUNTIME_RETRY",
            "validated_recovery_budget_gate": budget.get("budget_gate_classification") == BUDGET_GATE_PASS,
        },
        "apply_result": {
            "placeholder": True,
            "executed": False,
            "budget_event_appended": False,
            "reason": "RUNTIME_RETRY adapter is wired but disabled by policy in v1.",
        },
    }


def _budget_event_preview(
    *,
    recovery_attempt_id: str,
    validation: Mapping[str, Any],
    budget_key: str,
    normalized_target: Mapping[str, str],
) -> dict[str, Any]:
    attempt_id = recovery_attempt_id or "runtime-retry-preview"
    event = build_budget_reservation_event(
        recovery_attempt_id=attempt_id,
        control_plane_snapshot_id=str(validation.get("control_plane_snapshot_id") or ""),
        shared_truth_generation_id=str(validation.get("shared_truth_refresh_generation_id") or ""),
        action_type="RUNTIME_RETRY",
        budget_key=budget_key,
        agent_id=DEFAULT_AGENT_ID,
        target_identity=normalized_target,
    )
    validation_result = validate_budget_event(event)
    return {
        "event_type": event["event_type"],
        "event": event,
        "validation": validation_result,
        "would_append": False,
        "append_enabled": False,
        "reason": "Dry-run disabled adapter previews reservation semantics but does not append budget events.",
    }


def _budget_transaction_preview(
    *,
    config: TrackBPaperAutonomousRecoveryExecutorConfig,
    recovery_attempt_id: str,
    validation: Mapping[str, Any],
    budget_key: str,
    normalized_target: Mapping[str, str],
    now: datetime,
) -> dict[str, Any]:
    ledger = _read_json(config.resolve(config.recovery_budget_ledger_path))
    common = {
        "ledger": ledger,
        "recovery_attempt_id": recovery_attempt_id or "runtime-retry-preview",
        "control_plane_snapshot_id": str(validation.get("control_plane_snapshot_id") or ""),
        "shared_truth_generation_id": str(validation.get("shared_truth_refresh_generation_id") or ""),
        "action_type": "RUNTIME_RETRY",
        "budget_key": budget_key,
        "agent_id": DEFAULT_AGENT_ID,
        "target_identity": normalized_target,
        "existing_events": [],
        "now": now,
    }
    return {
        "consume_success": simulate_recovery_budget_transaction(
            **common,
            followup_event_type=BUDGET_ATTEMPT_CONSUMED_SUCCESS,
        ),
        "consume_failure": simulate_recovery_budget_transaction(
            **common,
            followup_event_type=BUDGET_ATTEMPT_CONSUMED_FAILURE,
        ),
        "would_append": False,
        "append_enabled": False,
        "reason": "Disabled adapter simulates reserve+consume ordering only; no Recovery Budget events are appended.",
    }


def _runtime_retry_command(config: TrackBPaperAutonomousRecoveryExecutorConfig) -> list[str]:
    launcher = config.resolve(config.runtime_retry_launcher_path)
    return ["bash", str(launcher)]


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


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _ledger_budget_for_action(
    payload: Mapping[str, Any],
    *,
    action_type: str,
    target_identity: Mapping[str, str],
) -> dict[str, Any]:
    wanted_hash = target_identity_hash(target_identity)
    for entry in _list(payload.get("entries")):
        row = _mapping(entry)
        if (
            row.get("agent_id") == DEFAULT_AGENT_ID
            and row.get("action_type") == action_type
            and row.get("target_identity_hash") == wanted_hash
        ):
            return dict(row)
    summary = _mapping(payload.get("summary"))
    return {
        "budget_entry_missing": bool(payload),
        "budget_exhausted": payload.get("budget_exhausted") is True or summary.get("budget_exhausted") is True,
        "attempts_remaining": summary.get("minimum_attempts_remaining"),
        "quarantine_required": payload.get("quarantine_required") is True or summary.get("quarantine_required") is True,
    }


def _budget_gate(
    *,
    ledger: Mapping[str, Any],
    ledger_budget: Mapping[str, Any],
    action_type: str,
    now: datetime,
) -> dict[str, str]:
    if action_type != "RUNTIME_RETRY":
        return {"classification": BUDGET_GATE_NOT_APPLICABLE, "reason": "Budget gate applies only to RUNTIME_RETRY in v1."}
    if not ledger or not ledger.get("classification"):
        return {"classification": BUDGET_GATE_BLOCKED_MISSING, "reason": "Recovery Budget Ledger authority artifact is missing."}
    if ledger_budget.get("budget_entry_missing") is True:
        return {"classification": BUDGET_GATE_BLOCKED_MISSING, "reason": "Recovery Budget Ledger has no matching RUNTIME_RETRY budget entry."}
    if ledger_budget.get("quarantine_required") is True:
        return {"classification": BUDGET_GATE_BLOCKED_QUARANTINE, "reason": "Recovery Budget Ledger requires quarantine."}
    cooldown_until = _parse_datetime(ledger_budget.get("cooldown_until"))
    if cooldown_until is not None and cooldown_until > now:
        return {"classification": BUDGET_GATE_BLOCKED_COOLDOWN, "reason": "Recovery Budget Ledger cooldown is active."}
    if ledger_budget.get("budget_exhausted") is True:
        return {"classification": BUDGET_GATE_BLOCKED_EXHAUSTED, "reason": "Recovery Budget Ledger budget is exhausted."}
    attempts_remaining = ledger_budget.get("attempts_remaining")
    if attempts_remaining is None:
        return {"classification": BUDGET_GATE_BLOCKED_MISSING, "reason": "Recovery Budget Ledger attempts_remaining is missing."}
    try:
        remaining = int(attempts_remaining)
    except (TypeError, ValueError):
        return {"classification": BUDGET_GATE_BLOCKED_MISSING, "reason": "Recovery Budget Ledger attempts_remaining is invalid."}
    if remaining <= 0:
        return {"classification": BUDGET_GATE_BLOCKED_EXHAUSTED, "reason": "Recovery Budget Ledger has no attempts remaining."}
    return {"classification": BUDGET_GATE_PASS, "reason": "Recovery Budget Ledger permits one bounded RUNTIME_RETRY attempt."}


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


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _safe_slug(value: str) -> str:
    return "".join(ch if ch.isalnum() or ch in {"-", "_"} else "_" for ch in value).strip("_") or "unknown"


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as fh:
        fh.write(json.dumps(dict(payload), sort_keys=True) + "\n")


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
