"""Hourly Track B PAPER runtime recovery audit.

The audit is read-only. It records whether an hourly recovery supervisor is
installed/running, consumes the existing self-healing restart plan, and writes
diagnostic artifacts. It does not restart, submit, cancel, close, modify,
flatten, invoke paper_proof, or grant live-money authority.
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic

try:
    import tomllib  # type: ignore[attr-defined]
except ModuleNotFoundError:  # pragma: no cover - Python <3.11 fallback.
    try:
        import tomli as tomllib  # type: ignore[no-redef]
    except ModuleNotFoundError:  # pragma: no cover - optional dependency.
        tomllib = None  # type: ignore[assignment]

UTC = timezone.utc


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_LATEST_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_recovery"
    / "latest_hourly_runtime_recovery_audit.json"
)
DEFAULT_EVENTS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_recovery"
    / "hourly_runtime_recovery_events.jsonl"
)

SUPERVISOR_NOT_INSTALLED = "SUPERVISOR_NOT_INSTALLED"
SUPERVISOR_NOT_RUNNING = "SUPERVISOR_NOT_RUNNING"
SUPERVISOR_PAUSED = "SUPERVISOR_PAUSED"
SUPERVISOR_RUNNING = "SUPERVISOR_RUNNING"
RECOVERY_POLICY_TOO_PASSIVE = "RECOVERY_POLICY_TOO_PASSIVE"
CONTROL_PLANE_REAL_BLOCK = "CONTROL_PLANE_REAL_BLOCK"
STALE_RUNTIME_AUTHORITY = "STALE_RUNTIME_AUTHORITY"
STALE_PROOF_OR_DATA = "STALE_PROOF_OR_DATA"
RUNTIME_HEALTHY_NO_ACTION = "RUNTIME_HEALTHY_NO_ACTION"

NO_AUTHORITY_FLAGS = {
    "read_only": True,
    "submit_authority": False,
    "submit_allowed": False,
    "broker_mutation_allowed": False,
    "lifecycle_authority": False,
    "live_money_eligible": False,
    "paper_proof_invoked": False,
    "runtime_restart_performed": False,
}


@dataclass(frozen=True)
class HourlyRuntimeRecoveryAuditConfig:
    repo_root: Path = REPO_ROOT
    latest_output_path: Path = DEFAULT_LATEST_OUTPUT_PATH
    events_path: Path = DEFAULT_EVENTS_PATH

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_hourly_runtime_recovery_audit(
    *,
    self_healing_health: Mapping[str, Any],
    scheduler_evidence: Mapping[str, Any],
    operability_contract: Mapping[str, Any] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    plan = _mapping(self_healing_health.get("last_restart_plan"))
    runtime_restart_eligible = self_healing_health.get("runtime_restart_eligible") is True
    runtime_restart_blockers = tuple(str(item) for item in self_healing_health.get("runtime_restart_blockers") or [])
    paper_runtime = _mapping(_mapping(self_healing_health.get("agents")).get("paper_runtime"))
    scheduler_classification = classify_scheduler_evidence(scheduler_evidence)
    scheduler_details = _scheduler_details(scheduler_evidence, classification=scheduler_classification)
    root_cause = _root_cause(
        scheduler_classification=scheduler_classification,
        self_healing_health=self_healing_health,
        plan=plan,
        runtime_restart_eligible=runtime_restart_eligible,
        runtime_restart_blockers=runtime_restart_blockers,
        operability_contract=_mapping(operability_contract),
    )
    operability = _mapping(operability_contract)
    return {
        "schema_version": "track_b_hourly_runtime_recovery_audit_v1",
        "generated_at": actual_now.isoformat(),
        "classification": root_cause,
        **NO_AUTHORITY_FLAGS,
        "hourly_supervisor": {
            "classification": scheduler_classification,
            "installed": scheduler_classification != SUPERVISOR_NOT_INSTALLED,
            "running": scheduler_classification == SUPERVISOR_RUNNING,
            "active": scheduler_classification == SUPERVISOR_RUNNING,
            "paused": scheduler_classification == SUPERVISOR_PAUSED,
            "latest_audit_run_at": actual_now.isoformat(),
            "latest_run_time": actual_now.isoformat(),
            "recovery_authoritative": False,
            "authority_reason": "read_only_audit_no_runtime_restart_authority",
            **scheduler_details,
            "evidence": dict(scheduler_evidence),
        },
        "self_healing": {
            "classification": self_healing_health.get("classification"),
            "auto_restart_allowed": self_healing_health.get("auto_restart_allowed") is True,
            "restart_candidates": list(self_healing_health.get("restart_candidates") or []),
            "restart_plan_classification": plan.get("classification"),
            "restart_allowed": plan.get("restart_allowed") is True,
            "paper_runtime_auto_restart_allowed": plan.get("paper_runtime_auto_restart_allowed") is True,
            "runtime_restart_eligible": runtime_restart_eligible,
            "runtime_restart_blockers": list(runtime_restart_blockers),
            "runtime_agent": {
                "health_state": paper_runtime.get("health_state"),
                "process_running": paper_runtime.get("process_running") is True,
                "restart_candidate": paper_runtime.get("restart_candidate") is True,
                "blockers": list(paper_runtime.get("blockers") or []),
            },
            "global_blockers": list(plan.get("global_blockers") or []),
            "blocked": list(plan.get("blocked") or []),
        },
        "restart_decision": _restart_decision(
            root_cause=root_cause,
            plan=plan,
            runtime_restart_eligible=runtime_restart_eligible,
            runtime_restart_blockers=runtime_restart_blockers,
            operability_contract=operability,
        ),
        "canonical_operability": {
            "artifact": operability.get("artifact_path"),
            "state": operability.get("canonical_state"),
            "runtime_healthy": operability.get("canonical_state") == "READY_SUBMIT_CAPABLE",
            "restart_allowed_if_runtime_down": operability.get("restart_allowed_if_runtime_down") is True,
            "blocker_family": operability.get("blocker_family"),
            "blockers": list(operability.get("blockers") or []),
        },
        "root_cause": {
            "classification": root_cause,
            "reason": _root_cause_reason(root_cause),
        },
    }


def collect_hourly_runtime_recovery_audit(
    *,
    config: HourlyRuntimeRecoveryAuditConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    from mgc_v05l.app.track_b_self_healing_supervisor import run_track_b_self_healing_status

    health = run_track_b_self_healing_status(
        repo_root=config.repo_root,
        expected_root=config.repo_root,
        write=False,
        mode="dry-run",
        now=now,
    )
    scheduler = collect_scheduler_evidence(repo_root=config.repo_root)
    from mgc_v05l.execution_core.track_b_runtime_operability_contract import (
        RuntimeOperabilityConfig,
        build_runtime_operability_contract,
        write_runtime_operability_contract,
    )

    operability_config = RuntimeOperabilityConfig(repo_root=config.repo_root)
    operability = build_runtime_operability_contract(config=operability_config, now=now)
    operability["artifact_path"] = str(write_runtime_operability_contract(config=operability_config, payload=operability))
    return build_hourly_runtime_recovery_audit(
        self_healing_health=health,
        scheduler_evidence=scheduler,
        operability_contract=operability,
        now=now,
    )


def collect_scheduler_evidence(*, repo_root: Path) -> dict[str, Any]:
    launchd = _run_command(("launchctl", "list"), repo_root)
    crontab = _run_command(("crontab", "-l"), repo_root)
    labels = _matching_scheduler_lines(launchd.get("stdout", ""))
    cron = _matching_scheduler_lines(crontab.get("stdout", ""))
    codex_automations = _matching_codex_automations()
    return {
        "launchd_query_returncode": launchd.get("returncode"),
        "crontab_query_returncode": crontab.get("returncode"),
        "launchd_matching_labels": labels,
        "crontab_matching_entries": cron,
        "codex_automation_matching_entries": codex_automations,
        "launchd_query_error": launchd.get("stderr_tail"),
        "crontab_query_error": crontab.get("stderr_tail"),
    }


def write_hourly_runtime_recovery_audit(
    *,
    config: HourlyRuntimeRecoveryAuditConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.latest_output_path)
    write_json_atomic(output_path, to_jsonable(dict(payload)))
    _append_jsonl(config.resolve(config.events_path), payload)
    return output_path


def classify_scheduler_evidence(evidence: Mapping[str, Any]) -> str:
    labels = list(evidence.get("launchd_matching_labels") or [])
    cron = list(evidence.get("crontab_matching_entries") or [])
    codex_automations = list(evidence.get("codex_automation_matching_entries") or [])
    if not labels and not cron and not codex_automations:
        return SUPERVISOR_NOT_INSTALLED
    if any(str(item.get("status", "")).upper() == "ACTIVE" for item in codex_automations if isinstance(item, Mapping)):
        return SUPERVISOR_RUNNING
    running = any(_looks_running(line) for line in labels)
    if running:
        return SUPERVISOR_RUNNING
    if codex_automations and all(
        str(item.get("status", "")).upper() == "PAUSED" for item in codex_automations if isinstance(item, Mapping)
    ):
        return SUPERVISOR_PAUSED
    return SUPERVISOR_NOT_RUNNING


def _scheduler_details(evidence: Mapping[str, Any], *, classification: str) -> dict[str, Any]:
    codex_automations = [
        item for item in list(evidence.get("codex_automation_matching_entries") or []) if isinstance(item, Mapping)
    ]
    statuses = [str(item.get("status") or "").upper() for item in codex_automations]
    return {
        "codex_automation_count": len(codex_automations),
        "codex_automation_statuses": statuses,
        "codex_automation_active_count": sum(1 for status in statuses if status == "ACTIVE"),
        "codex_automation_paused_count": sum(1 for status in statuses if status == "PAUSED"),
        "launchd_match_count": len(list(evidence.get("launchd_matching_labels") or [])),
        "crontab_match_count": len(list(evidence.get("crontab_matching_entries") or [])),
        "status_truth_source": "live_scheduler_probe",
        "stale_artifact_can_claim_active": False,
        "should_remain_paused": classification == SUPERVISOR_PAUSED,
    }


def _root_cause(
    *,
    scheduler_classification: str,
    self_healing_health: Mapping[str, Any],
    plan: Mapping[str, Any],
    runtime_restart_eligible: bool,
    runtime_restart_blockers: Sequence[str],
    operability_contract: Mapping[str, Any] | None = None,
) -> str:
    operability = _mapping(operability_contract)
    if scheduler_classification == SUPERVISOR_NOT_INSTALLED:
        return SUPERVISOR_NOT_INSTALLED
    if scheduler_classification == SUPERVISOR_PAUSED:
        return SUPERVISOR_PAUSED
    if scheduler_classification == SUPERVISOR_NOT_RUNNING:
        return SUPERVISOR_NOT_RUNNING
    if operability.get("canonical_state") == "BLOCKED_STALE_TRUTH":
        return STALE_RUNTIME_AUTHORITY
    if str(operability.get("canonical_state") or "").startswith("BLOCKED_"):
        return CONTROL_PLANE_REAL_BLOCK
    if runtime_restart_eligible and plan.get("restart_allowed") is True:
        return RECOVERY_POLICY_TOO_PASSIVE
    if any("runtime" in blocker and "stale" in blocker for blocker in runtime_restart_blockers):
        return STALE_RUNTIME_AUTHORITY
    if any("stale" in str(blocker).lower() or "data" in str(blocker).lower() for blocker in runtime_restart_blockers):
        return STALE_PROOF_OR_DATA
    if runtime_restart_blockers:
        return CONTROL_PLANE_REAL_BLOCK
    if self_healing_health.get("classification") == "SELF_HEALING_READY":
        return RUNTIME_HEALTHY_NO_ACTION
    return CONTROL_PLANE_REAL_BLOCK


def _restart_decision(
    *,
    root_cause: str,
    plan: Mapping[str, Any],
    runtime_restart_eligible: bool,
    runtime_restart_blockers: Sequence[str],
    operability_contract: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    operability = _mapping(operability_contract)
    if operability.get("canonical_state") == "READY_SUBMIT_CAPABLE":
        return {
            "classification": "RUNTIME_HEALTHY_NO_RESTART_NEEDED",
            "action": "none",
            "blockers": [],
            "authority_source": "canonical_operability",
        }
    if operability and operability.get("restart_allowed_if_runtime_down") is not True:
        return {
            "classification": "RUNTIME_RESTART_NOT_ALLOWED_BY_CANONICAL_OPERABILITY",
            "action": "write_blocker_report",
            "blockers": list(operability.get("blockers") or []) or list(runtime_restart_blockers),
        }
    if runtime_restart_eligible and plan.get("restart_allowed") is True:
        return {
            "classification": "RUNTIME_RESTART_RECOMMENDED_BY_EXISTING_SELF_HEALING_PLAN",
            "action": "run_track_b_self_healing_supervisor_apply",
            "operator_authorization_required": False,
            "blockers": [],
        }
    if root_cause == RUNTIME_HEALTHY_NO_ACTION:
        return {"classification": "RUNTIME_HEALTHY_NO_RESTART_NEEDED", "action": "none", "blockers": []}
    return {
        "classification": "RUNTIME_RESTART_NOT_ALLOWED",
        "action": "write_blocker_report",
        "blockers": list(runtime_restart_blockers) or list(plan.get("global_blockers") or []),
    }


def _root_cause_reason(root_cause: str) -> str:
    return {
        SUPERVISOR_NOT_INSTALLED: (
            "No hourly Track B runtime recovery crontab entry, launchd job, or Codex automation was found."
        ),
        SUPERVISOR_PAUSED: "The hourly Track B runtime recovery automation is installed but paused.",
        SUPERVISOR_NOT_RUNNING: "A scheduler definition was found, but no running hourly recovery supervisor was evident.",
        RECOVERY_POLICY_TOO_PASSIVE: "Existing self-healing policy can recommend a runtime restart, but no hourly runner applied it.",
        CONTROL_PLANE_REAL_BLOCK: "Current self-healing/control-plane evidence blocks restart.",
        STALE_RUNTIME_AUTHORITY: "Runtime authority evidence is stale or conflicting.",
        STALE_PROOF_OR_DATA: "Current proof/data evidence needs refresh before restart.",
        RUNTIME_HEALTHY_NO_ACTION: "Runtime is currently healthy; no restart is needed.",
    }.get(root_cause, "Unclassified runtime recovery state.")


def _matching_scheduler_lines(text: str) -> list[str]:
    rows = []
    for line in text.splitlines():
        lowered = line.lower()
        if _is_track_b_hourly_runtime_recovery_scheduler(lowered):
            rows.append(line.strip())
    return rows


def _matching_codex_automations() -> list[dict[str, Any]]:
    automations_root = Path(os.environ.get("CODEX_HOME", Path.home() / ".codex")) / "automations"
    if not automations_root.exists():
        return []
    if tomllib is None:
        return []
    rows: list[dict[str, Any]] = []
    for automation_path in sorted(automations_root.glob("*/automation.toml")):
        try:
            payload = tomllib.loads(automation_path.read_text(encoding="utf-8"))
        except Exception:
            continue
        searchable = " ".join(
            str(payload.get(key, ""))
            for key in (
                "id",
                "name",
                "prompt",
                "rrule",
                "status",
            )
        ).lower()
        if not _is_track_b_hourly_runtime_recovery_scheduler(searchable):
            continue
        rows.append(
            {
                "id": payload.get("id"),
                "name": payload.get("name"),
                "path": str(automation_path),
                "rrule": payload.get("rrule"),
                "status": payload.get("status"),
            }
        )
    return rows


def _is_track_b_hourly_runtime_recovery_scheduler(lowered_line: str) -> bool:
    if not any(token in lowered_line for token in ("mgc", "track_b", "track-b")):
        return False
    if "sunday_preflight" in lowered_line:
        return False
    return any(
        token in lowered_line
        for token in (
            "hourly_runtime_recovery",
            "runtime_recovery",
            "runtime-recovery",
            "runtime recovery",
            "self_healing",
            "self-healing",
            "track_b_hourly",
            "paper_runtime_recovery",
            "paper-runtime-recovery",
            "paper runtime recovery",
        )
    )


def _looks_running(launchd_line: str) -> bool:
    first = launchd_line.split(maxsplit=1)[0] if launchd_line.split() else "-"
    return first.isdigit()


def _run_command(command: Sequence[str], cwd: Path) -> dict[str, Any]:
    try:
        proc = subprocess.run(list(command), cwd=str(cwd), capture_output=True, text=True, check=False, timeout=20)
    except Exception as exc:  # noqa: BLE001 - diagnostic command failures become evidence.
        return {"command": list(command), "returncode": 124, "stdout": "", "stderr_tail": str(exc)[-2000:]}
    return {
        "command": list(command),
        "returncode": proc.returncode,
        "stdout": proc.stdout,
        "stderr_tail": proc.stderr[-2000:],
    }


def _mapping(value: object) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(dict(payload)), sort_keys=True) + "\n")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Audit hourly Track B PAPER runtime recovery.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = HourlyRuntimeRecoveryAuditConfig(repo_root=args.repo_root.expanduser().resolve())
    payload = collect_hourly_runtime_recovery_audit(config=config)
    write_hourly_runtime_recovery_audit(config=config, payload=payload)
    if args.json:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(f"{payload['classification']}: {payload['root_cause']['reason']}")
    return 0


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
