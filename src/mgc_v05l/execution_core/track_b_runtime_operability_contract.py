"""Canonical Track B PAPER runtime operability contract.

This module is read-only glue over the existing Track B truth surfaces.  It
does not start, stop, submit, cancel, close, modify, flatten, or alter runtime
configuration.  Its job is to make the operator/recovery answer boring:

* which artifacts are authority,
* which artifacts are diagnostic projections,
* which stale/deprecated surfaces are quarantined, and
* whether the current runtime state is submit capable, diagnostic only, or
  blocked with one clear blocker family.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.models import to_jsonable
from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic


REPO_ROOT = Path(__file__).resolve().parents[3]

READY_SUBMIT_CAPABLE = "READY_SUBMIT_CAPABLE"
READY_DIAGNOSTIC_ONLY = "READY_DIAGNOSTIC_ONLY"
READY_TO_START_DIAGNOSTIC_ONLY = "READY_TO_START_DIAGNOSTIC_ONLY"
WAITING_FOR_MARKET_REOPEN = "WAITING_FOR_MARKET_REOPEN"
BLOCKED_SAFETY = "BLOCKED_SAFETY"
BLOCKED_INFRASTRUCTURE = "BLOCKED_INFRASTRUCTURE"
BLOCKED_CONFIG = "BLOCKED_CONFIG"
BLOCKED_STALE_TRUTH = "BLOCKED_STALE_TRUTH"

AUTHORITY_ACTIVE = "active_authority"
DIAGNOSTIC_ONLY = "active_diagnostic_only"
STALE_DEPRECATED = "stale_deprecated"
GENERATED_EVIDENCE_ONLY = "generated_evidence_only"
GENERATED_PROJECTION_ONLY = "generated_projection_only"

DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_operability"
    / "latest_runtime_operability_contract.json"
)
DEFAULT_EVENTS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_operability"
    / "runtime_operability_events.jsonl"
)

DEFAULT_CANONICAL_READINESS_PATH = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
)
DEFAULT_CANONICAL_READINESS_SUMMARY_PATH = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness_summary.json"
)
DEFAULT_PAPER_CONFIG_IN_FORCE_PATH = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "paper_config_in_force.json"
)
DEFAULT_PAPER_RUNTIME_TRUTH_PATH = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "paper_runtime_truth.json"
)
DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_supervisor"
    / "latest_runtime_supervisor_authority.json"
)
DEFAULT_HOURLY_RECOVERY_AUDIT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_recovery"
    / "latest_hourly_runtime_recovery_audit.json"
)
DEFAULT_BROKER_RECONCILIATION_PATH = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_OPERATOR_DASHBOARD_READINESS_PATH = (
    Path("outputs") / "operator_dashboard" / "runtime" / "operator_dashboard_readiness.json"
)
DEFAULT_QUARANTINED_STALE_RUNTIME_DIR = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "_quarantine_stale_launch_metadata_20260522_ee53089"
)

AUTHORITY_STALE_SECONDS = 300.0
RECOVERY_STALE_SECONDS = 3900.0


@dataclass(frozen=True)
class RuntimeOperabilityConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    events_path: Path = DEFAULT_EVENTS_PATH
    canonical_readiness_path: Path = DEFAULT_CANONICAL_READINESS_PATH
    canonical_readiness_summary_path: Path = DEFAULT_CANONICAL_READINESS_SUMMARY_PATH
    paper_config_in_force_path: Path = DEFAULT_PAPER_CONFIG_IN_FORCE_PATH
    paper_runtime_truth_path: Path = DEFAULT_PAPER_RUNTIME_TRUTH_PATH
    runtime_supervisor_authority_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_PATH
    hourly_recovery_audit_path: Path = DEFAULT_HOURLY_RECOVERY_AUDIT_PATH
    broker_reconciliation_path: Path = DEFAULT_BROKER_RECONCILIATION_PATH
    operator_dashboard_readiness_path: Path = DEFAULT_OPERATOR_DASHBOARD_READINESS_PATH
    quarantined_stale_runtime_dir: Path = DEFAULT_QUARANTINED_STALE_RUNTIME_DIR

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_runtime_operability_contract(
    *,
    config: RuntimeOperabilityConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    surfaces = build_runtime_authority_map(config=config, now=actual_now)
    payloads = {
        key: _read_json(Path(row["path"]))
        for key, row in surfaces.items()
        if row["classification"] in {AUTHORITY_ACTIVE, DIAGNOSTIC_ONLY, GENERATED_PROJECTION_ONLY}
    }
    classification = classify_runtime_operability(
        surfaces=surfaces,
        payloads=payloads,
        now=actual_now,
    )
    return {
        "schema_version": "track_b_runtime_operability_contract_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "broker_mutation_allowed": False,
        "lifecycle_authority": False,
        "runtime_mutation_allowed": False,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "canonical_state": classification["state"],
        "ready_submit_capable": classification["state"] == READY_SUBMIT_CAPABLE,
        "submit_allowed": classification.get("submit_allowed") is True,
        "runtime_start_allowed": classification.get("runtime_start_allowed") is True,
        "restart_allowed_if_runtime_down": classification["restart_allowed_if_runtime_down"],
        "market_schedule_state": classification.get("market_schedule_state"),
        "stale_market_data_expected": classification.get("stale_market_data_expected") is True,
        "next_expected_reopen_time": classification.get("next_expected_reopen_time"),
        "market_data_grace_until": classification.get("market_data_grace_until"),
        "readiness_block_is_scheduled_halt": classification.get("readiness_block_is_scheduled_halt") is True,
        "blocker_family": classification["blocker_family"],
        "blockers": classification["blockers"],
        "warnings": classification["warnings"],
        "operator_command": (
            "PYTHONPATH=src ./.venv/bin/python -m "
            "mgc_v05l.execution_core.track_b_runtime_operability_contract "
            "--repo-root /Users/patrick/Dev/MGC-v05l-automation --json"
        ),
        "authority_map": surfaces,
        "deprecated_surface_quarantine": _deprecated_surface_quarantine(surfaces),
        "artifact_freshness_policy": {
            "authority_artifacts": "Stale authority artifacts block as BLOCKED_STALE_TRUTH.",
            "diagnostic_projections": "Diagnostic projections may warn, but cannot grant authority.",
            "generated_evidence": "Replay, research, historical recovery, and report outputs are evidence only and cannot block runtime startup.",
            "deprecated_surfaces": "Quarantined surfaces cannot imply submit authority, runtime health, or active config.",
        },
        "runtime_summary": classification["runtime_summary"],
        "config_summary": classification["config_summary"],
        "recovery_summary": classification["recovery_summary"],
    }


def build_runtime_authority_map(
    *,
    config: RuntimeOperabilityConfig,
    now: datetime,
) -> dict[str, dict[str, Any]]:
    def row(
        key: str,
        path: Path,
        classification: str,
        purpose: str,
        *,
        stale_after_seconds: float | None = None,
        can_support_submit_capable_classification: bool = False,
        can_block_runtime: bool = False,
    ) -> tuple[str, dict[str, Any]]:
        resolved = config.resolve(path)
        freshness = _freshness(resolved, now=now, stale_after_seconds=stale_after_seconds)
        return key, {
            "path": str(resolved),
            "classification": classification,
            "purpose": purpose,
            "exists": resolved.exists(),
            "freshness": freshness,
            "can_support_submit_capable_classification": can_support_submit_capable_classification,
            "can_block_runtime": can_block_runtime,
            "operator_dashboard_authority": False,
        }

    rows = [
        row(
            "active_paper_config",
            config.paper_config_in_force_path,
            AUTHORITY_ACTIVE,
            "Only active PAPER lane/config source in force.",
            stale_after_seconds=None,
            can_block_runtime=True,
        ),
        row(
            "runtime_truth",
            config.paper_runtime_truth_path,
            AUTHORITY_ACTIVE,
            "Current PAPER runtime process/generation/lane truth.",
            stale_after_seconds=AUTHORITY_STALE_SECONDS,
            can_block_runtime=True,
        ),
        row(
            "canonical_readiness",
            config.canonical_readiness_path,
            AUTHORITY_ACTIVE,
            "Canonical readiness detail used by operators and recovery.",
            stale_after_seconds=AUTHORITY_STALE_SECONDS,
            can_support_submit_capable_classification=True,
            can_block_runtime=True,
        ),
        row(
            "canonical_readiness_summary",
            config.canonical_readiness_summary_path,
            GENERATED_PROJECTION_ONLY,
            "Compact projection of canonical readiness for display.",
            stale_after_seconds=AUTHORITY_STALE_SECONDS,
        ),
        row(
            "broker_lifecycle_reconciliation",
            config.broker_reconciliation_path,
            AUTHORITY_ACTIVE,
            "Broker/lifecycle reconciliation truth consumed by readiness.",
            stale_after_seconds=AUTHORITY_STALE_SECONDS,
            can_block_runtime=True,
        ),
        row(
            "runtime_supervisor_authority",
            config.runtime_supervisor_authority_path,
            DIAGNOSTIC_ONLY,
            "Read-only supervisor advisory; recommends but does not execute runtime actions.",
            stale_after_seconds=AUTHORITY_STALE_SECONDS,
        ),
        row(
            "hourly_recovery_audit",
            config.hourly_recovery_audit_path,
            DIAGNOSTIC_ONLY,
            "Hourly recovery scheduler/audit evidence.",
            stale_after_seconds=RECOVERY_STALE_SECONDS,
        ),
        row(
            "operator_dashboard_readiness",
            config.operator_dashboard_readiness_path,
            GENERATED_PROJECTION_ONLY,
            "Dashboard display projection only; dashboard is not on the critical runtime path.",
            stale_after_seconds=AUTHORITY_STALE_SECONDS,
        ),
        row(
            "quarantined_stale_launch_metadata",
            config.quarantined_stale_runtime_dir,
            STALE_DEPRECATED,
            "Quarantined legacy launch/runtime metadata; cannot imply authority or health.",
        ),
    ]
    return dict(rows)


def classify_runtime_operability(
    *,
    surfaces: Mapping[str, Mapping[str, Any]],
    payloads: Mapping[str, Mapping[str, Any]],
    now: datetime | None = None,
) -> dict[str, Any]:
    _ = now
    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    def block(code: str, detail: str, source: str, family: str) -> None:
        blockers.append({"code": code, "detail": detail, "source": source, "family": family})

    def warn(code: str, detail: str, source: str) -> None:
        warnings.append({"code": code, "detail": detail, "source": source})

    canonical = _mapping(payloads.get("canonical_readiness"))
    runtime_truth = _mapping(payloads.get("runtime_truth"))
    config = _mapping(payloads.get("active_paper_config"))
    reconciliation = _mapping(payloads.get("broker_lifecycle_reconciliation"))
    supervisor = _mapping(payloads.get("runtime_supervisor_authority"))
    recovery = _mapping(payloads.get("hourly_recovery_audit"))

    for key, row in surfaces.items():
        if row.get("classification") == STALE_DEPRECATED:
            continue
        if row.get("can_block_runtime") and not row.get("exists"):
            block(
                "authority_artifact_missing",
                f"Required authority artifact is missing: {key}",
                key,
                BLOCKED_STALE_TRUTH,
            )
            continue
        freshness = _mapping(row.get("freshness"))
        if row.get("can_block_runtime") and freshness.get("stale") is True:
            block(
                "authority_artifact_stale",
                f"Required authority artifact is stale: {key}",
                key,
                BLOCKED_STALE_TRUTH,
            )
        elif freshness.get("stale") is True:
            warn("diagnostic_artifact_stale", f"Diagnostic artifact is stale: {key}", key)

    if _bool(canonical.get("live_money_eligible")) or _bool(runtime_truth.get("live_money_eligible")):
        block(
            "live_money_eligible_true",
            "live_money_eligible=true is forbidden for Track B PAPER.",
            "paper_safety",
            BLOCKED_SAFETY,
        )
    if _bool(canonical.get("paper_proof_invoked")) or _bool(runtime_truth.get("paper_proof_invoked")):
        block(
            "paper_proof_invoked_true",
            "paper_proof_invoked=true is not allowed for this PAPER runtime.",
            "paper_safety",
            BLOCKED_SAFETY,
        )
    if _bool(canonical.get("broker_mutation_allowed")):
        block(
            "unguarded_broker_mutation_allowed",
            "Canonical readiness exposed broker_mutation_allowed=true.",
            "paper_safety",
            BLOCKED_SAFETY,
        )

    readiness_state = str(canonical.get("canonical_readiness") or canonical.get("state") or "")
    readiness_blockers = list(canonical.get("readiness_blockers") or canonical.get("blockers") or [])
    if readiness_state in {"NOT_READY_CONFIG", "NOT_READY_WRONG_ROOT"}:
        block("canonical_readiness_config_block", readiness_state, "canonical_readiness", BLOCKED_CONFIG)
    elif readiness_state in {WAITING_FOR_MARKET_REOPEN, READY_TO_START_DIAGNOSTIC_ONLY}:
        warn("canonical_readiness_waiting_for_market_reopen", readiness_state, "canonical_readiness")
    elif readiness_state in {"NOT_READY_DEPENDENCY", "NOT_READY_RECONCILIATION", "DEGRADED_NO_SUBMIT"}:
        block("canonical_readiness_infrastructure_block", readiness_state, "canonical_readiness", BLOCKED_INFRASTRUCTURE)
    elif readiness_state == "READY_OBSERVATION_ONLY":
        warn("canonical_readiness_observation_only", readiness_state, "canonical_readiness")
    elif readiness_state and readiness_state != READY_SUBMIT_CAPABLE:
        block("canonical_readiness_unknown_block", readiness_state, "canonical_readiness", BLOCKED_INFRASTRUCTURE)

    runtime_running = runtime_truth.get("running")
    if runtime_running is None:
        runtime_running = runtime_truth.get("process_running")
    if runtime_running is None:
        runtime_running = _runtime_truth_implies_running(runtime_truth)
    lane_count = _int(config.get("lane_count"), default=None)
    if lane_count is None:
        lanes = config.get("lanes") or config.get("enabled_lanes") or []
        lane_count = len(lanes) if isinstance(lanes, list) else 0
    runtime_lane_count = _int(runtime_truth.get("lane_count"), default=0)

    if lane_count <= 0:
        block("active_paper_config_empty", "Active paper_config_in_force has no lanes.", "active_paper_config", BLOCKED_CONFIG)
    runtime_down_with_clean_safety = runtime_running is False and readiness_state in {
        READY_SUBMIT_CAPABLE,
        READY_TO_START_DIAGNOSTIC_ONLY,
    }
    if runtime_down_with_clean_safety:
        warn("runtime_down_but_safety_clean", "Runtime is down while canonical readiness is clean.", "runtime_truth")
    if runtime_lane_count and lane_count and runtime_lane_count != lane_count:
        block(
            "runtime_config_lane_count_mismatch",
            f"Runtime lane_count {runtime_lane_count} does not match active config lane_count {lane_count}.",
            "runtime_truth",
            BLOCKED_STALE_TRUTH,
        )

    if reconciliation:
        recon_state = str(reconciliation.get("classification") or reconciliation.get("reconciliation_state") or "")
        if "BROKER_RECONCILED" not in recon_state and "RECONCILED" not in recon_state:
            warn("reconciliation_not_cleanly_reconciled", recon_state or "unknown", "broker_lifecycle_reconciliation")

    if blockers:
        family = _highest_precedence_family(blockers)
        state = family
    elif runtime_down_with_clean_safety:
        state = READY_DIAGNOSTIC_ONLY
        family = None
    elif readiness_state == READY_SUBMIT_CAPABLE:
        state = READY_SUBMIT_CAPABLE
        family = None
    elif readiness_state in {WAITING_FOR_MARKET_REOPEN, READY_TO_START_DIAGNOSTIC_ONLY}:
        state = READY_DIAGNOSTIC_ONLY
        family = None
    else:
        state = READY_DIAGNOSTIC_ONLY
        family = None

    runtime_start_allowed = state == READY_SUBMIT_CAPABLE or (
        state == READY_DIAGNOSTIC_ONLY and str(supervisor.get("safe_to_start_runtime")).lower() == "true"
    )
    if readiness_state in {READY_TO_START_DIAGNOSTIC_ONLY, WAITING_FOR_MARKET_REOPEN}:
        runtime_start_allowed = True
    restart_allowed = runtime_start_allowed
    if state in {BLOCKED_SAFETY, BLOCKED_CONFIG, BLOCKED_STALE_TRUTH, BLOCKED_INFRASTRUCTURE}:
        restart_allowed = False
        runtime_start_allowed = False
    elif readiness_state == WAITING_FOR_MARKET_REOPEN:
        restart_allowed = False
    elif runtime_down_with_clean_safety:
        restart_allowed = True
        runtime_start_allowed = True

    return {
        "state": state,
        "blocker_family": family,
        "blockers": blockers,
        "warnings": warnings,
        "submit_allowed": state == READY_SUBMIT_CAPABLE,
        "runtime_start_allowed": runtime_start_allowed,
        "restart_allowed_if_runtime_down": restart_allowed,
        "market_schedule_state": canonical.get("market_schedule_state"),
        "stale_market_data_expected": canonical.get("stale_market_data_expected") is True,
        "next_expected_reopen_time": canonical.get("next_expected_reopen_time"),
        "market_data_grace_until": canonical.get("market_data_grace_until"),
        "readiness_block_is_scheduled_halt": canonical.get("readiness_block_is_scheduled_halt") is True,
        "runtime_summary": {
            "running": runtime_running,
            "pid": runtime_truth.get("producer_pid") or runtime_truth.get("pid"),
            "lane_count": runtime_lane_count or lane_count,
            "heartbeat_state": runtime_truth.get("heartbeat_state"),
            "freshness_state": runtime_truth.get("freshness_state"),
            "writer_authority": runtime_truth.get("writer_authority"),
        },
        "config_summary": {
            "lane_count": lane_count,
            "config_hash": config.get("config_hash") or config.get("digest"),
            "paper_only": True,
            "live_money_eligible": False,
        },
        "recovery_summary": {
            "hourly_recovery_classification": recovery.get("classification"),
            "supervisor_classification": supervisor.get("classification"),
            "safe_to_start_runtime": supervisor.get("safe_to_start_runtime"),
        },
    }


def write_runtime_operability_contract(
    *,
    config: RuntimeOperabilityConfig,
    payload: Mapping[str, Any],
) -> Path:
    output_path = config.resolve(config.output_path)
    write_json_atomic(output_path, to_jsonable(dict(payload)))
    _append_jsonl(config.resolve(config.events_path), payload)
    return output_path


def _deprecated_surface_quarantine(surfaces: Mapping[str, Mapping[str, Any]]) -> dict[str, Any]:
    quarantined = {
        key: row
        for key, row in surfaces.items()
        if row.get("classification") in {STALE_DEPRECATED, GENERATED_EVIDENCE_ONLY, GENERATED_PROJECTION_ONLY}
    }
    return {
        "quarantined_count": len(quarantined),
        "quarantined_surfaces": quarantined,
        "cannot_imply_submit_authority": True,
        "cannot_imply_runtime_health": True,
        "cannot_override_active_paper_config": True,
        "cannot_influence_recovery": True,
    }


def _highest_precedence_family(blockers: Sequence[Mapping[str, Any]]) -> str:
    precedence = [BLOCKED_SAFETY, BLOCKED_STALE_TRUTH, BLOCKED_CONFIG, BLOCKED_INFRASTRUCTURE]
    families = {str(row.get("family")) for row in blockers}
    for family in precedence:
        if family in families:
            return family
    return BLOCKED_INFRASTRUCTURE


def _runtime_truth_implies_running(runtime_truth: Mapping[str, Any]) -> bool | None:
    pid = runtime_truth.get("producer_pid") or runtime_truth.get("pid")
    heartbeat = str(runtime_truth.get("heartbeat_state") or "").upper()
    freshness = str(runtime_truth.get("freshness_state") or "").upper()
    if pid and heartbeat == "HEALTHY" and freshness == "FRESH":
        return True
    if heartbeat in {"STOPPED", "DEAD", "MISSING"}:
        return False
    return None


def _freshness(path: Path, *, now: datetime, stale_after_seconds: float | None) -> dict[str, Any]:
    if not path.exists():
        return {"available": False, "stale": stale_after_seconds is not None, "age_seconds": None}
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except OSError:
        return {"available": False, "stale": stale_after_seconds is not None, "age_seconds": None}
    age = max(0.0, (now - mtime).total_seconds())
    return {
        "available": True,
        "mtime": mtime.isoformat(),
        "age_seconds": age,
        "stale_after_seconds": stale_after_seconds,
        "stale": False if stale_after_seconds is None else age > stale_after_seconds,
    }


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _append_jsonl(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(to_jsonable(dict(payload)), sort_keys=True) + "\n")


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y"}
    return bool(value)


def _int(value: Any, *, default: int | None = 0) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Write/read Track B PAPER runtime operability contract.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = RuntimeOperabilityConfig(
        repo_root=args.repo_root.expanduser().resolve(),
        output_path=args.output_path,
    )
    payload = build_runtime_operability_contract(config=config)
    write_runtime_operability_contract(config=config, payload=payload)
    if args.json:
        print(json.dumps(to_jsonable(payload), indent=2, sort_keys=True))
    else:
        print(
            f"{payload['canonical_state']}: "
            f"restart_allowed_if_runtime_down={payload['restart_allowed_if_runtime_down']}"
        )
    return 0 if payload["canonical_state"] in {READY_SUBMIT_CAPABLE, READY_DIAGNOSTIC_ONLY} else 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
