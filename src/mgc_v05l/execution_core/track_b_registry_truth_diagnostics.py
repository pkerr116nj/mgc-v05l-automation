"""Read-only Track B registry/truth diagnostics.

This module surfaces canonical truth, trade-registry reconstruction, shadow
registry state, and the latest lifecycle stress preflight result as diagnostics
only. It is not submit authority and is not wired into runtime gates.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    TRUTH_CONFLICT_REVIEW_REQUIRED,
    TrackBTruthSnapshot,
    TrackBTruthSnapshotConfig,
    TrackBTruthSource,
    build_track_b_truth_snapshot,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeCurrentState
from mgc_v05l.execution_core.track_b_lifecycle_stress_preflight import DEFAULT_PREFLIGHT_SUMMARY_PATH
from mgc_v05l.execution_core.track_b_trade_registry_reconstruction import (
    TradeRegistryReconstructionConfig,
    TradeRegistryReconstructionReport,
    reconstruct_trade_registry_from_artifacts,
)
from mgc_v05l.execution_core.track_b_trade_registry_shadow_report import (
    TradeRegistryShadowReport,
    TradeRegistryShadowReportConfig,
    build_trade_registry_shadow_report,
)


SCHEMA_VERSION = "track_b_registry_truth_diagnostics_v1"
DEFAULT_DIAGNOSTICS_REPORT_PATH = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "latest_track_b_registry_truth_diagnostics.json"
)

TRACK_B_DIAGNOSTICS_CLEAN = "TRACK_B_DIAGNOSTICS_CLEAN"
TRACK_B_DIAGNOSTICS_REVIEW_REQUIRED = "TRACK_B_DIAGNOSTICS_REVIEW_REQUIRED"
TRACK_B_DIAGNOSTICS_STALE = "TRACK_B_DIAGNOSTICS_STALE"
TRACK_B_DIAGNOSTICS_CONFLICT = "TRACK_B_DIAGNOSTICS_CONFLICT"


@dataclass(frozen=True)
class TrackBRegistryTruthDiagnosticsConfig:
    repo_root: Path
    output_path: Path = DEFAULT_DIAGNOSTICS_REPORT_PATH
    truth_config: TrackBTruthSnapshotConfig | None = None
    reconstruction_config: TradeRegistryReconstructionConfig | None = None
    shadow_config: TradeRegistryShadowReportConfig | None = None
    preflight_summary_path: Path = DEFAULT_PREFLIGHT_SUMMARY_PATH
    max_preflight_age_seconds: float = 86_400.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path

    def effective_truth_config(self) -> TrackBTruthSnapshotConfig:
        return self.truth_config or TrackBTruthSnapshotConfig(repo_root=self.repo_root)

    def effective_reconstruction_config(self) -> TradeRegistryReconstructionConfig:
        return self.reconstruction_config or TradeRegistryReconstructionConfig(repo_root=self.repo_root)

    def effective_shadow_config(self) -> TradeRegistryShadowReportConfig:
        if self.shadow_config is not None:
            return self.shadow_config
        return TradeRegistryShadowReportConfig(
            repo_root=self.repo_root,
            truth_config=self.effective_truth_config(),
            reconstruction_config=self.effective_reconstruction_config(),
        )


@dataclass(frozen=True)
class TrackBRegistryTruthDiagnosticsReport:
    schema_version: str
    generated_at: datetime
    classification: str
    read_only: bool
    diagnostic_only: bool
    broker_mutation_allowed: bool
    runtime_restart_allowed: bool
    production_gate_wiring_allowed: bool
    runtime_ready: bool
    runtime_alive: bool
    recovery_active: bool
    broker_lifecycle_reconciled: bool
    broker_open_order_count: int
    broker_position_count: int
    lifecycle_open_position_count: int
    registry_trade_state_counts: Mapping[str, int]
    review_required_trade_ids: tuple[str, ...]
    truth_conflicts: tuple[Mapping[str, Any], ...]
    registry_reconciliation_disagreements: tuple[str, ...]
    latest_preflight_hard_failure_count: int
    latest_preflight_stale: bool
    latest_preflight_source_path: str
    reason_codes: tuple[str, ...]
    truth_snapshot: Mapping[str, Any]
    reconstruction_summary: Mapping[str, Any]
    shadow_summary: Mapping[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "schema_version": self.schema_version,
            "generated_at": self.generated_at.isoformat(),
            "classification": self.classification,
            "read_only": self.read_only,
            "diagnostic_only": self.diagnostic_only,
            "broker_mutation_allowed": self.broker_mutation_allowed,
            "runtime_restart_allowed": self.runtime_restart_allowed,
            "production_gate_wiring_allowed": self.production_gate_wiring_allowed,
            "runtime_ready": self.runtime_ready,
            "runtime_alive": self.runtime_alive,
            "recovery_active": self.recovery_active,
            "broker_lifecycle_reconciled": self.broker_lifecycle_reconciled,
            "broker_open_order_count": self.broker_open_order_count,
            "broker_position_count": self.broker_position_count,
            "lifecycle_open_position_count": self.lifecycle_open_position_count,
            "registry_trade_state_counts": dict(self.registry_trade_state_counts),
            "review_required_trade_ids": list(self.review_required_trade_ids),
            "truth_conflicts": list(self.truth_conflicts),
            "registry_reconciliation_disagreements": list(self.registry_reconciliation_disagreements),
            "latest_preflight_hard_failure_count": self.latest_preflight_hard_failure_count,
            "latest_preflight_stale": self.latest_preflight_stale,
            "latest_preflight_source_path": self.latest_preflight_source_path,
            "reason_codes": list(self.reason_codes),
            "truth_snapshot": dict(self.truth_snapshot),
            "reconstruction_summary": dict(self.reconstruction_summary),
            "shadow_summary": dict(self.shadow_summary),
        }


def build_track_b_registry_truth_diagnostics(
    *,
    config: TrackBRegistryTruthDiagnosticsConfig,
    now: datetime | None = None,
) -> TrackBRegistryTruthDiagnosticsReport:
    generated_at = _ensure_utc(now or datetime.now(UTC))
    truth_config = config.effective_truth_config()
    reconstruction_config = config.effective_reconstruction_config()
    shadow_config = config.effective_shadow_config()
    truth = build_track_b_truth_snapshot(config=truth_config, now=generated_at)
    reconstruction = reconstruct_trade_registry_from_artifacts(config=reconstruction_config, now=generated_at)
    shadow = build_trade_registry_shadow_report(config=shadow_config, now=generated_at)
    preflight = _preflight_state(
        path=config.resolve(config.preflight_summary_path),
        now=generated_at,
        max_age_seconds=config.max_preflight_age_seconds,
    )
    reason_codes = _reason_codes(truth=truth, shadow=shadow, preflight=preflight)
    classification = _classification(truth=truth, shadow=shadow, preflight=preflight)
    return TrackBRegistryTruthDiagnosticsReport(
        schema_version=SCHEMA_VERSION,
        generated_at=generated_at,
        classification=classification,
        read_only=True,
        diagnostic_only=True,
        broker_mutation_allowed=False,
        runtime_restart_allowed=False,
        production_gate_wiring_allowed=False,
        runtime_ready=truth.runtime.submit_capable,
        runtime_alive=truth.runtime.runtime_alive,
        recovery_active=truth.recovery.active,
        broker_lifecycle_reconciled=truth.reconciliation.reconciled,
        broker_open_order_count=truth.broker_truth.open_order_count,
        broker_position_count=truth.broker_truth.broker_position_count,
        lifecycle_open_position_count=truth.lifecycle.open_position_count,
        registry_trade_state_counts=_registry_state_counts(reconstruction),
        review_required_trade_ids=tuple(
            record.trade_id
            for record in reconstruction.records
            if record.current_state == TradeCurrentState.REVIEW_REQUIRED
        ),
        truth_conflicts=tuple(_conflict_summary(conflict) for conflict in truth.conflicts),
        registry_reconciliation_disagreements=tuple(
            row.trade_id for row in shadow.rows if not row.registry_agrees_with_reconciliation
        ),
        latest_preflight_hard_failure_count=int(preflight["hard_failure_count"]),
        latest_preflight_stale=bool(preflight["stale"]),
        latest_preflight_source_path=str(preflight["source_path"]),
        reason_codes=reason_codes,
        truth_snapshot=_truth_surface(truth),
        reconstruction_summary=_reconstruction_summary(reconstruction),
        shadow_summary=shadow.to_dict()["summary"],
    )


def write_track_b_registry_truth_diagnostics(
    *,
    config: TrackBRegistryTruthDiagnosticsConfig,
    report: TrackBRegistryTruthDiagnosticsReport,
) -> Path:
    path = config.resolve(config.output_path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(report.to_dict(), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def _classification(
    *,
    truth: TrackBTruthSnapshot,
    shadow: TradeRegistryShadowReport,
    preflight: Mapping[str, Any],
) -> str:
    if _truth_has_stale_authority(truth) or preflight["stale"]:
        return TRACK_B_DIAGNOSTICS_STALE
    if truth.classification == TRUTH_CONFLICT_REVIEW_REQUIRED or any(
        not row.registry_agrees_with_reconciliation
        and row.current_derived_state != TradeCurrentState.REVIEW_REQUIRED.value
        for row in shadow.rows
    ):
        return TRACK_B_DIAGNOSTICS_CONFLICT
    if any(row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value for row in shadow.rows):
        return TRACK_B_DIAGNOSTICS_REVIEW_REQUIRED
    if int(preflight["hard_failure_count"]) > 0:
        return TRACK_B_DIAGNOSTICS_REVIEW_REQUIRED
    return TRACK_B_DIAGNOSTICS_CLEAN


def _reason_codes(
    *,
    truth: TrackBTruthSnapshot,
    shadow: TradeRegistryShadowReport,
    preflight: Mapping[str, Any],
) -> tuple[str, ...]:
    reasons: list[str] = list(truth.reason_codes)
    if _truth_has_stale_authority(truth):
        reasons.append("TRUTH_AUTHORITY_STALE")
    if preflight["stale"]:
        reasons.append("LIFECYCLE_STRESS_PREFLIGHT_STALE")
    if int(preflight["hard_failure_count"]) > 0:
        reasons.append("LIFECYCLE_STRESS_PREFLIGHT_HARD_FAILURE")
    if any(row.current_derived_state == TradeCurrentState.REVIEW_REQUIRED.value for row in shadow.rows):
        reasons.append("REGISTRY_REVIEW_REQUIRED_TRADE")
    if any(not row.registry_agrees_with_reconciliation for row in shadow.rows):
        reasons.append("REGISTRY_RECONCILIATION_DISAGREEMENT")
    return tuple(dict.fromkeys(reasons))


def _truth_has_stale_authority(truth: TrackBTruthSnapshot) -> bool:
    sources: tuple[TrackBTruthSource, ...] = (
        truth.runtime.source,
        truth.broker_truth.source,
        truth.lifecycle.source,
        truth.reconciliation.source,
        truth.safe_state.source,
        truth.control_plane.source,
    )
    return any(not source.fresh for source in sources)


def _preflight_state(*, path: Path, now: datetime, max_age_seconds: float) -> Mapping[str, Any]:
    payload = _read_json(path)
    generated_at = _parse_datetime(payload.get("generated_at"))
    age = (now - generated_at).total_seconds() if generated_at is not None else float("inf")
    summary = payload.get("summary") if isinstance(payload.get("summary"), Mapping) else {}
    hard_failure_count = int(summary.get("hard_failure_stage_count") or 0)
    hard_failure_count += int(summary.get("unexpected_invariant_failures") or 0)
    hard_failure_count += int(summary.get("reducer_crashes") or 0)
    hard_failure_count += int(summary.get("trade_id_collisions") or 0)
    hard_failure_count += int(summary.get("silent_ambiguity_merges") or 0)
    hard_failure_count += int(summary.get("impossible_states") or 0)
    hard_failure_count += int(summary.get("bad_lifecycles_without_reason_codes") or 0)
    hard_failure_count += int(summary.get("broker_backed_evidence_violations") or 0)
    stale = not payload or generated_at is None or age > max_age_seconds
    return {
        "source_path": str(path),
        "generated_at": generated_at,
        "age_seconds": age,
        "stale": stale,
        "hard_failure_count": hard_failure_count,
    }


def _registry_state_counts(reconstruction: TradeRegistryReconstructionReport) -> Mapping[str, int]:
    counts: dict[str, int] = {}
    for record in reconstruction.records:
        counts[record.current_state.value] = counts.get(record.current_state.value, 0) + 1
    return counts


def _conflict_summary(conflict: Any) -> Mapping[str, Any]:
    return {
        "classification": conflict.classification,
        "question": conflict.question,
        "reason_codes": list(conflict.reason_codes),
        "review_required": conflict.review_required,
    }


def _truth_surface(truth: TrackBTruthSnapshot) -> Mapping[str, Any]:
    return {
        "snapshot_id": truth.snapshot_id,
        "classification": truth.classification,
        "runtime_ready": truth.runtime.submit_capable,
        "runtime_alive": truth.runtime.runtime_alive,
        "recovery_active": truth.recovery.active,
        "broker_truth_fresh": truth.broker_truth.fresh,
        "broker_open_order_count": truth.broker_truth.open_order_count,
        "broker_position_count": truth.broker_truth.broker_position_count,
        "lifecycle_fresh": truth.lifecycle.fresh,
        "lifecycle_open_position_count": truth.lifecycle.open_position_count,
        "reconciliation_fresh": truth.reconciliation.fresh,
        "reconciled": truth.reconciliation.reconciled,
        "safe_state_submit_allowed": truth.safe_state.submit_allowed,
        "control_plane_fresh": truth.control_plane.fresh,
    }


def _reconstruction_summary(reconstruction: TradeRegistryReconstructionReport) -> Mapping[str, Any]:
    payload = reconstruction.to_dict()
    return {
        "record_count": payload["summary"]["record_count"],
        "review_required_count": payload["summary"]["review_required_count"],
        "closed_flat_count": payload["summary"]["closed_flat_count"],
        "cancelled_count": payload["summary"]["cancelled_count"],
        "missing_link_count": len(reconstruction.missing_links),
        "ambiguity_reason_codes": list(reconstruction.ambiguity_reason_codes),
    }


def _read_json(path: Path) -> Mapping[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError:
        return {}
    return payload if isinstance(payload, Mapping) else {}


def _parse_datetime(value: Any) -> datetime | None:
    if not value:
        return None
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None or value.utcoffset() is None:
        return value.replace(tzinfo=UTC)
    return value.astimezone(UTC)
