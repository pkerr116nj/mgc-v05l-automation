"""Read-only canonical truth snapshot for Track B PAPER.

This module aggregates already-written authority artifacts into one typed
snapshot. It never connects to IBKR, never starts/stops runtime processes, and
never mutates broker, lifecycle, order, or strategy state.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import asdict, dataclass, field
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_atomic_io import write_json_atomic
from mgc_v05l.execution_core.track_b_control_plane_snapshot import DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
from mgc_v05l.execution_core.track_b_hourly_runtime_recovery_audit import DEFAULT_LATEST_OUTPUT_PATH
from mgc_v05l.execution_core.track_b_paper_autonomous_recovery_planner import (
    DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_paper_broker_reconciliation import ReconciliationConfig
from mgc_v05l.execution_core.track_b_runtime_environment_truth import (
    DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_runtime_safe_state_envelope import (
    DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_runtime_supervisor_authority import (
    DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "canonical_truth"
    / "latest_track_b_canonical_truth_snapshot.json"
)
DEFAULT_RECOVERY_LAUNCHD_STATUS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "runtime_recovery"
    / "latest_launchd_recovery_status.json"
)
DEFAULT_CONTRACT_STATUS_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "contract_resolver"
    / "latest_contract_resolver_status.json"
)
DEFAULT_FILL_EVIDENCE_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "paper_trade_ledger"
    / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_LOCAL_PAPER_ARTIFACT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "strategy_bridge"
    / "latest_strategy_bridge_submit_report.json"
)

TRUTH_SNAPSHOT_OK = "TRUTH_SNAPSHOT_OK"
TRUTH_CONFLICT_REVIEW_REQUIRED = "TRUTH_CONFLICT_REVIEW_REQUIRED"
RUNTIME_TRUTH_CONFLICT_REVIEW_REQUIRED = "RUNTIME_TRUTH_CONFLICT_REVIEW_REQUIRED"
SUBMIT_AUTHORITY_CONFLICT_REVIEW_REQUIRED = "SUBMIT_AUTHORITY_CONFLICT_REVIEW_REQUIRED"
RECOVERY_ACTIVE = "RECOVERY_ACTIVE"
RECOVERY_PAUSED = "RECOVERY_PAUSED"
RECOVERY_STATUS_STALE_REVIEW_REQUIRED = "RECOVERY_STATUS_STALE_REVIEW_REQUIRED"
BROKER_TRUTH_STALE = "BROKER_TRUTH_STALE"
BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED = "BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED"
LIFECYCLE_TRUTH_STALE = "LIFECYCLE_TRUTH_STALE"
BROKER_LIFECYCLE_RECONCILIATION_DIRTY = "BROKER_LIFECYCLE_RECONCILIATION_DIRTY"
SAFE_STATE_SUBMIT_BLOCKED = "SAFE_STATE_SUBMIT_BLOCKED"
SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT = "SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT"
CONTROL_PLANE_STALE = "CONTROL_PLANE_STALE"
CONTROL_PLANE_INCOHERENT = "CONTROL_PLANE_INCOHERENT"
PLANNER_SNAPSHOT_MISMATCH = "PLANNER_SNAPSHOT_MISMATCH"
FILL_NOT_BROKER_BACKED = "FILL_NOT_BROKER_BACKED"
LOCAL_ARTIFACT_NOT_AUTHORITY = "LOCAL_ARTIFACT_NOT_AUTHORITY"
CONTRACT_ENTRY_ELIGIBLE = "CONTRACT_ENTRY_ELIGIBLE"
CONTRACT_ENTRY_CLOSE_ONLY = "CONTRACT_ENTRY_CLOSE_ONLY"
CONTRACT_ENTRY_BLOCKED = "CONTRACT_ENTRY_BLOCKED"
CONTRACT_DETAILS_STALE = "CONTRACT_DETAILS_STALE"
CONTRACT_AMBIGUOUS = "CONTRACT_AMBIGUOUS"
EXACT_LIFECYCLE_OWNER_RESOLVED = "EXACT_LIFECYCLE_OWNER_RESOLVED"
AGGREGATE_PLACEHOLDER_DIAGNOSTIC_ONLY = "AGGREGATE_PLACEHOLDER_DIAGNOSTIC_ONLY"
EXACT_LIFECYCLE_IDENTITY_MISMATCH = "EXACT_LIFECYCLE_IDENTITY_MISMATCH"
DUPLICATE_WRITER_DETECTED = "DUPLICATE_WRITER_DETECTED"


class SourceAuthorityLevel(str, Enum):
    AUTHORITATIVE = "AUTHORITATIVE"
    SECONDARY = "SECONDARY"
    DIAGNOSTIC = "DIAGNOSTIC"


class SourceClassification(str, Enum):
    FRESH = "FRESH"
    STALE = "STALE"
    MISSING = "MISSING"
    INVALID = "INVALID"


@dataclass(frozen=True)
class TrackBTruthSource:
    source_name: str
    artifact_path: str
    generated_at: datetime | None
    freshness_seconds: float
    max_age_seconds: float
    fresh: bool
    classification: str
    authority_level: str
    diagnostic_only: bool = False


@dataclass(frozen=True)
class RuntimeTruthSection:
    runtime_alive: bool
    submit_capable: bool
    runtime_generation_id: str | None
    pid: int | None
    lane_count: int | None
    duplicate_writer_detected: bool
    source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class RecoveryTruthSection:
    active: bool
    paused: bool
    classification: str
    launchd_loaded: bool
    last_tick: str | None
    source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class BrokerTruthSection:
    fresh: bool
    open_order_count: int
    broker_position_count: int
    positions: tuple[Mapping[str, Any], ...]
    open_orders: tuple[Mapping[str, Any], ...]
    source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class LifecycleTruthSection:
    fresh: bool
    open_position_count: int
    managed_order_count: int
    open_positions: tuple[Mapping[str, Any], ...]
    exact_owner_resolved: bool
    aggregate_placeholder_accounts: tuple[str, ...]
    source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ReconciliationTruthSection:
    fresh: bool
    reconciled: bool
    classification: str
    review_required_count: int
    source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class SafeStateTruthSection:
    fresh: bool
    classification: str
    submit_allowed: bool
    runtime_start_allowed: bool
    source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ControlPlaneTruthSection:
    fresh: bool
    coherent: bool
    snapshot_id: str | None
    shared_truth_generation_id: str | None
    classification: str
    source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class PlannerSupervisorTruthSection:
    planner_classification: str | None
    supervisor_classification: str | None
    planner_snapshot_id: str | None
    supervisor_decision_id: str | None
    snapshot_match: bool
    planner_source: TrackBTruthSource
    supervisor_source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class ContractTruthSection:
    symbol: str | None
    selected_local_symbol: str | None
    resolved_local_symbol: str | None
    con_id: int | None
    expiry: str | None
    entry_status: str
    exit_status: str
    source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class BrokerBackedEvidenceSection:
    broker_backed: bool
    order_id: str | None
    client_id: str | None
    perm_id: str | None
    exec_id: str | None
    source: TrackBTruthSource
    reason_codes: tuple[str, ...] = ()


@dataclass(frozen=True)
class TruthConflict:
    classification: str
    question: str
    authoritative_source: str | None
    conflicting_sources: tuple[str, ...]
    reason_codes: tuple[str, ...]
    review_required: bool = True


@dataclass(frozen=True)
class TrackBTruthSnapshot:
    schema_version: str
    generated_at: datetime
    snapshot_id: str
    paper_only: bool
    live_money_eligible: bool
    paper_proof_invoked: bool
    runtime: RuntimeTruthSection
    recovery: RecoveryTruthSection
    broker_truth: BrokerTruthSection
    lifecycle: LifecycleTruthSection
    reconciliation: ReconciliationTruthSection
    safe_state: SafeStateTruthSection
    control_plane: ControlPlaneTruthSection
    planner_supervisor: PlannerSupervisorTruthSection
    contract_status: ContractTruthSection
    broker_backed_evidence: BrokerBackedEvidenceSection
    conflicts: tuple[TruthConflict, ...]
    reason_codes: tuple[str, ...]
    source_paths: Mapping[str, str]
    diagnostic_sources: tuple[TrackBTruthSource, ...] = ()

    @property
    def classification(self) -> str:
        return TRUTH_CONFLICT_REVIEW_REQUIRED if self.conflicts else TRUTH_SNAPSHOT_OK

    def to_dict(self) -> dict[str, Any]:
        payload = _jsonable(asdict(self))
        payload["classification"] = self.classification
        return payload


@dataclass(frozen=True)
class TrackBTruthSnapshotConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    runtime_truth_path: Path = DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT
    recovery_status_path: Path = DEFAULT_RECOVERY_LAUNCHD_STATUS_PATH
    recovery_audit_path: Path = DEFAULT_LATEST_OUTPUT_PATH
    broker_status_path: Path = Path("outputs/reports/ibkr_read_only_verification/ibkr_broker_truth_refresh_status.json")
    broker_positions_path: Path = Path("outputs/reports/ibkr_read_only_verification/ibkr_positions_snapshot.json")
    broker_open_orders_path: Path = Path("outputs/reports/ibkr_read_only_verification/ibkr_open_orders_snapshot.json")
    lifecycle_live_position_path: Path = Path(
        "outputs/track_b_execution_core/paper_trade_ledger/latest_track_b_live_position_status.json"
    )
    managed_position_registry_path: Path = Path(
        "outputs/track_b_execution_core/managed_positions/latest_managed_positions.json"
    )
    managed_order_registry_path: Path = Path(
        "outputs/track_b_execution_core/managed_orders/latest_managed_orders.json"
    )
    reconciliation_path: Path = ReconciliationConfig().report_path
    safe_state_path: Path = DEFAULT_RUNTIME_SAFE_STATE_ENVELOPE_ARTIFACT
    control_plane_path: Path = DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT
    planner_path: Path = DEFAULT_PAPER_AUTONOMOUS_RECOVERY_PLAN_ARTIFACT
    supervisor_path: Path = DEFAULT_RUNTIME_SUPERVISOR_AUTHORITY_ARTIFACT
    contract_status_path: Path = DEFAULT_CONTRACT_STATUS_PATH
    broker_backed_evidence_path: Path = DEFAULT_FILL_EVIDENCE_PATH
    local_paper_artifact_path: Path = DEFAULT_LOCAL_PAPER_ARTIFACT_PATH
    dashboard_runtime_path: Path | None = Path(
        "outputs/operator_dashboard/runtime/latest_track_b_runtime_environment_truth.json"
    )
    max_runtime_age_seconds: float = 120.0
    max_recovery_age_seconds: float = 3600.0
    max_broker_truth_age_seconds: float = 120.0
    max_lifecycle_age_seconds: float = 300.0
    max_reconciliation_age_seconds: float = 300.0
    max_safe_state_age_seconds: float = 300.0
    max_control_plane_age_seconds: float = 300.0
    max_planner_age_seconds: float = 300.0
    max_supervisor_age_seconds: float = 300.0
    max_contract_age_seconds: float = 86400.0
    max_fill_evidence_age_seconds: float = 86400.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def build_track_b_truth_snapshot(
    *,
    config: TrackBTruthSnapshotConfig,
    now: datetime | None = None,
) -> TrackBTruthSnapshot:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    runtime_payload, runtime_source = _read_source(
        config=config,
        name="runtime_truth",
        path=config.runtime_truth_path,
        max_age_seconds=config.max_runtime_age_seconds,
        now=actual_now,
    )
    recovery_payload, recovery_source = _read_source(
        config=config,
        name="recovery_status",
        path=config.recovery_status_path,
        max_age_seconds=config.max_recovery_age_seconds,
        now=actual_now,
    )
    recovery_audit_payload, recovery_audit_source = _read_source(
        config=config,
        name="recovery_audit",
        path=config.recovery_audit_path,
        max_age_seconds=config.max_recovery_age_seconds,
        now=actual_now,
        authority_level=SourceAuthorityLevel.SECONDARY,
    )
    broker_status_payload, broker_source = _read_source(
        config=config,
        name="broker_truth",
        path=config.broker_status_path,
        max_age_seconds=config.max_broker_truth_age_seconds,
        now=actual_now,
    )
    positions_path = _path_from_payload(
        broker_status_payload.get("positions_snapshot_path"),
        default=config.resolve(config.broker_positions_path),
    )
    open_orders_path = _path_from_payload(
        broker_status_payload.get("open_orders_snapshot_path"),
        default=config.resolve(config.broker_open_orders_path),
    )
    positions_payload, positions_source = _read_absolute_source(
        name="broker_positions",
        path=positions_path,
        max_age_seconds=config.max_broker_truth_age_seconds,
        now=actual_now,
    )
    open_orders_payload, open_orders_source = _read_absolute_source(
        name="broker_open_orders",
        path=open_orders_path,
        max_age_seconds=config.max_broker_truth_age_seconds,
        now=actual_now,
    )
    lifecycle_payload, lifecycle_source = _read_source(
        config=config,
        name="lifecycle_positions",
        path=config.lifecycle_live_position_path,
        max_age_seconds=config.max_lifecycle_age_seconds,
        now=actual_now,
    )
    managed_positions_payload, managed_positions_source = _read_source(
        config=config,
        name="managed_position_registry",
        path=config.managed_position_registry_path,
        max_age_seconds=config.max_lifecycle_age_seconds,
        now=actual_now,
        authority_level=SourceAuthorityLevel.SECONDARY,
    )
    managed_orders_payload, managed_orders_source = _read_source(
        config=config,
        name="managed_order_registry",
        path=config.managed_order_registry_path,
        max_age_seconds=config.max_lifecycle_age_seconds,
        now=actual_now,
        authority_level=SourceAuthorityLevel.SECONDARY,
    )
    reconciliation_payload, reconciliation_source = _read_source(
        config=config,
        name="reconciliation",
        path=config.reconciliation_path,
        max_age_seconds=config.max_reconciliation_age_seconds,
        now=actual_now,
    )
    safe_state_payload, safe_state_source = _read_source(
        config=config,
        name="safe_state",
        path=config.safe_state_path,
        max_age_seconds=config.max_safe_state_age_seconds,
        now=actual_now,
    )
    control_plane_payload, control_plane_source = _read_source(
        config=config,
        name="control_plane",
        path=config.control_plane_path,
        max_age_seconds=config.max_control_plane_age_seconds,
        now=actual_now,
    )
    planner_payload, planner_source = _read_source(
        config=config,
        name="planner",
        path=config.planner_path,
        max_age_seconds=config.max_planner_age_seconds,
        now=actual_now,
    )
    supervisor_payload, supervisor_source = _read_source(
        config=config,
        name="supervisor",
        path=config.supervisor_path,
        max_age_seconds=config.max_supervisor_age_seconds,
        now=actual_now,
    )
    contract_payload, contract_source = _read_source(
        config=config,
        name="contract_status",
        path=config.contract_status_path,
        max_age_seconds=config.max_contract_age_seconds,
        now=actual_now,
    )
    fill_payload, fill_source = _read_source(
        config=config,
        name="broker_backed_evidence",
        path=config.broker_backed_evidence_path,
        max_age_seconds=config.max_fill_evidence_age_seconds,
        now=actual_now,
    )
    local_paper_payload, local_paper_source = _read_source(
        config=config,
        name="local_paper_artifact",
        path=config.local_paper_artifact_path,
        max_age_seconds=config.max_fill_evidence_age_seconds,
        now=actual_now,
        diagnostic_only=True,
    )
    diagnostic_sources = [local_paper_source]
    if config.dashboard_runtime_path is not None:
        _, dashboard_source = _read_source(
            config=config,
            name="dashboard_runtime_projection",
            path=config.dashboard_runtime_path,
            max_age_seconds=config.max_runtime_age_seconds,
            now=actual_now,
            diagnostic_only=True,
        )
        diagnostic_sources.append(dashboard_source)

    runtime = _runtime_section(runtime_payload, runtime_source)
    recovery = _recovery_section(recovery_payload, recovery_source, recovery_audit_payload, recovery_audit_source)
    broker_truth = _broker_section(
        broker_source=broker_source,
        positions_source=positions_source,
        open_orders_source=open_orders_source,
        positions_payload=positions_payload,
        open_orders_payload=open_orders_payload,
    )
    lifecycle = _lifecycle_section(
        lifecycle_payload=lifecycle_payload,
        lifecycle_source=lifecycle_source,
        managed_positions_payload=managed_positions_payload,
        managed_positions_source=managed_positions_source,
        managed_orders_payload=managed_orders_payload,
    )
    reconciliation = _reconciliation_section(reconciliation_payload, reconciliation_source)
    safe_state = _safe_state_section(safe_state_payload, safe_state_source)
    control_plane = _control_plane_section(control_plane_payload, control_plane_source)
    planner_supervisor = _planner_supervisor_section(
        planner_payload=planner_payload,
        planner_source=planner_source,
        supervisor_payload=supervisor_payload,
        supervisor_source=supervisor_source,
        control_plane=control_plane,
    )
    contract_status = _contract_section(contract_payload, contract_source)
    broker_backed_evidence = _broker_backed_evidence_section(fill_payload, fill_source, local_paper_payload)
    conflicts = _conflicts(
        runtime=runtime,
        recovery=recovery,
        broker_truth=broker_truth,
        lifecycle=lifecycle,
        reconciliation=reconciliation,
        safe_state=safe_state,
        control_plane=control_plane,
        planner_supervisor=planner_supervisor,
        contract_status=contract_status,
        broker_backed_evidence=broker_backed_evidence,
    )
    reason_codes = _reason_codes(
        runtime,
        recovery,
        broker_truth,
        lifecycle,
        reconciliation,
        safe_state,
        control_plane,
        planner_supervisor,
        contract_status,
        broker_backed_evidence,
        conflicts,
    )
    source_paths = {
        "runtime_truth": runtime_source.artifact_path,
        "recovery_status": recovery_source.artifact_path,
        "recovery_audit": recovery_audit_source.artifact_path,
        "broker_truth": broker_source.artifact_path,
        "broker_positions": positions_source.artifact_path,
        "broker_open_orders": open_orders_source.artifact_path,
        "lifecycle_positions": lifecycle_source.artifact_path,
        "managed_position_registry": managed_positions_source.artifact_path,
        "managed_order_registry": managed_orders_source.artifact_path,
        "reconciliation": reconciliation_source.artifact_path,
        "safe_state": safe_state_source.artifact_path,
        "control_plane": control_plane_source.artifact_path,
        "planner": planner_source.artifact_path,
        "supervisor": supervisor_source.artifact_path,
        "contract_status": contract_source.artifact_path,
        "broker_backed_evidence": fill_source.artifact_path,
        "local_paper_artifact": local_paper_source.artifact_path,
    }
    return TrackBTruthSnapshot(
        schema_version="track_b_truth_snapshot_v1",
        generated_at=actual_now,
        snapshot_id=f"truth_snapshot_{actual_now.isoformat()}",
        paper_only=True,
        live_money_eligible=False,
        paper_proof_invoked=False,
        runtime=runtime,
        recovery=recovery,
        broker_truth=broker_truth,
        lifecycle=lifecycle,
        reconciliation=reconciliation,
        safe_state=safe_state,
        control_plane=control_plane,
        planner_supervisor=planner_supervisor,
        contract_status=contract_status,
        broker_backed_evidence=broker_backed_evidence,
        conflicts=tuple(conflicts),
        reason_codes=tuple(reason_codes),
        source_paths=source_paths,
        diagnostic_sources=tuple(diagnostic_sources),
    )


def write_track_b_truth_snapshot(
    *,
    config: TrackBTruthSnapshotConfig,
    snapshot: TrackBTruthSnapshot,
) -> Path:
    output_path = config.resolve(config.output_path)
    write_json_atomic(output_path, snapshot.to_dict())
    return output_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Emit read-only Track B canonical truth snapshot.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--write", action="store_true", help="Write the snapshot artifact as well as printing JSON.")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    config = TrackBTruthSnapshotConfig(
        repo_root=Path(args.repo_root).expanduser().resolve(),
        output_path=Path(args.output_path),
    )
    snapshot = build_track_b_truth_snapshot(config=config)
    if args.write:
        write_track_b_truth_snapshot(config=config, snapshot=snapshot)
    print(json.dumps(snapshot.to_dict(), indent=2, sort_keys=True))
    return 0 if snapshot.classification == TRUTH_SNAPSHOT_OK else 2


def _runtime_section(payload: Mapping[str, Any], source: TrackBTruthSource) -> RuntimeTruthSection:
    runtime = _mapping(payload.get("runtime"))
    readiness = _mapping(payload.get("canonical_readiness"))
    pid = _int_or_none(runtime.get("pid") or payload.get("pid"))
    runtime_alive = bool(
        source.fresh
        and (
            runtime.get("pid_alive") is True
            or payload.get("runtime_alive") is True
            or payload.get("alive") is True
            or payload.get("classification") == "RUNTIME_ACTIVE_TRADE_CAPABLE"
        )
    )
    submit_capable = bool(
        source.fresh
        and (
            readiness.get("ready_submit_capable") is True
            or readiness.get("classification") == "READY_SUBMIT_CAPABLE"
            or payload.get("ready_submit_capable") is True
            or payload.get("runtime_submit_capable") is True
        )
    )
    duplicate_writer = bool(
        payload.get("agent_health_has_duplicate_writer") is True
        or payload.get("duplicate_writer_detected") is True
        or payload.get("classification") == "DUPLICATE_RUNTIME_WRITERS"
    )
    reasons = []
    if duplicate_writer:
        reasons.append(DUPLICATE_WRITER_DETECTED)
    if not source.fresh:
        reasons.append("RUNTIME_TRUTH_STALE_OR_MISSING")
    return RuntimeTruthSection(
        runtime_alive=runtime_alive,
        submit_capable=submit_capable,
        runtime_generation_id=_str_or_none(
            runtime.get("runtime_instance_id")
            or runtime.get("restart_generation")
            or payload.get("runtime_generation_id")
            or payload.get("runtime_instance_id")
        ),
        pid=pid,
        lane_count=_int_or_none(runtime.get("lane_count") or payload.get("lane_count")),
        duplicate_writer_detected=duplicate_writer,
        source=source,
        reason_codes=tuple(reasons),
    )


def _recovery_section(
    payload: Mapping[str, Any],
    source: TrackBTruthSource,
    audit_payload: Mapping[str, Any],
    audit_source: TrackBTruthSource,
) -> RecoveryTruthSection:
    status = payload if payload else _mapping(audit_payload.get("hourly_supervisor"))
    classification = str(status.get("classification") or status.get("state") or audit_payload.get("classification") or "")
    active = bool(source.fresh and (status.get("launchd_enabled") is True or status.get("active") is True or classification == RECOVERY_ACTIVE))
    paused = bool(classification in {"SUPERVISOR_PAUSED", "RECOVERY_DISABLED_BY_OPERATOR"} or status.get("paused") is True)
    reasons = []
    if not source.fresh and not audit_source.fresh:
        reasons.append(RECOVERY_STATUS_STALE_REVIEW_REQUIRED)
    if active:
        reasons.append(RECOVERY_ACTIVE)
    if paused:
        reasons.append(RECOVERY_PAUSED)
    return RecoveryTruthSection(
        active=active,
        paused=paused,
        classification=classification or ("RECOVERY_ACTIVE" if active else "RECOVERY_UNKNOWN"),
        launchd_loaded=bool(status.get("launchd_loaded") is True or status.get("running") is True),
        last_tick=_str_or_none(status.get("last_tick") or status.get("latest_audit_run_at")),
        source=source if payload else audit_source,
        reason_codes=tuple(reasons),
    )


def _broker_section(
    *,
    broker_source: TrackBTruthSource,
    positions_source: TrackBTruthSource,
    open_orders_source: TrackBTruthSource,
    positions_payload: Mapping[str, Any],
    open_orders_payload: Mapping[str, Any],
) -> BrokerTruthSection:
    positions = tuple(row for row in _rows(positions_payload, ("positions", "broker_positions", "rows")) if _qty(row) != 0)
    open_orders = tuple(row for row in _rows(open_orders_payload, ("open_orders", "orders", "rows")) if _open_order_active(row))
    fresh = broker_source.fresh and positions_source.fresh and open_orders_source.fresh
    reasons = ()
    if not fresh:
        reasons = (BROKER_TRUTH_STALE,)
    return BrokerTruthSection(
        fresh=fresh,
        open_order_count=len(open_orders),
        broker_position_count=len(positions),
        positions=positions,
        open_orders=open_orders,
        source=broker_source,
        reason_codes=reasons,
    )


def _lifecycle_section(
    *,
    lifecycle_payload: Mapping[str, Any],
    lifecycle_source: TrackBTruthSource,
    managed_positions_payload: Mapping[str, Any],
    managed_positions_source: TrackBTruthSource,
    managed_orders_payload: Mapping[str, Any],
) -> LifecycleTruthSection:
    lifecycle_rows = _rows(lifecycle_payload, ("open_positions", "positions", "live_positions", "rows"))
    managed_rows = _rows(managed_positions_payload, ("open_positions", "managed_positions", "positions", "rows"))
    open_positions = tuple(row for row in [*lifecycle_rows, *managed_rows] if _lifecycle_open(row))
    managed_orders = tuple(row for row in _rows(managed_orders_payload, ("managed_orders", "orders", "rows")) if _open_order_active(row))
    aggregate_accounts = tuple(
        sorted(
            {
                str(row.get("account_id") or row.get("account") or "")
                for row in open_positions
                if str(row.get("account_id") or row.get("account") or "") in {"MULTIPLE", "MISSING", "UNKNOWN"}
            }
        )
    )
    exact_owner_resolved = any(
        row.get("lifecycle_id")
        and str(row.get("exact_lifecycle_account_id") or row.get("resolved_account_id") or row.get("broker_account_id") or "")
        not in {"", "MULTIPLE", "MISSING", "UNKNOWN"}
        for row in open_positions
    )
    reasons = []
    if not lifecycle_source.fresh and not managed_positions_source.fresh:
        reasons.append(LIFECYCLE_TRUTH_STALE)
    if aggregate_accounts and exact_owner_resolved:
        reasons.extend([EXACT_LIFECYCLE_OWNER_RESOLVED, AGGREGATE_PLACEHOLDER_DIAGNOSTIC_ONLY])
    return LifecycleTruthSection(
        fresh=lifecycle_source.fresh or managed_positions_source.fresh,
        open_position_count=len(open_positions),
        managed_order_count=len(managed_orders),
        open_positions=open_positions,
        exact_owner_resolved=exact_owner_resolved,
        aggregate_placeholder_accounts=aggregate_accounts,
        source=lifecycle_source if lifecycle_source.fresh else managed_positions_source,
        reason_codes=tuple(reasons),
    )


def _reconciliation_section(payload: Mapping[str, Any], source: TrackBTruthSource) -> ReconciliationTruthSection:
    classification = str(payload.get("classification") or payload.get("reconciliation_status") or "")
    review_required_count = _int_or_none(payload.get("review_required_count")) or 0
    reconciled = bool(
        source.fresh
        and (
            payload.get("broker_reconciled") is True
            or payload.get("reconciled") is True
            or classification in {"CLEAN", "RECONCILED", "RECONCILED_FLAT", "BROKER_LIFECYCLE_RECONCILED"}
        )
        and review_required_count == 0
    )
    reasons = []
    if not source.fresh:
        reasons.append("RECONCILIATION_STALE")
    if source.fresh and not reconciled:
        reasons.append(BROKER_LIFECYCLE_RECONCILIATION_DIRTY)
    return ReconciliationTruthSection(
        fresh=source.fresh,
        reconciled=reconciled,
        classification=classification or "UNKNOWN_RECONCILIATION",
        review_required_count=review_required_count,
        source=source,
        reason_codes=tuple(reasons),
    )


def _safe_state_section(payload: Mapping[str, Any], source: TrackBTruthSource) -> SafeStateTruthSection:
    classification = str(payload.get("safe_state_classification") or payload.get("classification") or "")
    submit_allowed = bool(source.fresh and payload.get("submit_allowed") is True)
    runtime_start_allowed = bool(source.fresh and payload.get("runtime_start_allowed") is True)
    reasons = []
    if not source.fresh:
        reasons.append("SAFE_STATE_STALE")
    if source.fresh and not submit_allowed:
        reasons.append(SAFE_STATE_SUBMIT_BLOCKED)
    if runtime_start_allowed and not submit_allowed:
        reasons.append(SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT)
    return SafeStateTruthSection(
        fresh=source.fresh,
        classification=classification or "SAFE_STATE_UNKNOWN",
        submit_allowed=submit_allowed,
        runtime_start_allowed=runtime_start_allowed,
        source=source,
        reason_codes=tuple(reasons),
    )


def _control_plane_section(payload: Mapping[str, Any], source: TrackBTruthSource) -> ControlPlaneTruthSection:
    classification = str(payload.get("classification") or payload.get("shared_truth_coherence_status") or "")
    coherent = bool(source.fresh and (payload.get("shared_truth_coherence_status") == "COHERENT" or classification == "COHERENT"))
    reasons = []
    if not source.fresh:
        reasons.append(CONTROL_PLANE_STALE)
    if source.fresh and not coherent:
        reasons.append(CONTROL_PLANE_INCOHERENT)
    return ControlPlaneTruthSection(
        fresh=source.fresh,
        coherent=coherent,
        snapshot_id=_str_or_none(payload.get("control_plane_snapshot_id")),
        shared_truth_generation_id=_str_or_none(payload.get("shared_truth_refresh_generation_id")),
        classification=classification or "CONTROL_PLANE_UNKNOWN",
        source=source,
        reason_codes=tuple(reasons),
    )


def _planner_supervisor_section(
    *,
    planner_payload: Mapping[str, Any],
    planner_source: TrackBTruthSource,
    supervisor_payload: Mapping[str, Any],
    supervisor_source: TrackBTruthSource,
    control_plane: ControlPlaneTruthSection,
) -> PlannerSupervisorTruthSection:
    planner_snapshot_id = _str_or_none(planner_payload.get("control_plane_snapshot_id"))
    snapshot_match = bool(planner_source.fresh and control_plane.snapshot_id and planner_snapshot_id == control_plane.snapshot_id)
    reasons = []
    if planner_source.fresh and control_plane.snapshot_id and not snapshot_match:
        reasons.append(PLANNER_SNAPSHOT_MISMATCH)
    if not planner_source.fresh:
        reasons.append("PLANNER_STALE")
    if not supervisor_source.fresh:
        reasons.append("SUPERVISOR_STALE")
    return PlannerSupervisorTruthSection(
        planner_classification=_str_or_none(planner_payload.get("classification")),
        supervisor_classification=_str_or_none(supervisor_payload.get("classification") or supervisor_payload.get("supervisor_classification")),
        planner_snapshot_id=planner_snapshot_id,
        supervisor_decision_id=_str_or_none(supervisor_payload.get("supervisor_decision_id")),
        snapshot_match=snapshot_match,
        planner_source=planner_source,
        supervisor_source=supervisor_source,
        reason_codes=tuple(reasons),
    )


def _contract_section(payload: Mapping[str, Any], source: TrackBTruthSource) -> ContractTruthSection:
    classification = str(payload.get("classification") or "")
    selected = _mapping(payload.get("selected_contract") or payload.get("selected_target"))
    resolved = _mapping(payload.get("resolved_contract") or payload.get("recommended_contract") or selected)
    entry_status = str(payload.get("entry_status") or "")
    if not entry_status:
        if not source.fresh:
            entry_status = CONTRACT_DETAILS_STALE
        elif classification in {"CONTRACT_ALLOWED"} and payload.get("submit_allowed") is not False:
            entry_status = CONTRACT_ENTRY_ELIGIBLE
        elif classification in {"CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED"}:
            entry_status = CONTRACT_ENTRY_CLOSE_ONLY
        elif classification in {"CONTRACT_AMBIGUOUS"}:
            entry_status = CONTRACT_AMBIGUOUS
        else:
            entry_status = CONTRACT_ENTRY_BLOCKED
    exit_status = str(payload.get("exit_status") or "")
    if not exit_status:
        exit_status = (
            "EXIT_ORIGINAL_CONTRACT_ALLOWED"
            if classification == "CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED" or entry_status == CONTRACT_ENTRY_CLOSE_ONLY
            else "EXIT_STATUS_UNKNOWN"
        )
    reasons = []
    if entry_status == CONTRACT_ENTRY_CLOSE_ONLY:
        reasons.append(CONTRACT_ENTRY_CLOSE_ONLY)
    elif entry_status == CONTRACT_ENTRY_ELIGIBLE:
        reasons.append(CONTRACT_ENTRY_ELIGIBLE)
    elif entry_status in {CONTRACT_DETAILS_STALE, CONTRACT_AMBIGUOUS, CONTRACT_ENTRY_BLOCKED}:
        reasons.append(entry_status)
    return ContractTruthSection(
        symbol=_str_or_none(payload.get("symbol") or selected.get("symbol")),
        selected_local_symbol=_str_or_none(selected.get("localSymbol") or selected.get("local_symbol")),
        resolved_local_symbol=_str_or_none(resolved.get("localSymbol") or resolved.get("local_symbol")),
        con_id=_int_or_none(selected.get("conId") or selected.get("con_id") or payload.get("conId") or payload.get("con_id")),
        expiry=_str_or_none(selected.get("expiry") or payload.get("expiry")),
        entry_status=entry_status,
        exit_status=exit_status,
        source=source,
        reason_codes=tuple(reasons),
    )


def _broker_backed_evidence_section(
    payload: Mapping[str, Any],
    source: TrackBTruthSource,
    local_paper_payload: Mapping[str, Any],
) -> BrokerBackedEvidenceSection:
    row = _first_evidence_row(payload)
    if not row:
        row = _mapping(payload)
    order_id = _str_or_none(row.get("order_id") or row.get("broker_order_id"))
    client_id = _str_or_none(row.get("client_id"))
    perm_id = _str_or_none(row.get("perm_id"))
    exec_id = _str_or_none(row.get("exec_id"))
    broker_backed = bool(source.fresh and order_id and client_id and perm_id and exec_id)
    reasons = []
    if broker_backed:
        reasons.append("BROKER_BACKED_EVIDENCE_CONFIRMED")
    else:
        local_row = _first_evidence_row(local_paper_payload) or _mapping(local_paper_payload)
        if local_row:
            reasons.extend([FILL_NOT_BROKER_BACKED, LOCAL_ARTIFACT_NOT_AUTHORITY])
        elif payload:
            reasons.append(FILL_NOT_BROKER_BACKED)
    return BrokerBackedEvidenceSection(
        broker_backed=broker_backed,
        order_id=order_id,
        client_id=client_id,
        perm_id=perm_id,
        exec_id=exec_id,
        source=source,
        reason_codes=tuple(reasons),
    )


def _conflicts(
    *,
    runtime: RuntimeTruthSection,
    recovery: RecoveryTruthSection,
    broker_truth: BrokerTruthSection,
    lifecycle: LifecycleTruthSection,
    reconciliation: ReconciliationTruthSection,
    safe_state: SafeStateTruthSection,
    control_plane: ControlPlaneTruthSection,
    planner_supervisor: PlannerSupervisorTruthSection,
    contract_status: ContractTruthSection,
    broker_backed_evidence: BrokerBackedEvidenceSection,
) -> list[TruthConflict]:
    conflicts: list[TruthConflict] = []
    if runtime.duplicate_writer_detected:
        conflicts.append(_conflict(DUPLICATE_WRITER_DETECTED, "Is there a duplicate runtime writer?", runtime.source, (runtime.source,)))
    if runtime.submit_capable and (not broker_truth.fresh or not control_plane.fresh or not safe_state.fresh):
        conflicts.append(
            _conflict(
                SUBMIT_AUTHORITY_CONFLICT_REVIEW_REQUIRED,
                "Can stale artifacts authorize submit?",
                None,
                (broker_truth.source, control_plane.source, safe_state.source),
                (BROKER_TRUTH_STALE if not broker_truth.fresh else "", CONTROL_PLANE_STALE if not control_plane.fresh else ""),
            )
        )
    if not broker_truth.fresh:
        conflicts.append(_conflict(BROKER_TRUTH_STALE, "Is broker truth fresh?", broker_truth.source, (broker_truth.source,)))
    if lifecycle.open_position_count != broker_truth.broker_position_count and not reconciliation.reconciled:
        conflicts.append(
            _conflict(
                BROKER_TRUTH_CONFLICT_REVIEW_REQUIRED,
                "Do broker and lifecycle open exposure agree?",
                broker_truth.source,
                (broker_truth.source, lifecycle.source, reconciliation.source),
                (BROKER_LIFECYCLE_RECONCILIATION_DIRTY,),
            )
        )
    if not reconciliation.reconciled and reconciliation.fresh:
        conflicts.append(
            _conflict(
                BROKER_LIFECYCLE_RECONCILIATION_DIRTY,
                "Are broker/lifecycle reconciled?",
                reconciliation.source,
                (broker_truth.source, lifecycle.source, reconciliation.source),
            )
        )
    if not safe_state.submit_allowed and safe_state.fresh:
        conflicts.append(
            _conflict(
                SAFE_STATE_SUBMIT_BLOCKED,
                "Is Safe-State allowing submit?",
                safe_state.source,
                (safe_state.source,),
                safe_state.reason_codes,
            )
        )
    if SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT in safe_state.reason_codes:
        conflicts.append(
            _conflict(
                SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT,
                "Is runtime-start authority distinct from submit authority?",
                safe_state.source,
                (safe_state.source, runtime.source),
                (SAFE_STATE_RUNTIME_AUTHORITY_CONFLICT,),
            )
        )
    if not control_plane.fresh:
        conflicts.append(_conflict(CONTROL_PLANE_STALE, "Is Control Plane fresh?", control_plane.source, (control_plane.source,)))
    elif not control_plane.coherent:
        conflicts.append(
            _conflict(CONTROL_PLANE_INCOHERENT, "Is Control Plane coherent?", control_plane.source, (control_plane.source,))
        )
    if PLANNER_SNAPSHOT_MISMATCH in planner_supervisor.reason_codes:
        conflicts.append(
            _conflict(
                PLANNER_SNAPSHOT_MISMATCH,
                "Does planner reference the active Control Plane Snapshot?",
                control_plane.source,
                (planner_supervisor.planner_source, control_plane.source),
                (PLANNER_SNAPSHOT_MISMATCH,),
            )
        )
    if FILL_NOT_BROKER_BACKED in broker_backed_evidence.reason_codes:
        conflicts.append(
            _conflict(
                FILL_NOT_BROKER_BACKED,
                "Is fill evidence broker-backed?",
                broker_backed_evidence.source,
                (broker_backed_evidence.source,),
                broker_backed_evidence.reason_codes,
            )
        )
    if contract_status.entry_status in {CONTRACT_ENTRY_BLOCKED, CONTRACT_DETAILS_STALE, CONTRACT_AMBIGUOUS}:
        conflicts.append(
            _conflict(
                contract_status.entry_status,
                "Is contract eligible for new entry?",
                contract_status.source,
                (contract_status.source,),
                contract_status.reason_codes,
            )
        )
    return conflicts


def _reason_codes(*sections: Any) -> list[str]:
    codes: list[str] = []
    for section in sections:
        if isinstance(section, Sequence) and not isinstance(section, (str, bytes, bytearray)):
            for item in section:
                codes.extend(getattr(item, "reason_codes", ()))
                classification = getattr(item, "classification", "")
                if classification:
                    codes.append(classification)
        else:
            codes.extend(getattr(section, "reason_codes", ()))
    return sorted({code for code in codes if code})


def _conflict(
    classification: str,
    question: str,
    authoritative_source: TrackBTruthSource | None,
    conflicting_sources: Sequence[TrackBTruthSource],
    reason_codes: Sequence[str] = (),
) -> TruthConflict:
    return TruthConflict(
        classification=classification,
        question=question,
        authoritative_source=authoritative_source.source_name if authoritative_source else None,
        conflicting_sources=tuple(source.source_name for source in conflicting_sources),
        reason_codes=tuple(code for code in reason_codes if code) or (classification,),
    )


def _read_source(
    *,
    config: TrackBTruthSnapshotConfig,
    name: str,
    path: Path,
    max_age_seconds: float,
    now: datetime,
    authority_level: SourceAuthorityLevel = SourceAuthorityLevel.AUTHORITATIVE,
    diagnostic_only: bool = False,
) -> tuple[dict[str, Any], TrackBTruthSource]:
    return _read_absolute_source(
        name=name,
        path=config.resolve(path),
        max_age_seconds=max_age_seconds,
        now=now,
        authority_level=authority_level,
        diagnostic_only=diagnostic_only,
    )


def _read_absolute_source(
    *,
    name: str,
    path: Path,
    max_age_seconds: float,
    now: datetime,
    authority_level: SourceAuthorityLevel = SourceAuthorityLevel.AUTHORITATIVE,
    diagnostic_only: bool = False,
) -> tuple[dict[str, Any], TrackBTruthSource]:
    payload = _read_json(path)
    generated_at = _parse_datetime(payload.get("generated_at") or payload.get("timestamp"))
    age = (now - generated_at).total_seconds() if generated_at else float("inf")
    if not payload:
        classification = SourceClassification.MISSING.value
    elif generated_at is None:
        classification = SourceClassification.INVALID.value
    elif age > max_age_seconds:
        classification = SourceClassification.STALE.value
    else:
        classification = SourceClassification.FRESH.value
    source = TrackBTruthSource(
        source_name=name,
        artifact_path=str(path),
        generated_at=generated_at,
        freshness_seconds=age,
        max_age_seconds=max_age_seconds,
        fresh=classification == SourceClassification.FRESH.value and not diagnostic_only,
        classification=classification,
        authority_level=SourceAuthorityLevel.DIAGNOSTIC.value if diagnostic_only else authority_level.value,
        diagnostic_only=diagnostic_only,
    )
    return payload, source


def _rows(payload: Mapping[str, Any], keys: Sequence[str]) -> list[dict[str, Any]]:
    for key in keys:
        value = payload.get(key)
        if isinstance(value, list):
            return [dict(row) for row in value if isinstance(row, Mapping)]
    if isinstance(payload.get("summary"), Mapping):
        return _rows(_mapping(payload.get("summary")), keys)
    return []


def _first_evidence_row(payload: Mapping[str, Any]) -> dict[str, Any]:
    rows = _rows(payload, ("fills", "executions", "trades", "rows", "orders"))
    if rows:
        return rows[-1]
    return {}


def _lifecycle_open(row: Mapping[str, Any]) -> bool:
    state = str(row.get("state") or row.get("lifecycle_state") or row.get("status") or "").upper()
    if state in {"CLOSED", "FLAT", "RECONCILED_FLAT"}:
        return False
    return state.startswith("OPEN") or _qty(row) != 0 or bool(row.get("lifecycle_id"))


def _open_order_active(row: Mapping[str, Any]) -> bool:
    status = str(row.get("status") or row.get("order_status") or "").upper()
    return status not in {"CANCELLED", "CANCELED", "FILLED", "INACTIVE"}


def _qty(row: Mapping[str, Any]) -> float:
    for key in ("qty", "quantity", "position", "position_qty", "broker_position_qty"):
        if key not in row or row.get(key) in (None, ""):
            continue
        try:
            return float(row.get(key))
        except (TypeError, ValueError):
            continue
    return 0.0


def _path_from_payload(value: Any, *, default: Path) -> Path:
    if not value:
        return default
    return Path(str(value)).expanduser()


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


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


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _str_or_none(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _jsonable(value: Any) -> Any:
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, Mapping):
        return {str(key): _jsonable(inner) for key, inner in value.items()}
    if isinstance(value, (list, tuple)):
        return [_jsonable(inner) for inner in value]
    return value


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
