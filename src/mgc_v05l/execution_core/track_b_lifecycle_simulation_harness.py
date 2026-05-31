"""Synthetic Track B lifecycle simulation harness.

The harness exercises decision/execution handoffs with fake broker-backed
evidence. It is intentionally read-only: it never imports an IBKR adapter, never
places/cancels/modifies orders, and never mutates lifecycle state.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_canonical_truth_snapshot import (
    TrackBTruthSnapshot,
    TrackBTruthSnapshotConfig,
    build_track_b_truth_snapshot,
)
from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import (
    PRE_ACTION_BLOCKED_HARD_INVARIANT,
    PRE_ACTION_BLOCKED_PLAN_MISMATCH,
    PRE_ACTION_BLOCKED_SNAPSHOT_INCOHERENT,
    PRE_ACTION_BLOCKED_SNAPSHOT_MISSING,
    PRE_ACTION_BLOCKED_SNAPSHOT_STALE,
    PRE_ACTION_BLOCKED_SUPERVISOR_MISMATCH,
    PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH,
    PRE_ACTION_SNAPSHOT_VALID,
    TrackBPreActionSnapshotValidatorConfig,
    validate_track_b_pre_action_snapshot,
)


NOW = datetime(2026, 5, 31, 12, 0, tzinfo=UTC)

LIFECYCLE_SIMULATION_PASSED = "LIFECYCLE_SIMULATION_PASSED"
LIFECYCLE_SIMULATION_BLOCKED = "LIFECYCLE_SIMULATION_BLOCKED"
LIFECYCLE_SIMULATION_CANCELLED = "LIFECYCLE_SIMULATION_CANCELLED"

REASON_ENTRY_INTENT_VALID = "ENTRY_INTENT_VALID"
REASON_BROKER_ORDER_ACCEPTED = "BROKER_ORDER_ACCEPTED"
REASON_BROKER_BACKED_ENTRY_FILL = "BROKER_BACKED_ENTRY_FILL"
REASON_MANAGED_POSITION_ADOPTED = "MANAGED_POSITION_ADOPTED"
REASON_TIMEBOX_EXIT_DUE = "TIMEBOX_EXIT_DUE"
REASON_BROKER_BACKED_CLOSE_FILL = "BROKER_BACKED_CLOSE_FILL"
REASON_RECONCILED_FLAT = "RECONCILED_FLAT"
REASON_PASSIVE_ENTRY_CANCELLED = "PASSIVE_ENTRY_CANCELLED"
REASON_ENTRY_FILL_NOT_ADOPTED = "ENTRY_FILL_NOT_ADOPTED"
REASON_MISSING_LIFECYCLE_ID = "MISSING_LIFECYCLE_ID"
REASON_MANAGED_EXIT_POLICY_MISSING = "MANAGED_EXIT_POLICY_MISSING"
REASON_MANAGED_EXIT_BAR_COUNT_MISMATCH = "MANAGED_EXIT_BAR_COUNT_MISMATCH"
REASON_LANE_THESIS_STRATEGY_MISMATCH = "LANE_THESIS_STRATEGY_MISMATCH"
REASON_EXACT_LIFECYCLE_IDENTITY_REQUIRED = "EXACT_LIFECYCLE_IDENTITY_REQUIRED"
REASON_EXIT_CONTRACT_MISMATCH = "EXIT_CONTRACT_MISMATCH"
REASON_AGGREGATE_ACCOUNT_DIAGNOSTIC_ONLY = "AGGREGATE_ACCOUNT_DIAGNOSTIC_ONLY"
REASON_CONTROL_PLANE_SNAPSHOT_STALE = "CONTROL_PLANE_SNAPSHOT_STALE"
REASON_SAFE_STATE_SUBMIT_BLOCKED = "SAFE_STATE_SUBMIT_BLOCKED"
REASON_PLANNER_SNAPSHOT_MISMATCH = "PLANNER_SNAPSHOT_MISMATCH"
REASON_SCOPED_CLEANUP_DIAGNOSTIC_FIELDS_IGNORED = "SCOPED_CLEANUP_DIAGNOSTIC_FIELDS_IGNORED"
REASON_LOCAL_ARTIFACT_NOT_BROKER_BACKED = "LOCAL_ARTIFACT_NOT_BROKER_BACKED"
REASON_CONTRACT_CLOSE_ONLY_ENTRY_BLOCKED = "CONTRACT_CLOSE_ONLY_ENTRY_BLOCKED"
REASON_REQUIRED_FIELD_MISSING = "REQUIRED_FIELD_MISSING"
REASON_PRE_ACTION_AUTHORITY_BLOCKED = "PRE_ACTION_AUTHORITY_BLOCKED"


class SimulationValidationMode(str, Enum):
    DRY_RUN = "DRY_RUN"
    SIMULATED_LIVE_PATH = "SIMULATED_LIVE_PATH"


@dataclass(frozen=True)
class LifecycleSource:
    source_artifact_path: str
    source_artifact_timestamp: datetime


@dataclass(frozen=True)
class HandoffAuthority:
    lane_id: str
    thesis_strategy_id: str
    account_id: str
    con_id: int
    local_symbol: str
    expiry: str
    side: str
    action: str
    qty: int
    runtime_generation_id: str
    control_plane_snapshot_id: str
    safe_state_snapshot_id: str
    contract_resolver_status: str
    source: LifecycleSource


@dataclass(frozen=True)
class EntryIntentContext:
    intent_id: str
    authority: HandoffAuthority
    submit_allowed: bool = True


@dataclass(frozen=True)
class BrokerOrderContext:
    order_id: str
    client_id: str
    order_type: str
    limit_price: str
    status: str
    authority: HandoffAuthority


@dataclass(frozen=True)
class FillEvidenceContext:
    order_id: str
    client_id: str
    perm_id: str
    exec_id: str
    fill_price: str
    filled_at: datetime
    broker_backed: bool
    authority: HandoffAuthority


@dataclass(frozen=True)
class ManagedPositionContext:
    lifecycle_id: str
    entry_perm_id: str
    entry_exec_id: str
    lifecycle_state: str
    hold_policy: str
    exact_lifecycle_account_id: str
    authority: HandoffAuthority


@dataclass(frozen=True)
class ExitIntentContext:
    lifecycle_id: str
    entry_perm_id: str
    entry_exec_id: str
    exit_reason: str
    authority: HandoffAuthority


@dataclass(frozen=True)
class CloseFillContext:
    lifecycle_id: str
    order_id: str
    client_id: str
    perm_id: str
    exec_id: str
    fill_price: str
    filled_at: datetime
    broker_backed: bool
    authority: HandoffAuthority


@dataclass(frozen=True)
class ReconciliationContext:
    lifecycle_id: str
    broker_position_qty: int
    lifecycle_open_qty: int
    open_order_count: int
    reconciliation_status: str
    authority: HandoffAuthority


@dataclass(frozen=True)
class LifecycleSimulationScenario:
    scenario_id: str
    entry_intent: EntryIntentContext
    broker_order: BrokerOrderContext
    fill_evidence: FillEvidenceContext
    managed_position: ManagedPositionContext
    exit_intent: ExitIntentContext
    close_fill: CloseFillContext
    reconciliation: ReconciliationContext
    safe_state_submit_allowed: bool = True
    expected_plan_classification: str = "PLAN_SCOPED_POSITION_CLEANUP"
    expected_action_type: str = "SCOPED_POSITION_CLEANUP"
    expected_target_identity: Mapping[str, Any] | None = None
    stale_control_plane_snapshot: bool = False
    planner_snapshot_mismatch: bool = False
    scoped_cleanup_extra_diagnostic_fields: bool = False
    entry_fill_adopted: bool = True
    passive_entry_cancelled: bool = False
    local_paper_artifact_only: bool = False


@dataclass(frozen=True)
class LifecycleSimulationStageResult:
    stage: str
    entered: bool
    passed: bool
    reason_codes: tuple[str, ...]
    authority_source: str
    source_artifact_path: str


@dataclass(frozen=True)
class LifecycleSimulationResult:
    scenario_id: str
    classification: str
    passed: bool
    validation_mode: str
    terminal_state: str
    reason_codes: tuple[str, ...]
    stage_results: tuple[LifecycleSimulationStageResult, ...]
    broker_mutation_allowed: bool = False
    lifecycle_mutation_allowed: bool = False
    ibkr_mutation_allowed: bool = False
    simulated_only: bool = True


@dataclass(frozen=True)
class CanonicalTruthSimulationReportRow:
    scenario_id: str
    expected_classification: str
    actual_classification: str
    expected_reason_codes: tuple[str, ...]
    actual_reason_codes: tuple[str, ...]
    expected_conflicts: tuple[str, ...]
    actual_conflicts: tuple[str, ...]
    expected_broker_backed: bool
    actual_broker_backed: bool
    expected_submit_allowed: bool
    actual_submit_allowed: bool
    passed: bool


SCENARIO_IDS = (
    "clean_full_lifecycle",
    "passive_entry_cancel",
    "entry_fill_not_adopted",
    "managed_exit_policy_wrong_bar_count",
    "managed_exit_due_missing_lifecycle_id",
    "lane_id_vs_thesis_strategy_id_mismatch",
    "managed_exit_close_identity_contract_mismatch",
    "aggregate_account_multiple_exact_row_valid",
    "stale_control_plane_snapshot",
    "safe_state_submit_blocked",
    "planner_snapshot_mismatch",
    "scoped_cleanup_extra_diagnostic_fields",
    "local_paper_artifact_without_broker_ids",
    "contract_close_only_new_entry_blocked_exit_allowed",
)


def build_lifecycle_simulation_scenario(scenario_id: str) -> LifecycleSimulationScenario:
    """Build one deterministic fake-broker scenario."""

    if scenario_id not in SCENARIO_IDS:
        raise ValueError(f"unknown lifecycle simulation scenario: {scenario_id}")

    authority = _base_authority()
    entry = EntryIntentContext(intent_id="intent-mnq-1", authority=authority)
    order = BrokerOrderContext(
        order_id="sim-order-1",
        client_id="sim-client-7",
        order_type="LMT",
        limit_price="30395.00",
        status="Submitted",
        authority=authority,
    )
    fill = FillEvidenceContext(
        order_id=order.order_id,
        client_id=order.client_id,
        perm_id="2047276405",
        exec_id="0000e1a7.6a29f525.01.01",
        fill_price="30395.00",
        filled_at=NOW,
        broker_backed=True,
        authority=authority,
    )
    managed = ManagedPositionContext(
        lifecycle_id="bridge_fill_MNQ|1m|2026-05-31T12:00:00Z|BUY_TO_OPEN",
        entry_perm_id=fill.perm_id,
        entry_exec_id=fill.exec_id,
        lifecycle_state="OPEN_MANAGED",
        hold_policy="TIME_BOXED_EXIT_AFTER_12_COMPLETED_5M_BARS",
        exact_lifecycle_account_id=authority.account_id,
        authority=authority,
    )
    exit_intent = ExitIntentContext(
        lifecycle_id=managed.lifecycle_id,
        entry_perm_id=fill.perm_id,
        entry_exec_id=fill.exec_id,
        exit_reason="TIMEBOX_DUE",
        authority=replace(authority, side="LONG", action="SELL_TO_CLOSE"),
    )
    close_fill = CloseFillContext(
        lifecycle_id=managed.lifecycle_id,
        order_id="sim-close-order-1",
        client_id=order.client_id,
        perm_id="2047276410",
        exec_id="0000e1a7.6a29f525.01.02",
        fill_price="30405.00",
        filled_at=NOW + timedelta(minutes=60),
        broker_backed=True,
        authority=exit_intent.authority,
    )
    reconciliation = ReconciliationContext(
        lifecycle_id=managed.lifecycle_id,
        broker_position_qty=0,
        lifecycle_open_qty=0,
        open_order_count=0,
        reconciliation_status="RECONCILED_FLAT",
        authority=exit_intent.authority,
    )
    scenario = LifecycleSimulationScenario(
        scenario_id=scenario_id,
        entry_intent=entry,
        broker_order=order,
        fill_evidence=fill,
        managed_position=managed,
        exit_intent=exit_intent,
        close_fill=close_fill,
        reconciliation=reconciliation,
    )

    if scenario_id == "passive_entry_cancel":
        return replace(
            scenario,
            passive_entry_cancelled=True,
            broker_order=replace(order, status="Cancelled"),
        )
    if scenario_id == "entry_fill_not_adopted":
        return replace(
            scenario,
            entry_fill_adopted=False,
            managed_position=replace(managed, lifecycle_state="NOT_ADOPTED"),
        )
    if scenario_id == "managed_exit_policy_wrong_bar_count":
        return replace(
            scenario,
            managed_position=replace(managed, hold_policy="TIME_BOXED_EXIT_AFTER_3_COMPLETED_5M_BARS"),
        )
    if scenario_id == "managed_exit_due_missing_lifecycle_id":
        return replace(
            scenario,
            managed_position=replace(managed, lifecycle_id=""),
            exit_intent=replace(exit_intent, lifecycle_id=""),
            close_fill=replace(close_fill, lifecycle_id=""),
            reconciliation=replace(reconciliation, lifecycle_id=""),
        )
    if scenario_id == "lane_id_vs_thesis_strategy_id_mismatch":
        mismatched = replace(authority, thesis_strategy_id="mnq_unrelated_strategy")
        return _replace_all_authority(scenario, mismatched)
    if scenario_id == "managed_exit_close_identity_contract_mismatch":
        bad_exit = replace(exit_intent.authority, con_id=770561202, local_symbol="MNQU6", expiry="202609")
        return replace(
            scenario,
            exit_intent=replace(exit_intent, authority=bad_exit),
            close_fill=replace(close_fill, authority=bad_exit),
            reconciliation=replace(reconciliation, authority=bad_exit),
        )
    if scenario_id == "aggregate_account_multiple_exact_row_valid":
        aggregate = replace(authority, account_id="MULTIPLE")
        return _replace_all_authority(
            replace(scenario, managed_position=replace(managed, exact_lifecycle_account_id="DUM882026")),
            aggregate,
        )
    if scenario_id == "stale_control_plane_snapshot":
        return replace(scenario, stale_control_plane_snapshot=True)
    if scenario_id == "safe_state_submit_blocked":
        return replace(scenario, safe_state_submit_allowed=False)
    if scenario_id == "planner_snapshot_mismatch":
        return replace(scenario, planner_snapshot_mismatch=True)
    if scenario_id == "scoped_cleanup_extra_diagnostic_fields":
        return replace(scenario, scoped_cleanup_extra_diagnostic_fields=True)
    if scenario_id == "local_paper_artifact_without_broker_ids":
        return replace(
            scenario,
            local_paper_artifact_only=True,
            fill_evidence=replace(fill, perm_id="", exec_id="", broker_backed=False),
            close_fill=replace(close_fill, perm_id="", exec_id="", broker_backed=False),
        )
    if scenario_id == "contract_close_only_new_entry_blocked_exit_allowed":
        close_only = replace(authority, contract_resolver_status="CLOSE_ONLY")
        return _replace_all_authority(scenario, close_only)
    return scenario


def write_lifecycle_simulation_authority_artifacts(
    repo_root: Path,
    scenario: LifecycleSimulationScenario,
    *,
    now: datetime = NOW,
) -> None:
    """Write fake authority artifacts for tmp-dir simulations and tests only."""

    generated_at = now - timedelta(minutes=20) if scenario.stale_control_plane_snapshot else now
    snapshot_id = scenario.entry_intent.authority.control_plane_snapshot_id
    generation_id = scenario.entry_intent.authority.runtime_generation_id
    supervisor_id = "supervisor-sim-1"
    planner_snapshot_id = "stale-snapshot" if scenario.planner_snapshot_mismatch else snapshot_id
    target_identity = dict(scenario.expected_target_identity or _target_identity(scenario))
    planner_target_identity = dict(target_identity)
    if scenario.scoped_cleanup_extra_diagnostic_fields:
        planner_target_identity.update(
            {
                "manifest_id": "diagnostic-manifest-1",
                "source_event_id": "diagnostic-source-1",
            }
        )

    _write_json(
        repo_root / "outputs/track_b_execution_core/control_plane/latest_control_plane_snapshot.json",
        {
            "generated_at": generated_at.isoformat(),
            "control_plane_snapshot_id": snapshot_id,
            "shared_truth_refresh_generation_id": generation_id,
            "shared_truth_coherence_status": "COHERENT",
            "runtime_supervisor_decision_id": supervisor_id,
            "runtime_supervisor_classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "safe_to_start_runtime": True,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "agent_health_top_blockers": [],
            "agent_health_has_duplicate_writer": False,
            "agent_health_blocks_proof": False,
            "agent_health_blocks_runtime_submit": False,
            "agent_health_blocks_recovery": False,
        },
    )
    _write_json(
        repo_root / "outputs/track_b_execution_core/runtime_supervisor/latest_runtime_supervisor_authority.json",
        {
            "generated_at": now.isoformat(),
            "supervisor_decision_id": supervisor_id,
            "classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "live_money_eligible": False,
            "paper_proof_invoked": False,
        },
    )
    _write_json(
        repo_root / "outputs/track_b_execution_core/paper_autonomous_recovery/latest_paper_autonomous_recovery_plan.json",
        {
            "generated_at": now.isoformat(),
            "classification": scenario.expected_plan_classification,
            "control_plane_snapshot_id": planner_snapshot_id,
            "shared_truth_refresh_generation_id": generation_id,
            "execution_enabled": False,
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "proposed_actions": [
                {
                    "action_id": scenario.expected_action_type.lower(),
                    "action_type": scenario.expected_action_type,
                    "target_identity": planner_target_identity,
                    "execution_enabled": False,
                }
            ],
        },
    )


def write_lifecycle_simulation_truth_snapshot_artifacts(
    repo_root: Path,
    scenario: LifecycleSimulationScenario,
    *,
    now: datetime = NOW,
) -> TrackBTruthSnapshotConfig:
    """Write synthetic real-artifact inputs for TrackBTruthSnapshot tests."""

    config = TrackBTruthSnapshotConfig(repo_root=repo_root)
    auth = scenario.entry_intent.authority
    exit_auth = scenario.exit_intent.authority
    generated_at = now - timedelta(minutes=20) if scenario.stale_control_plane_snapshot else now
    lifecycle_row = _truth_lifecycle_row(scenario)
    broker_position_rows = _truth_broker_positions(scenario)
    lifecycle_open_rows = [lifecycle_row] if lifecycle_row else []
    reconciled = scenario.reconciliation.reconciliation_status in {"RECONCILED_FLAT", "BROKER_LIFECYCLE_RECONCILED"}
    if scenario.entry_fill_adopted is False:
        reconciled = False
    if not scenario.managed_position.lifecycle_id:
        reconciled = False
    if auth.lane_id != auth.thesis_strategy_id:
        reconciled = False
    if scenario.managed_position.hold_policy != "TIME_BOXED_EXIT_AFTER_12_COMPLETED_5M_BARS":
        reconciled = False
    if exit_auth.con_id != scenario.managed_position.authority.con_id or exit_auth.local_symbol != scenario.managed_position.authority.local_symbol:
        reconciled = False

    _write_json(
        repo_root / config.runtime_truth_path,
        {
            "generated_at": now.isoformat(),
            "classification": "RUNTIME_ACTIVE_TRADE_CAPABLE",
            "runtime": {
                "pid": 99901,
                "pid_alive": True,
                "runtime_instance_id": auth.runtime_generation_id,
                "lane_count": 8,
            },
            "canonical_readiness": {
                "classification": "READY_SUBMIT_CAPABLE",
                "ready_submit_capable": True,
            },
            "paper_proof_invoked": False,
            "live_money_eligible": False,
        },
    )
    _write_json(
        repo_root / config.recovery_status_path,
        {
            "generated_at": now.isoformat(),
            "classification": "RECOVERY_ACTIVE",
            "launchd_loaded": True,
            "launchd_enabled": True,
            "last_tick": now.isoformat(),
        },
    )
    _write_json(
        repo_root / config.recovery_audit_path,
        {
            "generated_at": now.isoformat(),
            "classification": "RUNTIME_HEALTHY_NO_ACTION",
            "hourly_supervisor": {"classification": "SUPERVISOR_RUNNING", "active": True},
        },
    )
    _write_json(
        repo_root / config.broker_status_path,
        {
            "generated_at": now.isoformat(),
            "positions_snapshot_path": str(repo_root / config.broker_positions_path),
            "open_orders_snapshot_path": str(repo_root / config.broker_open_orders_path),
        },
    )
    _write_json(repo_root / config.broker_positions_path, {"generated_at": now.isoformat(), "positions": broker_position_rows})
    _write_json(repo_root / config.broker_open_orders_path, {"generated_at": now.isoformat(), "open_orders": _truth_open_orders(scenario)})
    _write_json(
        repo_root / config.lifecycle_live_position_path,
        {"generated_at": now.isoformat(), "open_positions": lifecycle_open_rows},
    )
    _write_json(
        repo_root / config.managed_position_registry_path,
        {"generated_at": now.isoformat(), "managed_positions": lifecycle_open_rows},
    )
    _write_json(repo_root / config.managed_order_registry_path, {"generated_at": now.isoformat(), "managed_orders": []})
    _write_json(
        repo_root / config.reconciliation_path,
        {
            "generated_at": now.isoformat(),
            "classification": "BROKER_LIFECYCLE_RECONCILED" if reconciled else "DIRTY",
            "broker_reconciled": reconciled,
            "review_required_count": 0 if reconciled else 1,
        },
    )
    _write_json(
        repo_root / config.safe_state_path,
        {
            "generated_at": now.isoformat(),
            "classification": "SAFE_STATE_NORMAL" if scenario.safe_state_submit_allowed else "SAFE_STATE_RECOVERY_ONLY",
            "submit_allowed": scenario.safe_state_submit_allowed,
            "runtime_start_allowed": True,
            "control_plane_snapshot_id": auth.control_plane_snapshot_id,
        },
    )
    _write_json(
        repo_root / config.control_plane_path,
        {
            "generated_at": generated_at.isoformat(),
            "control_plane_snapshot_id": auth.control_plane_snapshot_id,
            "shared_truth_refresh_generation_id": auth.runtime_generation_id,
            "shared_truth_coherence_status": "COHERENT",
        },
    )
    _write_json(
        repo_root / config.planner_path,
        {
            "generated_at": now.isoformat(),
            "classification": scenario.expected_plan_classification,
            "control_plane_snapshot_id": "stale-snapshot" if scenario.planner_snapshot_mismatch else auth.control_plane_snapshot_id,
            "shared_truth_refresh_generation_id": auth.runtime_generation_id,
            "proposed_actions": [
                {
                    "action_id": scenario.expected_action_type.lower(),
                    "action_type": scenario.expected_action_type,
                    "target_identity": _target_identity(scenario),
                    "execution_enabled": False,
                }
            ],
        },
    )
    _write_json(
        repo_root / config.supervisor_path,
        {
            "generated_at": now.isoformat(),
            "classification": "SUPERVISOR_RUNTIME_START_ALLOWED",
            "supervisor_decision_id": "supervisor-sim-1",
        },
    )
    _write_json(repo_root / config.contract_status_path, _truth_contract_status(scenario, now=now))
    _write_json(repo_root / config.broker_backed_evidence_path, _truth_fill_evidence(scenario, now=now))
    _write_json(repo_root / config.local_paper_artifact_path, _truth_local_paper_artifact(scenario, now=now))
    if config.dashboard_runtime_path is not None:
        _write_json(repo_root / config.dashboard_runtime_path, {"generated_at": now.isoformat(), "diagnostic": True})
    return config


def build_lifecycle_simulation_truth_snapshot(
    *,
    repo_root: Path,
    scenario_id: str,
    now: datetime = NOW,
) -> TrackBTruthSnapshot:
    scenario = build_lifecycle_simulation_scenario(scenario_id)
    config = write_lifecycle_simulation_truth_snapshot_artifacts(repo_root, scenario, now=now)
    return build_track_b_truth_snapshot(config=config, now=now)


def build_canonical_truth_simulation_report_row(
    *,
    repo_root: Path,
    scenario_id: str,
    expected_classification: str,
    expected_reason_codes: Sequence[str] = (),
    expected_conflicts: Sequence[str] = (),
    expected_broker_backed: bool = True,
    expected_submit_allowed: bool = True,
    now: datetime = NOW,
) -> CanonicalTruthSimulationReportRow:
    snapshot = build_lifecycle_simulation_truth_snapshot(repo_root=repo_root, scenario_id=scenario_id, now=now)
    actual_conflicts = tuple(conflict.classification for conflict in snapshot.conflicts)
    expected_reasons = tuple(expected_reason_codes)
    expected_conflict_tuple = tuple(expected_conflicts)
    passed = (
        snapshot.classification == expected_classification
        and all(reason in snapshot.reason_codes for reason in expected_reasons)
        and all(conflict in actual_conflicts for conflict in expected_conflict_tuple)
        and snapshot.broker_backed_evidence.broker_backed is expected_broker_backed
        and snapshot.safe_state.submit_allowed is expected_submit_allowed
    )
    return CanonicalTruthSimulationReportRow(
        scenario_id=scenario_id,
        expected_classification=expected_classification,
        actual_classification=snapshot.classification,
        expected_reason_codes=expected_reasons,
        actual_reason_codes=snapshot.reason_codes,
        expected_conflicts=expected_conflict_tuple,
        actual_conflicts=actual_conflicts,
        expected_broker_backed=expected_broker_backed,
        actual_broker_backed=snapshot.broker_backed_evidence.broker_backed,
        expected_submit_allowed=expected_submit_allowed,
        actual_submit_allowed=snapshot.safe_state.submit_allowed,
        passed=passed,
    )


def write_canonical_truth_simulation_report(
    path: Path,
    rows: Sequence[CanonicalTruthSimulationReportRow],
    *,
    now: datetime = NOW,
) -> None:
    _write_json(
        path,
        {
            "schema_version": "track_b_canonical_truth_simulation_report_v1",
            "generated_at": now.isoformat(),
            "simulated_only": True,
            "broker_mutation_allowed": False,
            "runtime_restart_allowed": False,
            "results": [asdict(row) for row in rows],
        },
    )


def run_lifecycle_simulation_scenario(
    *,
    repo_root: Path,
    scenario_id: str,
    validation_mode: SimulationValidationMode = SimulationValidationMode.DRY_RUN,
    now: datetime = NOW,
) -> LifecycleSimulationResult:
    """Run one scenario through the shared simulation validator."""

    scenario = build_lifecycle_simulation_scenario(scenario_id)
    return validate_lifecycle_simulation_scenario(
        scenario=scenario,
        repo_root=repo_root,
        validation_mode=validation_mode,
        now=now,
    )


def validate_lifecycle_simulation_scenario(
    *,
    scenario: LifecycleSimulationScenario,
    repo_root: Path,
    validation_mode: SimulationValidationMode,
    now: datetime = NOW,
) -> LifecycleSimulationResult:
    """Shared dry-run/simulated-live validation path."""

    stage_results: list[LifecycleSimulationStageResult] = []
    reason_codes: list[str] = []

    pre_action = validate_track_b_pre_action_snapshot(
        config=TrackBPreActionSnapshotValidatorConfig(repo_root=repo_root),
        expected_plan_classification=scenario.expected_plan_classification,
        expected_action_type=scenario.expected_action_type,
        expected_target_identity=scenario.expected_target_identity or _target_identity(scenario),
        max_snapshot_age_seconds=300,
        expected_snapshot_id=scenario.entry_intent.authority.control_plane_snapshot_id,
        expected_shared_truth_generation_id=scenario.entry_intent.authority.runtime_generation_id,
        now=now,
    )
    if pre_action["classification"] != PRE_ACTION_SNAPSHOT_VALID:
        mapped = _map_pre_action_reason(pre_action["classification"])
        return _blocked_result(
            scenario=scenario,
            validation_mode=validation_mode,
            stage_results=[
                _stage(
                    "pre_action_authority",
                    False,
                    (mapped, REASON_PRE_ACTION_AUTHORITY_BLOCKED),
                    scenario.entry_intent.authority,
                )
            ],
            reason_codes=(mapped, REASON_PRE_ACTION_AUTHORITY_BLOCKED),
            terminal_state=str(pre_action["classification"]),
        )

    if not scenario.safe_state_submit_allowed:
        return _blocked_result(
            scenario=scenario,
            validation_mode=validation_mode,
            stage_results=[
                _stage(
                    "safe_state_submit_authority",
                    False,
                    (REASON_SAFE_STATE_SUBMIT_BLOCKED,),
                    scenario.entry_intent.authority,
                )
            ],
            reason_codes=(REASON_SAFE_STATE_SUBMIT_BLOCKED,),
            terminal_state="SAFE_STATE_SUBMIT_BLOCKED",
        )

    missing = _missing_identity_fields(scenario)
    if missing:
        lifecycle_missing = REASON_MISSING_LIFECYCLE_ID if "managed_position.lifecycle_id" in missing else ""
        missing_reasons = (
            (REASON_REQUIRED_FIELD_MISSING,)
            + ((lifecycle_missing,) if lifecycle_missing else ())
            + tuple(f"MISSING_{field}" for field in missing)
        )
        return _blocked_result(
            scenario=scenario,
            validation_mode=validation_mode,
            stage_results=[
                _stage(
                    "identity_contract",
                    False,
                    missing_reasons,
                    scenario.entry_intent.authority,
                )
            ],
            reason_codes=missing_reasons,
            terminal_state="IDENTITY_CONTRACT_FAILED",
        )

    if scenario.entry_intent.authority.contract_resolver_status == "CLOSE_ONLY":
        return _blocked_result(
            scenario=scenario,
            validation_mode=validation_mode,
            stage_results=[
                _stage(
                    "contract_resolver_entry_gate",
                    False,
                    (REASON_CONTRACT_CLOSE_ONLY_ENTRY_BLOCKED,),
                    scenario.entry_intent.authority,
                ),
                _stage(
                    "contract_resolver_exit_gate",
                    True,
                    ("EXIT_ALLOWED_ON_ORIGINAL_FILLED_CONTRACT",),
                    scenario.exit_intent.authority,
                ),
            ],
            reason_codes=(REASON_CONTRACT_CLOSE_ONLY_ENTRY_BLOCKED, "EXIT_ALLOWED_ON_ORIGINAL_FILLED_CONTRACT"),
            terminal_state="ENTRY_BLOCKED_EXIT_ALLOWED_CLOSE_ONLY_CONTRACT",
        )

    if scenario.entry_intent.authority.lane_id != scenario.entry_intent.authority.thesis_strategy_id:
        return _blocked_result(
            scenario=scenario,
            validation_mode=validation_mode,
            stage_results=[
                _stage(
                    "strategy_signal_to_position_intent",
                    False,
                    (REASON_LANE_THESIS_STRATEGY_MISMATCH,),
                    scenario.entry_intent.authority,
                )
            ],
            reason_codes=(REASON_LANE_THESIS_STRATEGY_MISMATCH,),
            terminal_state="LANE_THESIS_MISMATCH_BLOCKED",
        )

    if scenario.scoped_cleanup_extra_diagnostic_fields:
        reason_codes.append(REASON_SCOPED_CLEANUP_DIAGNOSTIC_FIELDS_IGNORED)

    stage_results.append(_stage("entry_intent", True, (REASON_ENTRY_INTENT_VALID,), scenario.entry_intent.authority))
    reason_codes.append(REASON_ENTRY_INTENT_VALID)

    if scenario.passive_entry_cancelled:
        stage_results.append(
            _stage("broker_order", False, (REASON_PASSIVE_ENTRY_CANCELLED,), scenario.broker_order.authority)
        )
        return LifecycleSimulationResult(
            scenario_id=scenario.scenario_id,
            classification=LIFECYCLE_SIMULATION_CANCELLED,
            passed=False,
            validation_mode=validation_mode.value,
            terminal_state="ENTRY_ORDER_CANCELLED_BEFORE_FILL",
            reason_codes=tuple(reason_codes + [REASON_PASSIVE_ENTRY_CANCELLED]),
            stage_results=tuple(stage_results),
        )

    stage_results.append(_stage("broker_order", True, (REASON_BROKER_ORDER_ACCEPTED,), scenario.broker_order.authority))
    reason_codes.append(REASON_BROKER_ORDER_ACCEPTED)

    if scenario.local_paper_artifact_only or not _has_broker_backed_fill(scenario.fill_evidence):
        stage_results.append(
            _stage("entry_fill_evidence", False, (REASON_LOCAL_ARTIFACT_NOT_BROKER_BACKED,), scenario.fill_evidence.authority)
        )
        return LifecycleSimulationResult(
            scenario_id=scenario.scenario_id,
            classification=LIFECYCLE_SIMULATION_BLOCKED,
            passed=False,
            validation_mode=validation_mode.value,
            terminal_state="ENTRY_FILL_NOT_BROKER_BACKED",
            reason_codes=tuple(reason_codes + [REASON_LOCAL_ARTIFACT_NOT_BROKER_BACKED]),
            stage_results=tuple(stage_results),
        )

    stage_results.append(_stage("entry_fill_evidence", True, (REASON_BROKER_BACKED_ENTRY_FILL,), scenario.fill_evidence.authority))
    reason_codes.append(REASON_BROKER_BACKED_ENTRY_FILL)

    if not scenario.entry_fill_adopted:
        stage_results.append(
            _stage("lifecycle_adoption", False, (REASON_ENTRY_FILL_NOT_ADOPTED,), scenario.managed_position.authority)
        )
        return LifecycleSimulationResult(
            scenario_id=scenario.scenario_id,
            classification=LIFECYCLE_SIMULATION_BLOCKED,
            passed=False,
            validation_mode=validation_mode.value,
            terminal_state="BROKER_FILL_NOT_ADOPTED_BY_LIFECYCLE",
            reason_codes=tuple(reason_codes + [REASON_ENTRY_FILL_NOT_ADOPTED]),
            stage_results=tuple(stage_results),
        )

    stage_results.append(_stage("managed_position", True, (REASON_MANAGED_POSITION_ADOPTED,), scenario.managed_position.authority))
    reason_codes.append(REASON_MANAGED_POSITION_ADOPTED)

    policy_reasons = _managed_exit_policy_reasons(scenario)
    if policy_reasons:
        stage_results.append(
            _stage("managed_exit_policy", False, policy_reasons, scenario.managed_position.authority)
        )
        return LifecycleSimulationResult(
            scenario_id=scenario.scenario_id,
            classification=LIFECYCLE_SIMULATION_BLOCKED,
            passed=False,
            validation_mode=validation_mode.value,
            terminal_state="MANAGED_EXIT_POLICY_CONTRACT_FAILED",
            reason_codes=tuple(reason_codes + list(policy_reasons)),
            stage_results=tuple(stage_results),
        )

    if scenario.managed_position.authority.account_id == "MULTIPLE":
        stage_results.append(
            _stage(
                "exact_lifecycle_owner_resolution",
                True,
                (REASON_AGGREGATE_ACCOUNT_DIAGNOSTIC_ONLY,),
                scenario.managed_position.authority,
            )
        )
        reason_codes.append(REASON_AGGREGATE_ACCOUNT_DIAGNOSTIC_ONLY)

    stage_results.append(_stage("exit_intent", True, (REASON_TIMEBOX_EXIT_DUE,), scenario.exit_intent.authority))
    reason_codes.append(REASON_TIMEBOX_EXIT_DUE)

    identity_reasons = _exit_identity_reasons(scenario)
    if identity_reasons:
        stage_results.append(
            _stage("exact_lifecycle_exit_identity", False, identity_reasons, scenario.exit_intent.authority)
        )
        return LifecycleSimulationResult(
            scenario_id=scenario.scenario_id,
            classification=LIFECYCLE_SIMULATION_BLOCKED,
            passed=False,
            validation_mode=validation_mode.value,
            terminal_state="EXACT_LIFECYCLE_EXIT_IDENTITY_FAILED",
            reason_codes=tuple(reason_codes + list(identity_reasons)),
            stage_results=tuple(stage_results),
        )

    if not _has_broker_backed_close(scenario.close_fill):
        stage_results.append(
            _stage("close_fill_evidence", False, (REASON_LOCAL_ARTIFACT_NOT_BROKER_BACKED,), scenario.close_fill.authority)
        )
        return LifecycleSimulationResult(
            scenario_id=scenario.scenario_id,
            classification=LIFECYCLE_SIMULATION_BLOCKED,
            passed=False,
            validation_mode=validation_mode.value,
            terminal_state="CLOSE_FILL_NOT_BROKER_BACKED",
            reason_codes=tuple(reason_codes + [REASON_LOCAL_ARTIFACT_NOT_BROKER_BACKED]),
            stage_results=tuple(stage_results),
        )

    stage_results.append(_stage("close_fill_evidence", True, (REASON_BROKER_BACKED_CLOSE_FILL,), scenario.close_fill.authority))
    reason_codes.append(REASON_BROKER_BACKED_CLOSE_FILL)
    stage_results.append(_stage("reconciliation", True, (REASON_RECONCILED_FLAT,), scenario.reconciliation.authority))
    reason_codes.append(REASON_RECONCILED_FLAT)

    return LifecycleSimulationResult(
        scenario_id=scenario.scenario_id,
        classification=LIFECYCLE_SIMULATION_PASSED,
        passed=True,
        validation_mode=validation_mode.value,
        terminal_state="RECONCILED_FLAT",
        reason_codes=tuple(reason_codes),
        stage_results=tuple(stage_results),
    )


def write_lifecycle_simulation_report(path: Path, results: Sequence[LifecycleSimulationResult]) -> None:
    payload = {
        "schema_version": "track_b_lifecycle_simulation_report_v1",
        "generated_at": NOW.isoformat(),
        "simulated_only": True,
        "ibkr_mutation_allowed": False,
        "results": [simulation_result_to_dict(result) for result in results],
    }
    _write_json(path, payload)


def simulation_result_to_dict(result: LifecycleSimulationResult) -> dict[str, Any]:
    payload = asdict(result)
    payload["stage_results"] = [asdict(stage) for stage in result.stage_results]
    return payload


def _truth_lifecycle_row(scenario: LifecycleSimulationScenario) -> dict[str, Any]:
    if scenario.passive_entry_cancelled or scenario.entry_fill_adopted is False:
        return {}
    managed = scenario.managed_position
    auth = managed.authority
    return {
        "lifecycle_id": managed.lifecycle_id,
        "lane_id": auth.lane_id,
        "thesis_strategy_id": auth.thesis_strategy_id,
        "strategy_id": auth.thesis_strategy_id,
        "account_id": auth.account_id,
        "exact_lifecycle_account_id": managed.exact_lifecycle_account_id,
        "con_id": auth.con_id,
        "localSymbol": auth.local_symbol,
        "expiry": auth.expiry,
        "qty": auth.qty,
        "side": auth.side,
        "action": auth.action,
        "entry_perm_id": managed.entry_perm_id,
        "entry_exec_id": managed.entry_exec_id,
        "hold_policy": managed.hold_policy,
        "state": managed.lifecycle_state,
        "exit_con_id": scenario.exit_intent.authority.con_id,
        "exit_localSymbol": scenario.exit_intent.authority.local_symbol,
    }


def _truth_broker_positions(scenario: LifecycleSimulationScenario) -> list[dict[str, Any]]:
    if scenario.passive_entry_cancelled:
        return []
    if scenario.entry_fill_adopted is False:
        auth = scenario.fill_evidence.authority
        return [_broker_position_row(auth)]
    if scenario.reconciliation.broker_position_qty == 0 and scenario.reconciliation.reconciliation_status == "RECONCILED_FLAT":
        if scenario.scenario_id in {
            "managed_exit_policy_wrong_bar_count",
            "managed_exit_due_missing_lifecycle_id",
            "lane_id_vs_thesis_strategy_id_mismatch",
            "managed_exit_close_identity_contract_mismatch",
            "aggregate_account_multiple_exact_row_valid",
        }:
            return [_broker_position_row(scenario.managed_position.authority)]
        return []
    return [_broker_position_row(scenario.managed_position.authority)]


def _broker_position_row(auth: HandoffAuthority) -> dict[str, Any]:
    return {
        "symbol": _strategy_symbol(auth.local_symbol),
        "localSymbol": auth.local_symbol,
        "conId": auth.con_id,
        "expiry": auth.expiry,
        "account": "DUM882026" if auth.account_id == "MULTIPLE" else auth.account_id,
        "position": auth.qty,
    }


def _truth_open_orders(scenario: LifecycleSimulationScenario) -> list[dict[str, Any]]:
    if not scenario.passive_entry_cancelled:
        return []
    return [
        {
            "order_id": scenario.broker_order.order_id,
            "client_id": scenario.broker_order.client_id,
            "status": scenario.broker_order.status,
            "symbol": _strategy_symbol(scenario.broker_order.authority.local_symbol),
        }
    ]


def _truth_contract_status(scenario: LifecycleSimulationScenario, *, now: datetime) -> dict[str, Any]:
    auth = scenario.entry_intent.authority
    if auth.contract_resolver_status == "CLOSE_ONLY":
        classification = "CONTRACT_EXIT_OR_MANAGEMENT_ALLOWED"
        entry_status = "CONTRACT_ENTRY_CLOSE_ONLY"
        exit_status = "EXIT_ORIGINAL_CONTRACT_ALLOWED"
    else:
        classification = "CONTRACT_ALLOWED"
        entry_status = "CONTRACT_ENTRY_ELIGIBLE"
        exit_status = "EXIT_STATUS_UNKNOWN"
    return {
        "generated_at": now.isoformat(),
        "classification": classification,
        "symbol": _strategy_symbol(auth.local_symbol),
        "entry_status": entry_status,
        "exit_status": exit_status,
        "submit_allowed": entry_status == "CONTRACT_ENTRY_ELIGIBLE",
        "selected_contract": {
            "localSymbol": auth.local_symbol,
            "conId": auth.con_id,
            "expiry": auth.expiry,
            "symbol": _strategy_symbol(auth.local_symbol),
        },
    }


def _truth_fill_evidence(scenario: LifecycleSimulationScenario, *, now: datetime) -> dict[str, Any]:
    if scenario.passive_entry_cancelled:
        return {"generated_at": now.isoformat(), "fills": []}
    fill = scenario.fill_evidence
    return {
        "generated_at": now.isoformat(),
        "fills": [
            {
                "order_id": fill.order_id,
                "client_id": fill.client_id,
                "perm_id": fill.perm_id,
                "exec_id": fill.exec_id,
                "fill_price": fill.fill_price,
            }
        ],
    }


def _truth_local_paper_artifact(scenario: LifecycleSimulationScenario, *, now: datetime) -> dict[str, Any]:
    if scenario.local_paper_artifact_only:
        return {
            "generated_at": now.isoformat(),
            "fills": [
                {
                    "order_id": scenario.fill_evidence.order_id or "paper-local-order",
                    "client_id": scenario.fill_evidence.client_id or "paper-local-client",
                    "source": "local_paper_artifact",
                }
            ],
        }
    return {"generated_at": now.isoformat(), "local_rows": []}


def _base_authority() -> HandoffAuthority:
    return HandoffAuthority(
        lane_id="mnq_us_active_participation_long",
        thesis_strategy_id="mnq_us_active_participation_long",
        account_id="DUM882026",
        con_id=770561201,
        local_symbol="MNQM6",
        expiry="202606",
        side="LONG",
        action="BUY_TO_OPEN",
        qty=1,
        runtime_generation_id="generation-sim-1",
        control_plane_snapshot_id="snapshot-sim-1",
        safe_state_snapshot_id="safe-state-sim-1",
        contract_resolver_status="ALLOWED",
        source=LifecycleSource(
            source_artifact_path="synthetic://track_b_lifecycle_simulation/base",
            source_artifact_timestamp=NOW,
        ),
    )


def _replace_all_authority(
    scenario: LifecycleSimulationScenario,
    authority: HandoffAuthority,
) -> LifecycleSimulationScenario:
    exit_authority = replace(authority, side="LONG", action="SELL_TO_CLOSE")
    return replace(
        scenario,
        entry_intent=replace(scenario.entry_intent, authority=authority),
        broker_order=replace(scenario.broker_order, authority=authority),
        fill_evidence=replace(scenario.fill_evidence, authority=authority),
        managed_position=replace(scenario.managed_position, authority=authority),
        exit_intent=replace(scenario.exit_intent, authority=exit_authority),
        close_fill=replace(scenario.close_fill, authority=exit_authority),
        reconciliation=replace(scenario.reconciliation, authority=exit_authority),
    )


def _target_identity(scenario: LifecycleSimulationScenario) -> Mapping[str, Any]:
    auth = scenario.exit_intent.authority
    return {
        "account_id": scenario.managed_position.exact_lifecycle_account_id,
        "lane_id": auth.lane_id,
        "strategy_id": auth.thesis_strategy_id,
        "symbol": _strategy_symbol(auth.local_symbol),
        "contract": auth.local_symbol,
        "lifecycle_id": scenario.exit_intent.lifecycle_id,
        "con_id": auth.con_id,
        "localSymbol": auth.local_symbol,
        "side": auth.side,
        "quantity": auth.qty,
        "action": auth.action,
    }


def _missing_identity_fields(scenario: LifecycleSimulationScenario) -> tuple[str, ...]:
    missing: list[str] = []
    for label, authority in (
        ("entry.lane_id", scenario.entry_intent.authority),
        ("entry.thesis_strategy_id", scenario.entry_intent.authority),
        ("entry.account_id", scenario.entry_intent.authority),
        ("entry.localSymbol", scenario.entry_intent.authority),
        ("entry.expiry", scenario.entry_intent.authority),
        ("entry.runtime_generation_id", scenario.entry_intent.authority),
        ("entry.control_plane_snapshot_id", scenario.entry_intent.authority),
        ("entry.safe_state_snapshot_id", scenario.entry_intent.authority),
        ("entry.contract_resolver_status", scenario.entry_intent.authority),
        ("exit.lane_id", scenario.exit_intent.authority),
    ):
        if not getattr(authority, label.split(".")[1].replace("localSymbol", "local_symbol")):
            missing.append(label)
    if scenario.entry_intent.authority.con_id <= 0:
        missing.append("entry.conId")
    if scenario.entry_intent.authority.qty <= 0:
        missing.append("entry.qty")
    if not scenario.broker_order.order_id:
        missing.append("broker_order.order_id")
    if not scenario.broker_order.client_id:
        missing.append("broker_order.client_id")
    if not scenario.managed_position.lifecycle_id:
        missing.append("managed_position.lifecycle_id")
        missing.append("exit_intent.lifecycle_id")
    return tuple(missing)


def _strategy_symbol(local_symbol: str) -> str:
    for prefix in ("MNQ", "MES", "MGC", "GC"):
        if local_symbol.startswith(prefix):
            return prefix
    return local_symbol


def _has_broker_backed_fill(fill: FillEvidenceContext) -> bool:
    return fill.broker_backed and bool(fill.perm_id and fill.exec_id and fill.order_id and fill.client_id)


def _has_broker_backed_close(close: CloseFillContext) -> bool:
    return close.broker_backed and bool(close.perm_id and close.exec_id and close.order_id and close.client_id)


def _managed_exit_policy_reasons(scenario: LifecycleSimulationScenario) -> tuple[str, ...]:
    expected_policy = "TIME_BOXED_EXIT_AFTER_12_COMPLETED_5M_BARS"
    actual_policy = scenario.managed_position.hold_policy
    if not actual_policy:
        return (REASON_MANAGED_EXIT_POLICY_MISSING,)
    if actual_policy != expected_policy:
        return (REASON_MANAGED_EXIT_BAR_COUNT_MISMATCH,)
    return ()


def _exit_identity_reasons(scenario: LifecycleSimulationScenario) -> tuple[str, ...]:
    managed = scenario.managed_position
    exit_intent = scenario.exit_intent
    reasons: list[str] = []
    if managed.lifecycle_id != exit_intent.lifecycle_id:
        reasons.append(REASON_EXACT_LIFECYCLE_IDENTITY_REQUIRED)
    if managed.entry_perm_id != exit_intent.entry_perm_id or managed.entry_exec_id != exit_intent.entry_exec_id:
        reasons.append(REASON_EXACT_LIFECYCLE_IDENTITY_REQUIRED)
    managed_account = managed.exact_lifecycle_account_id or managed.authority.account_id
    exit_account = managed.exact_lifecycle_account_id if exit_intent.authority.account_id == "MULTIPLE" else exit_intent.authority.account_id
    if managed_account != exit_account:
        reasons.append(REASON_EXACT_LIFECYCLE_IDENTITY_REQUIRED)
    if managed.authority.con_id != exit_intent.authority.con_id or managed.authority.local_symbol != exit_intent.authority.local_symbol:
        reasons.append(REASON_EXIT_CONTRACT_MISMATCH)
    if managed.authority.qty != exit_intent.authority.qty:
        reasons.append(REASON_EXACT_LIFECYCLE_IDENTITY_REQUIRED)
    if reasons:
        return tuple(dict.fromkeys((REASON_EXACT_LIFECYCLE_IDENTITY_REQUIRED, *reasons)))
    return ()


def _map_pre_action_reason(classification: str) -> str:
    return {
        PRE_ACTION_BLOCKED_SNAPSHOT_STALE: REASON_CONTROL_PLANE_SNAPSHOT_STALE,
        PRE_ACTION_BLOCKED_PLAN_MISMATCH: REASON_PLANNER_SNAPSHOT_MISMATCH,
        PRE_ACTION_BLOCKED_TARGET_IDENTITY_MISMATCH: REASON_PLANNER_SNAPSHOT_MISMATCH,
        PRE_ACTION_BLOCKED_SNAPSHOT_MISSING: REASON_PRE_ACTION_AUTHORITY_BLOCKED,
        PRE_ACTION_BLOCKED_SNAPSHOT_INCOHERENT: REASON_PRE_ACTION_AUTHORITY_BLOCKED,
        PRE_ACTION_BLOCKED_SUPERVISOR_MISMATCH: REASON_PRE_ACTION_AUTHORITY_BLOCKED,
        PRE_ACTION_BLOCKED_HARD_INVARIANT: REASON_PRE_ACTION_AUTHORITY_BLOCKED,
    }.get(classification, REASON_PRE_ACTION_AUTHORITY_BLOCKED)


def _blocked_result(
    *,
    scenario: LifecycleSimulationScenario,
    validation_mode: SimulationValidationMode,
    stage_results: Sequence[LifecycleSimulationStageResult],
    reason_codes: Sequence[str],
    terminal_state: str,
) -> LifecycleSimulationResult:
    return LifecycleSimulationResult(
        scenario_id=scenario.scenario_id,
        classification=LIFECYCLE_SIMULATION_BLOCKED,
        passed=False,
        validation_mode=validation_mode.value,
        terminal_state=terminal_state,
        reason_codes=tuple(reason_codes),
        stage_results=tuple(stage_results),
    )


def _stage(
    stage: str,
    passed: bool,
    reason_codes: Sequence[str],
    authority: HandoffAuthority,
) -> LifecycleSimulationStageResult:
    return LifecycleSimulationStageResult(
        stage=stage,
        entered=True,
        passed=passed,
        reason_codes=tuple(reason_codes),
        authority_source="SYNTHETIC_BROKER_BACKED_SIMULATION",
        source_artifact_path=authority.source.source_artifact_path,
    )


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(_jsonable(payload), indent=2, sort_keys=True) + "\n", encoding="utf-8")


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
