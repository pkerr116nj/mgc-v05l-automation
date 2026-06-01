"""Central unattended lifecycle maintenance for Track B PAPER positions.

This boundary keeps broker-effect PAPER entries converged into local lifecycle
truth, then delegates exit handling to the managed exit roster/profile path.
It never creates one-off flatten behavior and it never uses dashboard
projections as authority.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.app.track_b_paper_lifecycle_adoption import (
    LifecycleAdoptionConfig,
    run_track_b_paper_lifecycle_adoption,
)

from .track_b_atomic_io import write_json_atomic
from .track_b_broker_position_guardian import (
    TrackBBrokerPositionGuardianConfig,
    build_track_b_broker_position_guardian,
    write_track_b_broker_position_guardian,
)
from .track_b_control_plane_snapshot import (
    CONTROL_PLANE_SNAPSHOT_READY,
    TrackBControlPlaneSnapshotConfig,
    build_track_b_control_plane_snapshot,
    write_track_b_control_plane_snapshot,
)
from .track_b_exit_strategy_roster import (
    ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
    ACTIVE_EVIDENCE_MANAGED_CLOSE_WIDEN_AFTER_SECONDS,
    managed_close_limit_from_reference,
)
from .track_b_managed_exit_attach import TrackBManagedExitAttachConfig, run_track_b_managed_exit_attach
from .track_b_managed_order_modify_in_place import (
    IbkrPaperManagedOrderModifyAdapter,
    ManagedOrderModifyInPlaceConfig,
    run_track_b_managed_order_modify_in_place,
)
from .track_b_managed_order_registry import (
    TrackBManagedOrderRegistryConfig,
    build_track_b_managed_order_registry,
    write_track_b_managed_order_registry,
)
from .track_b_managed_position_registry import (
    TrackBManagedPositionRegistryConfig,
    build_track_b_managed_position_registry,
    write_track_b_managed_position_registry,
)
from .track_b_open_order_truth import (
    TrackBOpenOrderTruthConfig,
    build_track_b_open_order_truth,
    write_track_b_open_order_truth,
)
from .track_b_order_adjustment_planner import (
    BROKER_FLAT_NO_REPLACE,
    MODIFY_IN_PLACE_ELIGIBLE,
    TrackBOrderAdjustmentPlannerConfig,
    build_track_b_order_adjustment_plan,
    write_track_b_order_adjustment_plan,
)
from .track_b_paper_broker_reconciliation import ReconciliationConfig, reconcile_track_b_paper_broker_truth
from .track_b_paper_proof_readiness import (
    READY_FOR_PROOF,
    TrackBPaperProofReadinessConfig,
    build_track_b_paper_proof_readiness,
    write_track_b_paper_proof_readiness,
)
from .track_b_managed_exit_order_resolution import (
    ManagedExitOrderResolutionConfig,
    resolve_known_managed_exit_order_disappearance,
)
from .track_b_position_truth_monitor import (
    TrackBPositionTruthMonitorConfig,
    build_track_b_position_truth,
    write_track_b_position_truth,
)


REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_OUTPUT_PATH = (
    Path("outputs")
    / "track_b_execution_core"
    / "diagnostics"
    / "latest_unattended_lifecycle_maintenance.json"
)

UNATTENDED_LIFECYCLE_MAINTENANCE_READY = "UNATTENDED_LIFECYCLE_MAINTENANCE_READY"
UNATTENDED_LIFECYCLE_MAINTENANCE_ACTIVE_HOLD = "UNATTENDED_LIFECYCLE_MAINTENANCE_ACTIVE_HOLD"
UNATTENDED_LIFECYCLE_MAINTENANCE_EXIT_DUE = "UNATTENDED_LIFECYCLE_MAINTENANCE_EXIT_DUE"
UNATTENDED_LIFECYCLE_MAINTENANCE_CLOSE_ORDER_MANAGED = "UNATTENDED_LIFECYCLE_MAINTENANCE_CLOSE_ORDER_MANAGED"
UNATTENDED_LIFECYCLE_MAINTENANCE_BLOCKED_AMBIGUOUS = "UNATTENDED_LIFECYCLE_MAINTENANCE_BLOCKED_AMBIGUOUS"


@dataclass(frozen=True)
class TrackBUnattendedLifecycleMaintenanceConfig:
    repo_root: Path = REPO_ROOT
    output_path: Path = DEFAULT_OUTPUT_PATH
    account_id: str = "DUM882026"
    symbols: str = "MGC,MNQ,GC,PL"
    local_adoption_enabled: bool = True
    exit_apply_enabled: bool = False
    operator_authorized_managed_exit: bool = False
    order_modify_apply_enabled: bool = False
    operator_authorized_modify: bool = False
    broker_flat_close_cleanup_enabled: bool = True
    readiness_recovery_handoff_enabled: bool = True

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def run_unattended_lifecycle_maintenance(
    *,
    config: TrackBUnattendedLifecycleMaintenanceConfig,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = now or datetime.now(UTC)
    reconciliation_before = _refresh_reconciliation(config=config, now=actual_now)
    adoption_results = _run_pending_adoptions(config=config, reconciliation=reconciliation_before)
    truth_after_adoption = _refresh_truth_stack(config=config, now=actual_now)
    exit_plans = _run_due_exit_plans(config=config, managed_positions=truth_after_adoption["managed_positions"], now=actual_now)
    truth_after_exit = _refresh_truth_stack(config=config, now=actual_now)
    order_adjustment_plan = _refresh_order_adjustment_plan(config=config, now=actual_now)
    working_close_order_management = _run_working_close_order_management(
        config=config,
        managed_orders=truth_after_exit["managed_orders"],
        order_adjustment_plan=order_adjustment_plan,
        now=actual_now,
    )
    truth_after_order_management = _refresh_truth_stack(config=config, now=actual_now)
    broker_flat_close_cleanup = _run_broker_flat_close_cleanup(
        config=config,
        managed_orders=truth_after_order_management["managed_orders"],
        order_adjustment_plan=order_adjustment_plan,
        now=actual_now,
    )
    lifecycle_without_broker_cleanup = _run_lifecycle_without_broker_cleanup(
        config=config,
        managed_positions=truth_after_order_management["managed_positions"],
        now=actual_now,
    )
    truth_final = _refresh_truth_stack(config=config, now=actual_now)
    classification = _classification(
        truth_final=truth_final,
        exit_plans=exit_plans,
        working_close_order_management=working_close_order_management,
        broker_flat_close_cleanup=broker_flat_close_cleanup,
        lifecycle_without_broker_cleanup=lifecycle_without_broker_cleanup,
    )
    readiness_recovery_handoff = _run_readiness_recovery_handoff(
        config=config,
        truth_final=truth_final,
        classification=classification,
        now=actual_now,
    )
    report = {
        "schema_version": "track_b_unattended_lifecycle_maintenance_v1",
        "generated_at": actual_now.isoformat(),
        "mode": "PAPER",
        "paper_only": True,
        "live_money_eligible": False,
        "paper_proof_invoked": False,
        "dashboard_projection_consumed": False,
        "broad_cancel_allowed": False,
        "global_flatten_allowed": False,
        "unmanaged_submit_allowed": False,
        "classification": classification,
        "local_adoption_enabled": config.local_adoption_enabled,
        "exit_apply_enabled": config.exit_apply_enabled,
        "operator_authorized_managed_exit": config.operator_authorized_managed_exit,
        "order_modify_apply_enabled": config.order_modify_apply_enabled,
        "operator_authorized_modify": config.operator_authorized_modify,
        "broker_flat_close_cleanup_enabled": config.broker_flat_close_cleanup_enabled,
        "readiness_recovery_handoff_enabled": config.readiness_recovery_handoff_enabled,
        "adoption_results": adoption_results,
        "managed_exit_plans": exit_plans,
        "order_adjustment_plan": {
            "classification": order_adjustment_plan.get("classification"),
            "plan_count": (order_adjustment_plan.get("summary") or {}).get("plan_count"),
            "modify_in_place_eligible_count": (order_adjustment_plan.get("summary") or {}).get(
                "modify_in_place_eligible_count"
            ),
        },
        "working_close_order_management": working_close_order_management,
        "broker_flat_close_cleanup": broker_flat_close_cleanup,
        "lifecycle_without_broker_cleanup": lifecycle_without_broker_cleanup,
        "readiness_recovery_handoff": readiness_recovery_handoff,
        "root_cause_controls": {
            "fresh_order_adjustment_plan_rebuilt_each_pass": True,
            "exit_due_positions_routed_by_exit_roster_profile": True,
            "working_close_orders_managed_by_modify_in_place": True,
            "known_close_order_flat_cleanup_enabled": config.broker_flat_close_cleanup_enabled,
            "lifecycle_without_broker_flat_cleanup_enabled": config.broker_flat_close_cleanup_enabled,
            "readiness_recovery_handoff_after_clean_truth": config.readiness_recovery_handoff_enabled,
            "control_plane_rebuilt_after_readiness_refresh": readiness_recovery_handoff.get(
                "control_plane_refreshed"
            )
            is True,
            "duplicate_close_orders_blocked_by_managed_order_registry_and_guardian": True,
            "dashboard_projection_consumed": False,
        },
        "truth": {
            "reconciliation": truth_final["reconciliation"].get("classification"),
            "guardian": truth_final["guardian"].get("classification"),
            "position_truth": truth_final["position_truth"].get("classification")
            or (truth_final["position_truth"].get("summary") or {}).get("overall_classification"),
            "open_order_truth": truth_final["open_order_truth"].get("classification"),
            "managed_positions": truth_final["managed_positions"].get("classification"),
            "managed_orders": truth_final["managed_orders"].get("classification"),
        },
        "active_managed_positions": truth_final["managed_positions"].get("managed_positions", []),
        "blocked_reason": _blocked_reason(truth_final=truth_final),
        "future_lane_handling": (
            "Future leak-test lanes should call this pass after broker-effect submits: adopt known broker-backed "
            "fills, refresh truth, let the exit roster/profile boundary handle due exits, rebuild the order "
            "adjustment plan, then manage known working close orders through modify-in-place when exact identity "
            "is clean. Duplicate exits remain blocked by managed order registry and guardian checks."
        ),
        "output_path": str(config.resolve(config.output_path)),
    }
    write_json_atomic(config.resolve(config.output_path), report)
    return report


def _refresh_reconciliation(
    *, config: TrackBUnattendedLifecycleMaintenanceConfig, now: datetime
) -> dict[str, Any]:
    reconciliation_config = ReconciliationConfig(
        repo_root=config.repo_root,
        ledger_root=config.repo_root / "outputs" / "track_b_execution_core" / "paper_trade_ledger",
        broker_truth_root=config.repo_root / "outputs" / "reports" / "ibkr_read_only_verification",
        market_data_root=config.repo_root / "outputs" / "track_b_execution_core" / "phase1_runtime_market_data",
        report_path=config.repo_root
        / "outputs"
        / "reports"
        / "track_b_paper_broker_reconciliation"
        / "latest_track_b_paper_broker_reconciliation.json",
        account=config.account_id,
        symbols=tuple(item.strip() for item in config.symbols.split(",") if item.strip()),
    )
    return reconcile_track_b_paper_broker_truth(config=reconciliation_config, now=now)


def _refresh_truth_stack(
    *, config: TrackBUnattendedLifecycleMaintenanceConfig, now: datetime
) -> dict[str, dict[str, Any]]:
    reconciliation = _refresh_reconciliation(config=config, now=now)
    order_truth_config = TrackBOpenOrderTruthConfig(repo_root=config.repo_root)
    open_order_truth = build_track_b_open_order_truth(config=order_truth_config, now=now)
    write_track_b_open_order_truth(config=order_truth_config, payload=open_order_truth)

    position_config = TrackBPositionTruthMonitorConfig(repo_root=config.repo_root)
    position_truth = build_track_b_position_truth(config=position_config, now=now)
    write_track_b_position_truth(config=position_config, payload=position_truth)

    managed_position_config = TrackBManagedPositionRegistryConfig(repo_root=config.repo_root)
    managed_positions = build_track_b_managed_position_registry(config=managed_position_config, now=now)
    write_track_b_managed_position_registry(config=managed_position_config, payload=managed_positions)

    managed_order_config = TrackBManagedOrderRegistryConfig(repo_root=config.repo_root)
    managed_orders = build_track_b_managed_order_registry(config=managed_order_config, now=now)
    write_track_b_managed_order_registry(config=managed_order_config, payload=managed_orders)

    position_truth = build_track_b_position_truth(config=position_config, now=now)
    write_track_b_position_truth(config=position_config, payload=position_truth)
    managed_positions = build_track_b_managed_position_registry(config=managed_position_config, now=now)
    write_track_b_managed_position_registry(config=managed_position_config, payload=managed_positions)

    guardian_config = TrackBBrokerPositionGuardianConfig(repo_root=config.repo_root)
    guardian = build_track_b_broker_position_guardian(config=guardian_config, now=now)
    write_track_b_broker_position_guardian(config=guardian_config, payload=guardian)
    return {
        "reconciliation": reconciliation,
        "position_truth": position_truth,
        "open_order_truth": open_order_truth,
        "managed_positions": managed_positions,
        "managed_orders": managed_orders,
        "guardian": guardian,
    }


def _run_pending_adoptions(
    *, config: TrackBUnattendedLifecycleMaintenanceConfig, reconciliation: Mapping[str, Any]
) -> list[dict[str, Any]]:
    if not config.local_adoption_enabled:
        return []
    records = _adoption_records(reconciliation)
    results: list[dict[str, Any]] = []
    for record in records:
        adoption = record.get("broker_backed_entry_adoption") if isinstance(record.get("broker_backed_entry_adoption"), Mapping) else record
        contract = adoption.get("contract") if isinstance(adoption.get("contract"), Mapping) else {}
        try:
            result = run_track_b_paper_lifecycle_adoption(
                config=LifecycleAdoptionConfig(
                    repo_root=config.repo_root,
                    lane_id=str(adoption.get("lane_id") or _lane_id_from_ownership(adoption) or ""),
                    order_intent_id=str(adoption.get("order_intent_id") or "") or None,
                    account_id=str(adoption.get("account") or adoption.get("account_id") or config.account_id),
                    symbol=str(contract.get("symbol") or adoption.get("symbol") or ""),
                    local_symbol=str(contract.get("local_symbol") or adoption.get("local_symbol") or ""),
                    expiry=str(contract.get("expiry") or adoption.get("expiry") or ""),
                    quantity=_decimal(adoption.get("qty") or adoption.get("quantity") or "1") or Decimal("1"),
                    apply=True,
                    allow_leak_test_synthetic_intent=True,
                    expected_broker_order_id=_string_or_none(adoption.get("broker_order_id")),
                    expected_client_id=_int_or_none(adoption.get("client_id")),
                    expected_perm_id=_int_or_none(adoption.get("perm_id")),
                    bridge_root=Path("outputs") / "reports" / "track_b_paper_leak_test",
                )
            )
            results.append(
                {
                    "classification": result.classification,
                    "audit_path": str(result.audit_path),
                    "broker_mutated_by_adoption": False,
                }
            )
        except Exception as exc:  # pragma: no cover - defensive audit boundary
            results.append({"classification": "UNATTENDED_ADOPTION_FAILED", "error": str(exc)})
    return results


def _run_due_exit_plans(
    *,
    config: TrackBUnattendedLifecycleMaintenanceConfig,
    managed_positions: Mapping[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    plans: list[dict[str, Any]] = []
    for position in managed_positions.get("managed_positions", []) or []:
        if not isinstance(position, Mapping):
            continue
        lifecycle_position = position.get("lifecycle_position") if isinstance(position.get("lifecycle_position"), Mapping) else {}
        exit_due = position.get("exit_due") is True
        if not exit_due:
            plans.append(
                {
                    "lifecycle_id": position.get("lifecycle_id"),
                    "classification": "MANAGED_EXIT_NOT_DUE_ACTIVE_HOLD",
                    "exit_due": False,
                    "managed_exit_policy_id": position.get("managed_exit_policy_id"),
                }
            )
            continue
        plan = run_track_b_managed_exit_attach(
            config=TrackBManagedExitAttachConfig(
                repo_root=config.repo_root,
                account_id=str(position.get("account_id") or config.account_id),
                expected_account_id=config.account_id,
                strategy_id=str(position.get("strategy_id") or lifecycle_position.get("strategy_id") or ""),
                lane_id=str(position.get("lane_id") or _lane_id_from_strategy(position.get("strategy_id"))),
                lifecycle_id=str(position.get("lifecycle_id") or ""),
                instrument_family=str(position.get("symbol") or lifecycle_position.get("instrument_family") or ""),
                contract_key=str(position.get("contract_key") or lifecycle_position.get("contract_key") or ""),
                local_symbol=str(position.get("local_symbol") or lifecycle_position.get("local_symbol") or ""),
                con_id=_int_or_none(position.get("con_id") or lifecycle_position.get("con_id")) or 0,
                expiry=str((position.get("broker_position") or {}).get("expiry") or ""),
                side=str(position.get("side") or lifecycle_position.get("side") or ""),
                quantity=int(_decimal(position.get("quantity") or "1") or Decimal("1")),
                apply=config.exit_apply_enabled,
                operator_authorized_managed_exit=config.operator_authorized_managed_exit,
                refresh_control_plane=True,
            ),
            now=now,
        )
        plans.append(
            {
                "lifecycle_id": position.get("lifecycle_id"),
                "classification": plan.get("classification"),
                "plan_classification": plan.get("plan_classification"),
                "apply_boundary_classification": plan.get("apply_boundary_classification"),
                "broker_state_mutated": plan.get("broker_state_mutated") is True,
                "submit_attempted": plan.get("submit_attempted") is True,
                "target_identity": plan.get("target_identity"),
            }
        )
    return plans


def _refresh_order_adjustment_plan(
    *, config: TrackBUnattendedLifecycleMaintenanceConfig, now: datetime
) -> dict[str, Any]:
    planner_config = TrackBOrderAdjustmentPlannerConfig(repo_root=config.repo_root)
    payload = build_track_b_order_adjustment_plan(config=planner_config, now=now)
    write_track_b_order_adjustment_plan(config=planner_config, payload=payload)
    return payload


def _run_working_close_order_management(
    *,
    config: TrackBUnattendedLifecycleMaintenanceConfig,
    managed_orders: Mapping[str, Any],
    order_adjustment_plan: Mapping[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    plans_by_order = {
        str(plan.get("broker_order_id") or ""): plan
        for plan in order_adjustment_plan.get("plans", []) or []
        if isinstance(plan, Mapping)
    }
    for order in managed_orders.get("managed_orders", []) or []:
        if not isinstance(order, Mapping):
            continue
        if str(order.get("classification") or "") not in {
            "WORKING_CLOSE_ORDER",
            "CLOSE_ORDER_MODIFIABLE",
            "CLOSE_ORDER_CANCEL_REPLACE_REQUIRED",
        }:
            continue
        broker_order_id = str(order.get("broker_order_id") or "")
        plan = plans_by_order.get(broker_order_id)
        if not plan:
            results.append(
                {
                    "broker_order_id": broker_order_id,
                    "lifecycle_id": order.get("lifecycle_id"),
                    "classification": "WORKING_CLOSE_ORDER_MANAGEMENT_NO_PLAN",
                    "detail": "No current order-adjustment plan matched the known managed close order.",
                }
            )
            continue
        if str(plan.get("classification") or "") != MODIFY_IN_PLACE_ELIGIBLE:
            results.append(
                {
                    "broker_order_id": broker_order_id,
                    "perm_id": order.get("perm_id"),
                    "lifecycle_id": order.get("lifecycle_id"),
                    "classification": "WORKING_CLOSE_ORDER_WAIT",
                    "order_adjustment_classification": plan.get("classification"),
                    "recommended_operator_action": plan.get("recommended_operator_action"),
                    "detail": plan.get("rationale") or "Working close order does not require modify-in-place.",
                }
            )
            continue
        modify_config = _modify_config_from_order_plan(config=config, order=order, plan=plan)
        if modify_config is None:
            results.append(
                {
                    "broker_order_id": broker_order_id,
                    "perm_id": order.get("perm_id"),
                    "lifecycle_id": order.get("lifecycle_id"),
                    "classification": "WORKING_CLOSE_ORDER_MODIFY_BLOCKED_INCOMPLETE_IDENTITY",
                    "order_adjustment_classification": plan.get("classification"),
                    "detail": "Modify-in-place was eligible, but current/new limit or exact order identity was incomplete.",
                }
            )
            continue
        adapter: IbkrPaperManagedOrderModifyAdapter | None = None
        hooks: dict[str, Any] = {}
        if config.order_modify_apply_enabled and config.operator_authorized_modify:
            adapter = IbkrPaperManagedOrderModifyAdapter(config=modify_config)
            hooks = {
                "pre_modify_open_order_refresh": adapter.refresh_open_orders,
                "modify_order_limit": adapter.modify_order_limit,
                "post_modify_open_order_refresh": adapter.refresh_open_orders,
            }
        try:
            report = run_track_b_managed_order_modify_in_place(config=modify_config, now=now, **hooks)
        finally:
            if adapter is not None:
                adapter.disconnect()
        results.append(
            {
                "broker_order_id": broker_order_id,
                "perm_id": order.get("perm_id"),
                "lifecycle_id": order.get("lifecycle_id"),
                "classification": report.get("classification"),
                "order_adjustment_classification": plan.get("classification"),
                "current_known_limit": modify_config.current_known_limit,
                "new_limit": modify_config.new_limit,
                "apply": modify_config.apply,
                "operator_authorized_modify": modify_config.operator_authorized_modify,
                "broker_mutation_attempted": report.get("broker_mutation_attempted") is True,
                "broker_mutation_performed": report.get("broker_mutation_performed") is True,
                "new_order_created": report.get("new_order_created") is True,
                "artifact_path": report.get("artifact_path"),
                "detail": report.get("detail"),
            }
        )
    return results


def _run_broker_flat_close_cleanup(
    *,
    config: TrackBUnattendedLifecycleMaintenanceConfig,
    managed_orders: Mapping[str, Any],
    order_adjustment_plan: Mapping[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    if not config.broker_flat_close_cleanup_enabled:
        return []
    results: list[dict[str, Any]] = []
    order_rows = {
        str(order.get("broker_order_id") or ""): order
        for order in managed_orders.get("managed_orders", []) or []
        if isinstance(order, Mapping)
    }
    for plan in order_adjustment_plan.get("plans", []) or []:
        if not isinstance(plan, Mapping):
            continue
        if str(plan.get("classification") or "") != BROKER_FLAT_NO_REPLACE:
            continue
        broker_order_id = str(plan.get("broker_order_id") or "")
        order = order_rows.get(broker_order_id, {})
        lifecycle_id = str(plan.get("lifecycle_id") or order.get("lifecycle_id") or "")
        if not broker_order_id or not lifecycle_id:
            results.append(
                {
                    "broker_order_id": broker_order_id,
                    "lifecycle_id": lifecycle_id,
                    "classification": "BROKER_FLAT_CLOSE_CLEANUP_BLOCKED_INCOMPLETE_IDENTITY",
                    "detail": "Broker-flat close cleanup requires an exact lifecycle id and broker order id.",
                }
            )
            continue
        resolution = resolve_known_managed_exit_order_disappearance(
            config=ManagedExitOrderResolutionConfig(
                repo_root=config.repo_root,
                lifecycle_id=lifecycle_id,
                broker_order_id=broker_order_id,
                client_id=_int_or_none(plan.get("client_id") or _mapping(plan.get("source_order")).get("client_id")),
                perm_id=_int_or_none(plan.get("perm_id") or _mapping(plan.get("identity")).get("perm_id")),
                account_id=config.account_id,
                symbol=_string_or_none(plan.get("symbol")),
                local_symbol=_string_or_none(plan.get("contract") or order.get("contract") or order.get("local_symbol")),
                con_id=_int_or_none(plan.get("con_id") or order.get("con_id")),
                apply=True,
            ),
            now=now,
        )
        results.append(
            {
                "broker_order_id": broker_order_id,
                "perm_id": plan.get("perm_id") or _mapping(plan.get("identity")).get("perm_id"),
                "lifecycle_id": lifecycle_id,
                "classification": resolution.get("classification"),
                "detail": resolution.get("detail"),
                "lifecycle_close": resolution.get("lifecycle_close"),
                "artifact_path": resolution.get("artifact_path"),
                "broker_mutation_attempted": False,
            }
        )
    return results


def _run_lifecycle_without_broker_cleanup(
    *,
    config: TrackBUnattendedLifecycleMaintenanceConfig,
    managed_positions: Mapping[str, Any],
    now: datetime,
) -> list[dict[str, Any]]:
    if not config.broker_flat_close_cleanup_enabled:
        return []
    results: list[dict[str, Any]] = []
    attach = _read_json(
        config.repo_root
        / "outputs"
        / "track_b_execution_core"
        / "managed_exit_attach"
        / "latest_managed_exit_attach_plan.json"
    )
    for position in managed_positions.get("managed_positions", []) or []:
        if not isinstance(position, Mapping):
            continue
        if str(position.get("classification") or "") != "LIFECYCLE_WITHOUT_BROKER":
            continue
        lifecycle_id = str(position.get("lifecycle_id") or "")
        known = _known_close_from_attach_for_lifecycle(attach=attach, lifecycle_id=lifecycle_id)
        if not known:
            results.append(
                {
                    "lifecycle_id": lifecycle_id,
                    "classification": "LIFECYCLE_WITHOUT_BROKER_CLEANUP_BLOCKED_NO_KNOWN_CLOSE_ORDER",
                    "detail": "Broker is flat but no exact managed close-order artifact was found for this lifecycle.",
                    "broker_mutation_attempted": False,
                }
            )
            continue
        resolution = resolve_known_managed_exit_order_disappearance(
            config=ManagedExitOrderResolutionConfig(
                repo_root=config.repo_root,
                lifecycle_id=lifecycle_id,
                broker_order_id=str(known.get("broker_order_id") or ""),
                client_id=_int_or_none(known.get("client_id")),
                perm_id=_int_or_none(known.get("perm_id")),
                account_id=config.account_id,
                symbol=_string_or_none(position.get("symbol") or known.get("symbol")),
                local_symbol=_string_or_none(position.get("local_symbol") or known.get("local_symbol")),
                con_id=_int_or_none(position.get("con_id") or known.get("con_id")),
                apply=True,
            ),
            now=now,
        )
        results.append(
            {
                "lifecycle_id": lifecycle_id,
                "broker_order_id": known.get("broker_order_id"),
                "perm_id": known.get("perm_id"),
                "classification": resolution.get("classification"),
                "detail": resolution.get("detail"),
                "lifecycle_close": resolution.get("lifecycle_close"),
                "artifact_path": resolution.get("artifact_path"),
                "broker_mutation_attempted": False,
            }
        )
    return results


def _run_readiness_recovery_handoff(
    *,
    config: TrackBUnattendedLifecycleMaintenanceConfig,
    truth_final: Mapping[str, Mapping[str, Any]],
    classification: str,
    now: datetime,
) -> dict[str, Any]:
    if not config.readiness_recovery_handoff_enabled:
        return {
            "classification": "READINESS_RECOVERY_HANDOFF_DISABLED",
            "detail": "Readiness refresh handoff disabled by configuration.",
            "refresh_attempted": False,
        }
    if not _truth_ready_for_readiness_recovery(truth_final=truth_final, classification=classification):
        return {
            "classification": "READINESS_RECOVERY_HANDOFF_SKIPPED_TRUTH_NOT_CLEAN",
            "detail": "Lifecycle maintenance has not reached flat/reconciled or intentional active-hold truth.",
            "refresh_attempted": False,
            "truth": {
                "reconciliation": truth_final["reconciliation"].get("classification"),
                "guardian": truth_final["guardian"].get("classification"),
                "position_truth": truth_final["position_truth"].get("classification")
                or (truth_final["position_truth"].get("summary") or {}).get("overall_classification"),
                "open_order_truth": truth_final["open_order_truth"].get("classification"),
                "managed_positions": truth_final["managed_positions"].get("classification"),
                "managed_orders": truth_final["managed_orders"].get("classification"),
            },
        }

    symbols = tuple(item.strip().upper() for item in str(config.symbols).split(",") if item.strip())
    proof_config = TrackBPaperProofReadinessConfig(
        repo_root=config.repo_root,
        required_symbols=symbols or TrackBPaperProofReadinessConfig.required_symbols,
        now=now,
        broker_lease_history_path=None,
    )
    proof = build_track_b_paper_proof_readiness(config=proof_config, now=now)
    proof_path = write_track_b_paper_proof_readiness(config=proof_config, payload=proof)

    control_plane_config = TrackBControlPlaneSnapshotConfig(
        repo_root=config.repo_root,
        broker_lease_history_path=None,
        refresh_proof_readiness_before_snapshot=True,
    )
    control_plane = build_track_b_control_plane_snapshot(config=control_plane_config, now=now)
    control_plane_path = write_track_b_control_plane_snapshot(config=control_plane_config, payload=control_plane)

    proof_class = str(proof.get("classification") or "")
    control_plane_class = str(control_plane.get("classification") or "")
    if proof_class == READY_FOR_PROOF and control_plane_class == CONTROL_PLANE_SNAPSHOT_READY:
        handoff_class = "READINESS_RECOVERY_HANDOFF_CONTROL_PLANE_READY"
    elif proof_class != READY_FOR_PROOF:
        handoff_class = "READINESS_RECOVERY_HANDOFF_BLOCKED_PROOF_READINESS"
    else:
        handoff_class = "READINESS_RECOVERY_HANDOFF_BLOCKED_CONTROL_PLANE"

    return {
        "classification": handoff_class,
        "refresh_attempted": True,
        "proof_readiness_refreshed": True,
        "control_plane_refreshed": True,
        "proof_readiness_classification": proof_class,
        "proof_readiness_primary_blocker": proof.get("primary_blocker"),
        "control_plane_classification": control_plane_class,
        "shared_truth_coherence_status": control_plane.get("shared_truth_coherence_status"),
        "safe_state_classification": control_plane.get("safe_state_classification"),
        "runtime_supervisor_classification": control_plane.get("runtime_supervisor_classification"),
        "proof_window_status": control_plane.get("proof_window_status"),
        "proof_readiness_path": str(proof_path),
        "control_plane_path": str(control_plane_path),
        "broker_mutation_attempted": False,
        "runtime_restart_attempted": False,
        "dashboard_projection_consumed": False,
    }


def _truth_ready_for_readiness_recovery(
    *,
    truth_final: Mapping[str, Mapping[str, Any]],
    classification: str,
) -> bool:
    if classification == UNATTENDED_LIFECYCLE_MAINTENANCE_BLOCKED_AMBIGUOUS:
        return False
    guardian = str(truth_final["guardian"].get("classification") or "")
    reconciliation = str(truth_final["reconciliation"].get("classification") or "")
    position_truth = str(
        truth_final["position_truth"].get("classification")
        or (truth_final["position_truth"].get("summary") or {}).get("overall_classification")
        or ""
    )
    open_order_truth = str(truth_final["open_order_truth"].get("classification") or "")
    managed_positions = str(truth_final["managed_positions"].get("classification") or "")
    managed_orders = str(truth_final["managed_orders"].get("classification") or "")
    clean_flat = (
        guardian == "BROKER_POSITION_GUARDIAN_READY"
        and reconciliation == "TRACK_B_PAPER_BROKER_RECONCILED"
        and position_truth == "CLEAN_FLAT_READY"
        and open_order_truth == "NO_OPEN_ORDERS"
        and managed_positions == "NO_MANAGED_POSITIONS"
        and managed_orders == "NO_MANAGED_ORDERS"
    )
    active_hold = (
        guardian == "BROKER_POSITION_GUARDIAN_READY"
        and reconciliation == "TRACK_B_PAPER_BROKER_RECONCILED"
        and position_truth == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
        and open_order_truth == "BROKER_POSITION_WITHOUT_CLOSE_ORDER"
        and managed_positions == "OPEN_MANAGED_MATCHED"
        and managed_orders == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
    )
    return clean_flat or active_hold


def _known_close_from_attach_for_lifecycle(*, attach: Mapping[str, Any], lifecycle_id: str) -> dict[str, Any] | None:
    if not attach or not lifecycle_id:
        return None
    apply_result = attach.get("apply_result") if isinstance(attach.get("apply_result"), Mapping) else {}
    close_submit = apply_result.get("close_submit_attempt") if isinstance(apply_result.get("close_submit_attempt"), Mapping) else {}
    close_intent = apply_result.get("close_intent") if isinstance(apply_result.get("close_intent"), Mapping) else {}
    preview = attach.get("close_intent_preview") if isinstance(attach.get("close_intent_preview"), Mapping) else {}
    attach_lifecycle = str(attach.get("lifecycle_id") or close_intent.get("lifecycle_id") or preview.get("lifecycle_id") or "")
    aggregate_group = attach.get("aggregate_exit_group") if isinstance(attach.get("aggregate_exit_group"), Mapping) else {}
    aggregate_lifecycle_ids = {str(item) for item in aggregate_group.get("lifecycle_ids") or []}
    if attach_lifecycle != lifecycle_id and lifecycle_id not in aggregate_lifecycle_ids:
        return None
    broker_order_id = _string_or_none(close_submit.get("broker_order_id"))
    if not broker_order_id:
        return None
    return {
        "broker_order_id": broker_order_id,
        "client_id": close_submit.get("client_id"),
        "perm_id": close_submit.get("perm_id"),
        "symbol": attach.get("symbol") or close_intent.get("symbol") or preview.get("symbol"),
        "local_symbol": attach.get("local_symbol") or close_intent.get("local_symbol") or preview.get("local_symbol"),
        "con_id": attach.get("con_id") or close_intent.get("con_id") or preview.get("con_id"),
    }


def _modify_config_from_order_plan(
    *,
    config: TrackBUnattendedLifecycleMaintenanceConfig,
    order: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> ManagedOrderModifyInPlaceConfig | None:
    identity = plan.get("identity") if isinstance(plan.get("identity"), Mapping) else {}
    source_order = plan.get("source_order") if isinstance(plan.get("source_order"), Mapping) else {}
    broker_order_id = _string_or_none(order.get("broker_order_id") or plan.get("broker_order_id") or identity.get("broker_order_id"))
    perm_id = _string_or_none(order.get("perm_id") or plan.get("perm_id") or identity.get("perm_id"))
    action = _string_or_none(order.get("action") or plan.get("action") or identity.get("action"))
    quantity = _string_or_none(order.get("quantity") or plan.get("quantity") or identity.get("quantity"))
    contract = _string_or_none(order.get("contract") or order.get("local_symbol") or plan.get("contract") or identity.get("contract"))
    symbol = _string_or_none(order.get("symbol") or plan.get("symbol") or source_order.get("symbol"))
    account_id = _string_or_none(
        order.get("account_id") or identity.get("account_id") or source_order.get("account_id") or config.account_id
    )
    current_limit = _current_known_limit_for_working_close(config=config, order=order, plan=plan)
    new_limit = _marketable_limit_for_working_close(order=order, plan=plan)
    if not all([broker_order_id, perm_id, action, quantity, contract, symbol, account_id, current_limit, new_limit]):
        return None
    return ManagedOrderModifyInPlaceConfig(
        repo_root=config.repo_root,
        broker_order_id=str(broker_order_id),
        perm_id=str(perm_id),
        symbol=str(symbol).upper(),
        contract=str(contract).upper(),
        con_id=_string_or_none(order.get("con_id") or plan.get("con_id") or identity.get("con_id")),
        action=str(action).upper(),
        quantity=str(_decimal(quantity) or quantity),
        current_known_limit=str(current_limit),
        new_limit=str(new_limit),
        account_id=str(account_id),
        apply=config.order_modify_apply_enabled,
        operator_authorized_modify=config.operator_authorized_modify,
    )


def _current_known_limit_for_working_close(
    *,
    config: TrackBUnattendedLifecycleMaintenanceConfig,
    order: Mapping[str, Any],
    plan: Mapping[str, Any],
) -> Decimal | None:
    direct = _decimal(order.get("limit_price") or plan.get("limit_price"))
    if direct is not None:
        return direct
    attach = _read_json(
        config.repo_root
        / "outputs"
        / "track_b_execution_core"
        / "managed_exit_attach"
        / "latest_managed_exit_attach_plan.json"
    )
    if not attach:
        return None
    apply_result = attach.get("apply_result") if isinstance(attach.get("apply_result"), Mapping) else {}
    close_submit = apply_result.get("close_submit_attempt") if isinstance(apply_result.get("close_submit_attempt"), Mapping) else {}
    preview = attach.get("close_intent_preview") if isinstance(attach.get("close_intent_preview"), Mapping) else {}
    close_intent = apply_result.get("close_intent") if isinstance(apply_result.get("close_intent"), Mapping) else {}
    if str(close_submit.get("broker_order_id") or "") != str(order.get("broker_order_id") or plan.get("broker_order_id") or ""):
        return None
    if str((preview or close_intent).get("lifecycle_id") or close_intent.get("lifecycle_id") or "") != str(
        order.get("lifecycle_id") or plan.get("lifecycle_id") or ""
    ):
        return None
    return _decimal(close_intent.get("close_limit_price") or preview.get("close_limit_price"))


def _marketable_limit_for_working_close(*, order: Mapping[str, Any], plan: Mapping[str, Any]) -> Decimal | None:
    policy = plan.get("managed_close_reprice_policy") if isinstance(plan.get("managed_close_reprice_policy"), Mapping) else {}
    policy_limit = _decimal(policy.get("limit_price"))
    if policy_limit is not None and str(policy.get("classification") or "") == "MANAGED_CLOSE_PRICED":
        return policy_limit
    reference = plan.get("market_reference") if isinstance(plan.get("market_reference"), Mapping) else {}
    reference_price = _decimal(reference.get("reference_price"))
    if reference_price is None:
        marketability = order.get("marketability") if isinstance(order.get("marketability"), Mapping) else {}
        market_reference = (
            marketability.get("market_reference") if isinstance(marketability.get("market_reference"), Mapping) else {}
        )
        reference_price = _decimal(market_reference.get("reference_price"))
    if reference_price is None:
        return None
    action = str(order.get("action") or plan.get("action") or "").upper()
    symbol = str(order.get("symbol") or plan.get("symbol") or "").upper()
    tick = _tick_size(symbol)
    priced = managed_close_limit_from_reference(
        reference_price=reference_price,
        close_action=action,
        tick_size=str(tick),
        base_offset_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_OFFSET_TICKS,
        max_slippage_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_MAX_SLIPPAGE_TICKS,
        reprice_attempts=_int_or_default(
            order.get("reprice_attempt_count")
            or plan.get("reprice_attempt_count")
            or order.get("modify_attempt_count")
            or plan.get("modify_attempt_count"),
            0,
        ),
        reprice_escalation_ticks=ACTIVE_EVIDENCE_MANAGED_CLOSE_REPRICE_ESCALATION_TICKS,
        reference_age_seconds=_float_or_none(
            reference.get("reference_age_seconds")
            or reference.get("pricing_reference_age_seconds")
            or reference.get("age_seconds")
        ),
        stale_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_STALE_AFTER_SECONDS,
        widen_reference_seconds=ACTIVE_EVIDENCE_MANAGED_CLOSE_WIDEN_AFTER_SECONDS,
    )
    return _decimal(priced.get("limit_price"))


def _tick_size(symbol: str) -> Decimal:
    if symbol in {"MNQ", "MES", "NQ", "ES"}:
        return Decimal("0.25")
    return Decimal("0.1")


def _round_to_tick(value: Decimal, tick: Decimal) -> Decimal:
    rounding = ROUND_FLOOR if value >= 0 else ROUND_CEILING
    steps = (value / tick).to_integral_value(rounding=rounding)
    return steps * tick


def _int_or_default(value: Any, default: int) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _float_or_none(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _adoption_records(reconciliation: Mapping[str, Any]) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    direct = reconciliation.get("broker_backed_entry_adoption")
    if isinstance(direct, Mapping):
        records.append(dict(direct))
    for blocker in reconciliation.get("blockers", []) or []:
        if isinstance(blocker, Mapping) and isinstance(blocker.get("broker_backed_entry_adoption"), Mapping):
            records.append(dict(blocker))
    return records


def _classification(
    *,
    truth_final: Mapping[str, Mapping[str, Any]],
    exit_plans: Sequence[Mapping[str, Any]],
    working_close_order_management: Sequence[Mapping[str, Any]],
    broker_flat_close_cleanup: Sequence[Mapping[str, Any]],
    lifecycle_without_broker_cleanup: Sequence[Mapping[str, Any]],
) -> str:
    guardian = str(truth_final["guardian"].get("classification") or "")
    managed_positions = str(truth_final["managed_positions"].get("classification") or "")
    if guardian.endswith("HARD_HOLD"):
        return UNATTENDED_LIFECYCLE_MAINTENANCE_BLOCKED_AMBIGUOUS
    if any(item.get("lifecycle_close") for item in broker_flat_close_cleanup) or any(
        item.get("lifecycle_close") for item in lifecycle_without_broker_cleanup
    ):
        return UNATTENDED_LIFECYCLE_MAINTENANCE_READY
    if any(item.get("broker_mutation_performed") is True for item in working_close_order_management):
        return UNATTENDED_LIFECYCLE_MAINTENANCE_CLOSE_ORDER_MANAGED
    if any(str(plan.get("classification") or "").startswith("MANAGED_EXIT_") for plan in exit_plans):
        if any(plan.get("broker_state_mutated") is True for plan in exit_plans):
            return UNATTENDED_LIFECYCLE_MAINTENANCE_READY
        if any(str(plan.get("classification") or "") not in {"MANAGED_EXIT_NOT_DUE_ACTIVE_HOLD"} for plan in exit_plans):
            return UNATTENDED_LIFECYCLE_MAINTENANCE_EXIT_DUE
    if any(
        str(item.get("classification") or "") in {"MODIFY_IN_PLACE_DRY_RUN_READY", "MODIFY_IN_PLACE_APPLIED"}
        for item in working_close_order_management
    ):
        return UNATTENDED_LIFECYCLE_MAINTENANCE_CLOSE_ORDER_MANAGED
    if managed_positions in {"OPEN_MANAGED_MATCHED", "OPEN_MANAGED_CLOSE_WORKING"}:
        return UNATTENDED_LIFECYCLE_MAINTENANCE_ACTIVE_HOLD
    return UNATTENDED_LIFECYCLE_MAINTENANCE_READY


def _blocked_reason(*, truth_final: Mapping[str, Mapping[str, Any]]) -> str | None:
    guardian = str(truth_final["guardian"].get("classification") or "")
    if guardian.endswith("HARD_HOLD"):
        return str(truth_final["guardian"].get("operator_explanation") or "guardian hard hold")
    return None


def _lane_id_from_ownership(adoption: Mapping[str, Any]) -> str | None:
    value = adoption.get("lane_id")
    return str(value) if value else None


def _lane_id_from_strategy(strategy_id: object) -> str:
    value = str(strategy_id or "")
    return value.split("__", 1)[-1] if "__" in value else value.lower()


def _decimal(value: object) -> Decimal | None:
    if value in {None, ""}:
        return None
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return None


def _int_or_none(value: object) -> int | None:
    if value in {None, ""}:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _string_or_none(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _mapping(value: object) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Run central Track B unattended lifecycle maintenance.")
    parser.add_argument("--repo-root", type=Path, default=REPO_ROOT)
    parser.add_argument("--output-path", type=Path, default=DEFAULT_OUTPUT_PATH)
    parser.add_argument("--symbols", default="MGC,MNQ,GC,PL")
    parser.add_argument("--no-local-adoption", action="store_true")
    parser.add_argument("--apply-exits", action="store_true")
    parser.add_argument("--operator-authorized-managed-exit", action="store_true")
    parser.add_argument("--apply-order-modifies", action="store_true")
    parser.add_argument("--operator-authorized-modify", action="store_true")
    parser.add_argument("--no-broker-flat-close-cleanup", action="store_true")
    parser.add_argument("--no-readiness-recovery-handoff", action="store_true")
    parser.add_argument("--json", action="store_true")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    report = run_unattended_lifecycle_maintenance(
        config=TrackBUnattendedLifecycleMaintenanceConfig(
            repo_root=args.repo_root.expanduser().resolve(),
            output_path=args.output_path,
            symbols=str(args.symbols),
            local_adoption_enabled=not args.no_local_adoption,
            exit_apply_enabled=bool(args.apply_exits),
            operator_authorized_managed_exit=bool(args.operator_authorized_managed_exit),
            order_modify_apply_enabled=bool(args.apply_order_modifies),
            operator_authorized_modify=bool(args.operator_authorized_modify),
            broker_flat_close_cleanup_enabled=not args.no_broker_flat_close_cleanup,
            readiness_recovery_handoff_enabled=not args.no_readiness_recovery_handoff,
        )
    )
    if args.json:
        print(json.dumps(report, indent=2, sort_keys=True))
    else:
        print(json.dumps({"classification": report["classification"], "output_path": report["output_path"]}))
    return 0 if report["classification"] != UNATTENDED_LIFECYCLE_MAINTENANCE_BLOCKED_AMBIGUOUS else 2


if __name__ == "__main__":
    raise SystemExit(main())
