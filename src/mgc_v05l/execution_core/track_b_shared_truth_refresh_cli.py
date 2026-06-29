"""Read-only Track B shared-truth refresh CLI.

This command refreshes execution_core authority artifacts only. Dashboard
artifacts are never consumed as authority and dashboard projections are not
written by this shared refresh path.
"""

from __future__ import annotations

import argparse
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from .track_b_atomic_io import write_json_atomic
from .track_b_broker_truth_lease import (
    DEFAULT_LEASE_ARTIFACT,
    DEFAULT_LEASE_HISTORY,
    classify_broker_truth_lease,
    preserve_invalidated_previous_lease_diagnostic,
)
from .track_b_broker_position_guardian import (
    BROKER_POSITION_GUARDIAN_READY,
    DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT,
    TrackBBrokerPositionGuardianConfig,
    build_track_b_broker_position_guardian,
    write_track_b_broker_position_guardian,
)
from .track_b_managed_order_registry import (
    ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING,
    NO_MANAGED_ORDERS,
    TrackBManagedOrderRegistryConfig,
    build_track_b_managed_order_registry,
    write_track_b_managed_order_registry,
)
from .track_b_managed_position_registry import (
    NO_MANAGED_POSITIONS,
    TrackBManagedPositionRegistryConfig,
    build_track_b_managed_position_registry,
    write_track_b_managed_position_registry,
)
from .track_b_open_order_truth import (
    BROKER_POSITION_WITHOUT_CLOSE_ORDER,
    DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT,
    NO_OPEN_ORDERS,
    TrackBOpenOrderTruthConfig,
    build_track_b_open_order_truth,
    write_track_b_open_order_truth,
)
from .track_b_paper_broker_reconciliation import (
    PAPER_ACCOUNT,
    PHASE1_RUNTIME_TICKER_ORDER,
)
from .track_b_position_truth_monitor import TrackBPositionTruthMonitorConfig, build_track_b_position_truth, write_track_b_position_truth
from .track_b_runtime_environment_truth import (
    RUNTIME_ACTIVE_OBSERVATION_ONLY,
    RUNTIME_ACTIVE_TRADE_CAPABLE,
    RUNTIME_DOWN_CLEAN,
    TrackBRuntimeEnvironmentTruthConfig,
    build_track_b_runtime_environment_truth,
    write_track_b_runtime_environment_truth,
)


DEFAULT_BROKER_TRUTH_STATUS = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"
)
DEFAULT_BROKER_TRUTH_LATEST_ATTEMPT = (
    Path("outputs") / "reports" / "ibkr_read_only_verification" / "ibkr_broker_truth_latest_attempt_status.json"
)
DEFAULT_LIVE_POSITION_STATUS = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json"
)
DEFAULT_TRADE_SUMMARY = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_RECONCILIATION_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "track_b_paper_broker_reconciliation"
    / "latest_track_b_paper_broker_reconciliation.json"
)
DEFAULT_SHARED_TRUTH_REFRESH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "shared_truth" / "latest_track_b_shared_truth_refresh.json"
)
DEFAULT_REGISTRY_DIAGNOSTICS_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "diagnostics" / "latest_track_b_registry_truth_diagnostics.json"
)
TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE = "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE"
RUNTIME_START_REQUIRED_CLASSIFICATIONS = {
    "Open Order Truth": {NO_OPEN_ORDERS},
    "Managed Order Registry": {NO_MANAGED_ORDERS},
    "Position Truth": {"CLEAN_FLAT_READY"},
    "Runtime Environment Truth": {RUNTIME_DOWN_CLEAN},
    "Managed Position Registry": {NO_MANAGED_POSITIONS},
    "Reconciliation": {"BROKER_LIFECYCLE_RECONCILED", "TRACK_B_PAPER_BROKER_RECONCILED"},
    "Broker Truth Lease": {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"},
    "Broker Position Guardian": {BROKER_POSITION_GUARDIAN_READY},
}


@dataclass(frozen=True)
class TrackBSharedTruthRefreshConfig:
    repo_root: Path
    account: str = PAPER_ACCOUNT
    symbols: tuple[str, ...] = PHASE1_RUNTIME_TICKER_ORDER
    broker_truth_status_path: Path = DEFAULT_BROKER_TRUTH_STATUS
    broker_truth_latest_attempt_path: Path = DEFAULT_BROKER_TRUTH_LATEST_ATTEMPT
    live_position_status_path: Path = DEFAULT_LIVE_POSITION_STATUS
    trade_summary_path: Path = DEFAULT_TRADE_SUMMARY
    broker_lease_path: Path = DEFAULT_LEASE_ARTIFACT
    broker_lease_history_path: Path | None = DEFAULT_LEASE_HISTORY
    broker_position_guardian_path: Path = DEFAULT_BROKER_POSITION_GUARDIAN_ARTIFACT
    shared_truth_refresh_path: Path = DEFAULT_SHARED_TRUTH_REFRESH_ARTIFACT
    registry_diagnostics_path: Path = DEFAULT_REGISTRY_DIAGNOSTICS_ARTIFACT
    broker_lease_max_entry_age_seconds: float = 300.0
    broker_lease_max_exit_age_seconds: float = 900.0
    broker_lease_degraded_refresh_grace_seconds: float = 120.0

    def resolve(self, path: Path) -> Path:
        return path if path.is_absolute() else self.repo_root / path


def refresh_track_b_shared_truth(
    *,
    config: TrackBSharedTruthRefreshConfig,
    now: datetime | None = None,
    pid_running: Callable[[int], bool] | None = None,
    process_root_resolver: Callable[[int], Path | None] | None = None,
    source_commit_resolver: Callable[[Path], str | None] | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or datetime.now(UTC))
    refresh_generation_id = _refresh_generation_id(actual_now)
    reconciliation = _read_json(config.resolve(DEFAULT_RECONCILIATION_ARTIFACT))
    registry_diagnostics = _read_json(config.resolve(config.registry_diagnostics_path))
    fast_path = _clean_flat_fast_path_eligibility(
        reconciliation=reconciliation,
        registry_diagnostics=registry_diagnostics,
    )

    open_order_config = TrackBOpenOrderTruthConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    if fast_path["used"]:
        open_order_truth = _clean_flat_open_order_truth(
            now=actual_now,
            config=open_order_config,
            reconciliation=reconciliation,
            registry_diagnostics=registry_diagnostics,
            fast_path=fast_path,
        )
    else:
        open_order_truth = build_track_b_open_order_truth(config=open_order_config, now=actual_now)
    open_order_truth = _with_authority_cycle(
        open_order_truth,
        generation_id=refresh_generation_id,
        cycle_generated_at=actual_now,
    )
    open_order_path, _ = write_track_b_open_order_truth(config=open_order_config, payload=open_order_truth, now=actual_now)

    managed_order_config = TrackBManagedOrderRegistryConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    if fast_path["used"]:
        managed_order_registry = _clean_flat_managed_order_registry(
            now=actual_now,
            config=managed_order_config,
            open_order_truth=open_order_truth,
            reconciliation=reconciliation,
            registry_diagnostics=registry_diagnostics,
            fast_path=fast_path,
        )
    else:
        managed_order_registry = build_track_b_managed_order_registry(config=managed_order_config, now=actual_now)
    managed_order_registry = _with_authority_cycle(
        managed_order_registry,
        generation_id=refresh_generation_id,
        cycle_generated_at=actual_now,
        open_order_truth=open_order_truth,
    )
    managed_order_path, _ = write_track_b_managed_order_registry(
        config=managed_order_config,
        payload=managed_order_registry,
        now=actual_now,
    )

    position_config = TrackBPositionTruthMonitorConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    if fast_path["used"]:
        position_truth = _clean_flat_position_truth(
            now=actual_now,
            config=position_config,
            open_order_truth=open_order_truth,
            managed_order_registry=managed_order_registry,
            reconciliation=reconciliation,
            registry_diagnostics=registry_diagnostics,
            fast_path=fast_path,
        )
    else:
        position_truth = build_track_b_position_truth(config=position_config, now=actual_now)
    position_truth = _with_authority_cycle(
        position_truth,
        generation_id=refresh_generation_id,
        cycle_generated_at=actual_now,
        open_order_truth=open_order_truth,
        managed_order_registry=managed_order_registry,
    )
    position_path, _ = write_track_b_position_truth(config=position_config, payload=position_truth, now=actual_now)

    managed_position_config = TrackBManagedPositionRegistryConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    if fast_path["used"]:
        managed_position_registry = _clean_flat_managed_position_registry(
            now=actual_now,
            config=managed_position_config,
            open_order_truth=open_order_truth,
            managed_order_registry=managed_order_registry,
            position_truth=position_truth,
            reconciliation=reconciliation,
            registry_diagnostics=registry_diagnostics,
            fast_path=fast_path,
        )
    else:
        managed_position_registry = build_track_b_managed_position_registry(config=managed_position_config, now=actual_now)
    managed_position_registry = _with_authority_cycle(
        managed_position_registry,
        generation_id=refresh_generation_id,
        cycle_generated_at=actual_now,
        open_order_truth=open_order_truth,
        managed_order_registry=managed_order_registry,
        position_truth=position_truth,
    )
    managed_position_path, _ = write_track_b_managed_position_registry(
        config=managed_position_config,
        payload=managed_position_registry,
        now=actual_now,
    )

    # Managed Order Registry and Managed Position Registry are mutually
    # informative. Build each once from the other fresh current-cycle artifact,
    # then rebuild Position Truth from the final managed-order projection.
    if fast_path["used"]:
        managed_order_registry = _clean_flat_managed_order_registry(
            now=actual_now,
            config=managed_order_config,
            open_order_truth=open_order_truth,
            managed_position_registry=managed_position_registry,
            reconciliation=reconciliation,
            registry_diagnostics=registry_diagnostics,
            fast_path=fast_path,
        )
    else:
        managed_order_registry = build_track_b_managed_order_registry(config=managed_order_config, now=actual_now)
    managed_order_registry = _with_authority_cycle(
        managed_order_registry,
        generation_id=refresh_generation_id,
        cycle_generated_at=actual_now,
        open_order_truth=open_order_truth,
        managed_position_registry=managed_position_registry,
    )
    managed_order_path, _ = write_track_b_managed_order_registry(
        config=managed_order_config,
        payload=managed_order_registry,
        now=actual_now,
    )
    if fast_path["used"]:
        position_truth = _clean_flat_position_truth(
            now=actual_now,
            config=position_config,
            open_order_truth=open_order_truth,
            managed_order_registry=managed_order_registry,
            managed_position_registry=managed_position_registry,
            reconciliation=reconciliation,
            registry_diagnostics=registry_diagnostics,
            fast_path=fast_path,
        )
    else:
        position_truth = build_track_b_position_truth(config=position_config, now=actual_now)
    position_truth = _with_authority_cycle(
        position_truth,
        generation_id=refresh_generation_id,
        cycle_generated_at=actual_now,
        open_order_truth=open_order_truth,
        managed_order_registry=managed_order_registry,
        managed_position_registry=managed_position_registry,
    )
    position_path, _ = write_track_b_position_truth(config=position_config, payload=position_truth, now=actual_now)
    runtime_config = TrackBRuntimeEnvironmentTruthConfig(repo_root=config.repo_root, dashboard_projection_path=None)
    runtime_environment_truth = build_track_b_runtime_environment_truth(
        config=runtime_config,
        now=actual_now,
        pid_running=pid_running,
        process_root_resolver=process_root_resolver,
        source_commit_resolver=source_commit_resolver,
    )
    runtime_environment_truth = _with_authority_cycle(
        runtime_environment_truth,
        generation_id=refresh_generation_id,
        cycle_generated_at=actual_now,
        position_truth=position_truth,
    )
    runtime_path, _ = write_track_b_runtime_environment_truth(
        config=runtime_config,
        payload=runtime_environment_truth,
        now=actual_now,
    )

    broker_position_guardian_config = TrackBBrokerPositionGuardianConfig(
        repo_root=config.repo_root,
        output_path=config.broker_position_guardian_path,
    )
    broker_position_guardian = build_track_b_broker_position_guardian(
        config=broker_position_guardian_config,
        now=actual_now,
        input_overrides={
            "open_order_truth": open_order_truth,
            "managed_order_registry": managed_order_registry,
            "position_truth": position_truth,
            "managed_position_registry": managed_position_registry,
            "reconciliation": reconciliation,
        },
    )
    broker_position_guardian = _with_authority_cycle(
        broker_position_guardian,
        generation_id=refresh_generation_id,
        cycle_generated_at=actual_now,
        open_order_truth=open_order_truth,
        managed_order_registry=managed_order_registry,
        position_truth=position_truth,
        runtime_environment_truth=runtime_environment_truth,
        managed_position_registry=managed_position_registry,
        reconciliation=reconciliation,
    )
    broker_position_guardian_path = write_track_b_broker_position_guardian(
        config=broker_position_guardian_config,
        payload=broker_position_guardian,
    )
    broker_lease = _refresh_broker_lease(config=config, reconciliation=reconciliation, now=actual_now)
    recovery_budget_ledger, recovery_budget_ledger_path = _refresh_recovery_budget_ledger(
        repo_root=config.repo_root,
        now=actual_now,
    )
    paper_recovery_policy, paper_recovery_policy_path = _refresh_paper_recovery_policy(
        repo_root=config.repo_root,
        now=actual_now,
    )
    autonomous_recovery_plan, autonomous_recovery_plan_path = _refresh_autonomous_recovery_plan(
        repo_root=config.repo_root,
        now=actual_now,
    )
    open_order_truth = _read_json(open_order_path) or open_order_truth
    managed_order_registry = _read_json(managed_order_path) or managed_order_registry
    position_truth = _read_json(position_path) or position_truth
    runtime_environment_truth = _read_json(runtime_path) or runtime_environment_truth
    managed_position_registry = _read_json(managed_position_path) or managed_position_registry
    broker_position_guardian = _read_json(broker_position_guardian_path) or broker_position_guardian
    broker_lease = _read_json(config.resolve(config.broker_lease_path)) or broker_lease
    paper_recovery_policy = _read_json(paper_recovery_policy_path) or paper_recovery_policy
    autonomous_recovery_plan = _read_json(autonomous_recovery_plan_path) or autonomous_recovery_plan
    services = [
        _service_row("Open Order Truth", open_order_truth, open_order_path),
        _service_row("Managed Order Registry", managed_order_registry, managed_order_path),
        _service_row("Position Truth", position_truth, position_path, summary_key="overall_classification"),
        _service_row("Runtime Environment Truth", runtime_environment_truth, runtime_path),
        _service_row("Managed Position Registry", managed_position_registry, managed_position_path),
        _reconciliation_row(config=config, reconciliation=reconciliation),
        _broker_lease_row(config=config, broker_lease=broker_lease),
        _service_row("Broker Position Guardian", broker_position_guardian, broker_position_guardian_path),
        _recovery_budget_ledger_row(payload=recovery_budget_ledger, artifact_path=recovery_budget_ledger_path),
        _paper_recovery_policy_row(payload=paper_recovery_policy, artifact_path=paper_recovery_policy_path),
        _autonomous_recovery_plan_row(payload=autonomous_recovery_plan, artifact_path=autonomous_recovery_plan_path),
    ]
    warnings = _warnings(services=services, payloads={
        "open_order_truth": open_order_truth,
        "managed_order_registry": managed_order_registry,
        "position_truth": position_truth,
        "runtime_environment_truth": runtime_environment_truth,
        "managed_position_registry": managed_position_registry,
        "reconciliation": reconciliation,
        "broker_lease": broker_lease,
        "broker_position_guardian": broker_position_guardian,
        "recovery_budget_ledger": recovery_budget_ledger,
        "paper_recovery_policy": paper_recovery_policy,
        "autonomous_recovery_plan": autonomous_recovery_plan,
    })
    blockers = _unsafe_blockers(
        open_order_truth=open_order_truth,
        managed_order_registry=managed_order_registry,
        position_truth=position_truth,
        runtime_environment_truth=runtime_environment_truth,
        managed_position_registry=managed_position_registry,
        reconciliation=reconciliation,
        broker_lease=broker_lease,
        broker_position_guardian=broker_position_guardian,
    )
    exit_code = 2 if blockers else 0
    result = {
        "schema_version": "track_b_shared_truth_refresh_v1",
        "generated_at": actual_now.isoformat(),
        "refresh_generation_id": refresh_generation_id,
        "authority_generation_id": refresh_generation_id,
        "authority_cycle_generated_at": actual_now.isoformat(),
        "source_generation_references": _source_generation_references(
            open_order_truth=open_order_truth,
            managed_order_registry=managed_order_registry,
            position_truth=position_truth,
            runtime_environment_truth=runtime_environment_truth,
            managed_position_registry=managed_position_registry,
            reconciliation=reconciliation,
        ),
        "refresh_phase": "pre_supervisor_refresh",
        "canonical_refresh_scope": "GLOBAL_COMPLETE",
        "canonical_scope_blockers": [],
        "input_symbols": list(PHASE1_RUNTIME_TICKER_ORDER),
        "canonical_symbols": list(PHASE1_RUNTIME_TICKER_ORDER),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "services": services,
        "classifications": {str(row["service"]): row.get("classification") for row in services},
        "artifact_paths": {str(row["service"]): row.get("artifact_path") for row in services if row.get("artifact_path")},
        "source_refresh_artifact_path": str(config.resolve(config.shared_truth_refresh_path)),
        "active_authority": {
            "current_flat_authority_clean": _current_flat_truth_clean(
                classifications={str(row["service"]): row.get("classification") for row in services}
            ),
            "managed_position_registry": _managed_position_registry_active_authority(managed_position_registry),
            "broker_truth_lease": _top_level_current_truth_invalidation_state(broker_lease),
        },
        "bounded_current_scope_fast_path": fast_path,
        "recovery_budget_ledger": recovery_budget_ledger.get("classification"),
        "recovery_budget_exhausted": recovery_budget_ledger.get("budget_exhausted") is True,
        "paper_recovery_policy": paper_recovery_policy.get("paper_action_policy"),
        "autonomous_recovery_plan_classification": autonomous_recovery_plan.get("classification"),
        "autonomous_recovery_next_action": _autonomous_recovery_next_action(autonomous_recovery_plan),
        "autonomous_recovery_execution_enabled": autonomous_recovery_plan.get("execution_enabled") is True,
        "warnings": warnings,
        "unsafe_blockers": blockers,
        "exit_code": exit_code,
    }
    _write_json_atomic(config.resolve(config.shared_truth_refresh_path), result)
    return result


def build_runtime_start_preflight_summary(result: Mapping[str, Any]) -> dict[str, Any]:
    """Validate shared-truth authority classifications for a PAPER runtime start."""

    classifications = _mapping(result.get("classifications"))
    active_authority = _mapping(result.get("active_authority"))
    managed_position_registry = _mapping(active_authority.get("managed_position_registry"))
    current_flat_authority_clean = active_authority.get("current_flat_authority_clean") is True
    broker_truth_lease_authority = _mapping(active_authority.get("broker_truth_lease"))
    active_hold = _managed_active_hold_pending(
        open_order_class=str(classifications.get("Open Order Truth") or ""),
        managed_order_class=str(classifications.get("Managed Order Registry") or ""),
        position_class=str(classifications.get("Position Truth") or ""),
        managed_position_class=str(classifications.get("Managed Position Registry") or ""),
        reconciliation_class=str(classifications.get("Reconciliation") or ""),
    )
    active_exit_due = _managed_exit_due(
        open_order_class=str(classifications.get("Open Order Truth") or ""),
        managed_order_class=str(classifications.get("Managed Order Registry") or ""),
        position_class=str(classifications.get("Position Truth") or ""),
        managed_position_class=str(classifications.get("Managed Position Registry") or ""),
        reconciliation_class=str(classifications.get("Reconciliation") or ""),
    )
    managed_exit_context = active_hold or active_exit_due
    blockers: list[dict[str, str]] = []
    for service, allowed_values in RUNTIME_START_REQUIRED_CLASSIFICATIONS.items():
        observed = str(classifications.get(service) or "MISSING")
        if observed not in allowed_values:
            if service == "Managed Position Registry" and _managed_position_registry_clean_for_runtime_start(
                observed=observed,
                managed_position_registry=managed_position_registry,
                classifications=classifications,
            ):
                continue
            if service == "Reconciliation" and current_flat_authority_clean:
                continue
            if (
                service == "Broker Truth Lease"
                and current_flat_authority_clean
                and broker_truth_lease_authority.get("invalidated_diagnostic_only") is True
            ):
                continue
            if managed_exit_context and service in {
                "Open Order Truth",
                "Managed Order Registry",
                "Position Truth",
                "Runtime Environment Truth",
                "Managed Position Registry",
            }:
                continue
            blockers.append(
                {
                    "code": f"{service.lower().replace(' ', '_')}_not_clean_for_runtime_start",
                    "detail": (
                        f"{service} is {observed}; expected one of "
                        f"{', '.join(sorted(allowed_values))} before Track B PAPER runtime start."
                    ),
                    "service": service,
                    "observed": observed,
                    "expected": ", ".join(sorted(allowed_values)),
                }
            )
    if result.get("live_money_eligible") is not False:
        blockers.append(
            {
                "code": "live_money_eligible_not_false",
                "detail": "Shared Truth preflight did not report live_money_eligible=false.",
                "service": "Shared Truth",
                "observed": str(result.get("live_money_eligible")),
                "expected": "False",
            }
        )
    for blocker in _list(result.get("unsafe_blockers")):
        code = str(_mapping(blocker).get("code") or "")
        if not code:
            continue
        blockers.append(
            {
                "code": f"shared_truth_{code}",
                "detail": str(_mapping(blocker).get("detail") or "Shared Truth reported an unsafe blocker."),
                "service": "Shared Truth",
                "observed": code,
                "expected": "no unsafe blockers",
            }
        )
    blockers = _dedupe_codes(blockers)
    return {
        "schema_version": "track_b_runtime_start_shared_truth_preflight_v1",
        "generated_at": result.get("generated_at"),
        "refresh_generation_id": result.get("refresh_generation_id"),
        "refresh_phase": result.get("refresh_phase"),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "source": "track_b_shared_truth_refresh_cli",
        "clean_for_runtime_start": not blockers,
        "active_hold_managed_timed_exit_pending": active_hold,
        "active_managed_exit_due": active_exit_due,
        "active_authority": active_authority,
        "classification": "SHARED_TRUTH_PREFLIGHT_CLEAN" if not blockers else "SHARED_TRUTH_PREFLIGHT_BLOCKED",
        "required_classifications": {
            service: sorted(values) for service, values in RUNTIME_START_REQUIRED_CLASSIFICATIONS.items()
        },
        "observed_classifications": dict(classifications),
        "artifact_paths": _mapping(result.get("artifact_paths")),
        "source_refresh_artifact_path": str(
            result.get("source_refresh_artifact_path") or ""
        )
        or None,
        "warnings": _list(result.get("warnings")),
        "blockers": blockers,
    }


def _active_rows(rows: Any) -> list[Mapping[str, Any]]:
    active: list[Mapping[str, Any]] = []
    for row in _list(rows):
        item = _mapping(row)
        if item.get("historical_only") is True:
            continue
        if item.get("diagnostic_only") is True:
            continue
        if item.get("current_scope_active") is False:
            continue
        if item.get("invalidated_by_current_truth") is True:
            continue
        active.append(item)
    return active


def _managed_position_registry_active_authority(payload: Mapping[str, Any]) -> dict[str, Any]:
    active = _active_rows(payload.get("managed_positions"))
    invalidation = _mapping(payload.get("current_truth_invalidation"))
    invalidated = _list(invalidation.get("invalidated_positions"))
    return {
        "classification": payload.get("classification"),
        "active_managed_position_count": len(active),
        "invalidated_position_count": len(invalidated),
        "only_invalidated_historical_positions": len(active) == 0 and bool(invalidated),
        "current_truth_invalidation_enabled": invalidation.get("enabled") is True,
    }


def _top_level_current_truth_invalidation_state(payload: Mapping[str, Any]) -> dict[str, Any]:
    invalidation = _mapping(payload.get("current_truth_invalidation"))
    return {
        "invalidated_diagnostic_only": (
            invalidation.get("invalidated_by_current_truth") is True
            and invalidation.get("current_scope_active") is False
            and invalidation.get("diagnostic_only") is True
        ),
        "current_scope_active": invalidation.get("current_scope_active"),
        "diagnostic_only": invalidation.get("diagnostic_only"),
        "invalidated_by_current_truth": invalidation.get("invalidated_by_current_truth"),
    }


def _current_flat_truth_clean(*, classifications: Mapping[str, Any]) -> bool:
    return (
        str(classifications.get("Open Order Truth") or "") == NO_OPEN_ORDERS
        and str(classifications.get("Managed Order Registry") or "") == NO_MANAGED_ORDERS
        and str(classifications.get("Position Truth") or "") == "CLEAN_FLAT_READY"
    )


def _managed_position_registry_clean_for_runtime_start(
    *,
    observed: str,
    managed_position_registry: Mapping[str, Any],
    classifications: Mapping[str, Any],
) -> bool:
    if observed != "STALE_MANAGED_POSITION_EVIDENCE":
        return False
    return (
        _current_flat_truth_clean(classifications=classifications)
        and int(managed_position_registry.get("active_managed_position_count") or 0) == 0
        and managed_position_registry.get("only_invalidated_historical_positions") is True
        and managed_position_registry.get("current_truth_invalidation_enabled") is True
    )


def _clean_flat_fast_path_eligibility(
    *,
    reconciliation: Mapping[str, Any],
    registry_diagnostics: Mapping[str, Any],
) -> dict[str, Any]:
    input_symbols = _normal_symbols(reconciliation.get("symbols"))
    canonical_symbols = _normal_symbols(PHASE1_RUNTIME_TICKER_ORDER)
    checks = {
        "canonical_symbol_scope_complete": input_symbols == canonical_symbols,
        "broker_positions_empty": len(_list(reconciliation.get("track_b_broker_positions"))) == 0
        and _int(reconciliation.get("track_b_broker_position_count")) == 0,
        "broker_open_orders_empty": len(_list(reconciliation.get("track_b_broker_open_orders"))) == 0
        and _int(reconciliation.get("track_b_broker_open_order_count")) == 0,
        "lifecycle_open_positions_empty": len(_list(reconciliation.get("track_b_lifecycle_positions"))) == 0
        and _int(reconciliation.get("lifecycle_open_position_count")) == 0,
        "lifecycle_open_orders_empty": _int(reconciliation.get("lifecycle_open_order_count")) == 0,
        "current_scope_review_required_zero": _int(
            _first_present(reconciliation, "current_scope_review_required_count", "review_required_count")
        ) == 0,
        "unresolved_submit_intent_ownership_zero": _int(
            reconciliation.get("unresolved_submit_intent_ownership_count")
        ) == 0,
        "registry_diagnostics_current_scope_clean": _registry_diagnostics_current_scope_clean(registry_diagnostics),
        "broker_lifecycle_reconciliation_clean": reconciliation.get("classification")
        in {"BROKER_LIFECYCLE_RECONCILED", "TRACK_B_PAPER_BROKER_RECONCILED"}
        and reconciliation.get("broker_reconciled") is not False,
    }
    failed = [name for name, passed in checks.items() if not passed]
    return {
        "used": not failed,
        "classification": "CLEAN_FLAT_CURRENT_SCOPE_FAST_PATH_USED"
        if not failed
        else "CLEAN_FLAT_CURRENT_SCOPE_FAST_PATH_DISABLED",
        "disabled_reasons": failed,
        "checks": checks,
        "registry_diagnostics_classification": registry_diagnostics.get("classification") or "MISSING",
        "skipped_full_registry_reduction": not failed,
        "skipped_manifest_directory_scan": not failed,
        "skipped_lifecycle_report_scan": not failed,
    }


def _clean_flat_open_order_truth(
    *,
    now: datetime,
    config: TrackBOpenOrderTruthConfig,
    reconciliation: Mapping[str, Any],
    registry_diagnostics: Mapping[str, Any],
    fast_path: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_open_order_truth_v1",
        "generated_at": now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": reconciliation.get("live_money_eligible") is True,
        "classification": NO_OPEN_ORDERS,
        "canonical_refresh_scope": "GLOBAL_COMPLETE",
        "canonical_scope_blockers": [],
        "input_symbols": list(PHASE1_RUNTIME_TICKER_ORDER),
        "canonical_symbols": list(PHASE1_RUNTIME_TICKER_ORDER),
        "source_freshness": {
            "reconciliation_generated_at": reconciliation.get("generated_at"),
            "age_seconds": 0.0,
            "ttl_seconds": float(config.artifact_max_age_seconds),
            "stale": False,
        },
        "broker_open_orders": [],
        "broker_positions": [],
        "lifecycle_open_positions": [],
        "unresolved_submit_ownership": [],
        "order_states": [],
        "duplicate_close_order_groups": [],
        "broker_positions_without_close_order": [],
        "broker_flat_with_open_close_order": [],
        "terminal_registry_truth_overlay": {
            "enabled": True,
            "record_count": None,
            "source": "bounded_current_scope_fast_path",
            "full_registry_reduction_skipped": True,
        },
        "registry_truth_diagnostics": _registry_diagnostics_summary(registry_diagnostics),
        "bounded_current_scope_fast_path": dict(fast_path),
        "position_truth_summary": {},
        "live_position_status_summary": {"open_position_count": 0, "review_required_count": 0},
        "reconciliation": _reconciliation_summary(reconciliation),
        "summary": {
            "classification": NO_OPEN_ORDERS,
            "open_order_count": 0,
            "working_close_order_count": 0,
            "working_entry_order_count": 0,
            "suspicious_order_count": 0,
            "duplicate_close_order_group_count": 0,
            "broker_position_without_close_order_count": 0,
            "broker_flat_with_open_close_order_count": 0,
        },
        "event_state": {
            "classification": NO_OPEN_ORDERS,
            "signature": "NO_OPEN_ORDERS|0|0|0",
            "order_count": 0,
            "duplicate_close_order_group_count": 0,
            "broker_position_without_close_order_count": 0,
            "broker_flat_with_open_close_order_count": 0,
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None,
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "live_position_status": str(config.resolve(config.live_position_status_path)),
            "lifecycle_root": str(config.resolve(config.lifecycle_root)),
        },
    }


def _clean_flat_managed_order_registry(
    *,
    now: datetime,
    config: TrackBManagedOrderRegistryConfig,
    open_order_truth: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    registry_diagnostics: Mapping[str, Any],
    fast_path: Mapping[str, Any],
    managed_position_registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_managed_order_registry_v1",
        "generated_at": now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": reconciliation.get("live_money_eligible") is True,
        "classification": NO_MANAGED_ORDERS,
        "managed_orders": [],
        "terminal_registry_truth_overlay": {
            "enabled": True,
            "record_count": None,
            "superseded_full_audit_only_count": 0,
            "superseded_full_audit_only": [],
            "source": "bounded_current_scope_fast_path",
            "full_registry_reduction_skipped": True,
        },
        "pre_restart_exposure_resolution": _no_open_exposure_resolution(reconciliation),
        "source_freshness": {},
        "bounded_current_scope_fast_path": dict(fast_path),
        "registry_truth_diagnostics": _registry_diagnostics_summary(registry_diagnostics),
        "open_order_truth": _authority_summary(open_order_truth, config.resolve(config.open_order_truth_path)),
        "position_truth": {},
        "managed_position_registry": _authority_summary(
            managed_position_registry or {},
            config.resolve(config.managed_position_registry_path),
        ),
        "reconciliation": _reconciliation_summary(reconciliation),
        "summary": {
            "classification": NO_MANAGED_ORDERS,
            "managed_order_count": 0,
            "working_entry_order_count": 0,
            "working_close_order_count": 0,
            "modifiable_close_order_count": 0,
            "cancel_replace_candidate_count": 0,
            "suspicious_order_count": 0,
            "duplicate_close_order_count": 0,
            "position_without_close_order_count": 0,
            "active_hold_managed_timed_exit_pending_count": 0,
        },
        "event_state": {
            "classification": NO_MANAGED_ORDERS,
            "signature": "NO_MANAGED_ORDERS|0|0|0|0",
            "managed_order_count": 0,
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None,
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "position_truth": str(config.resolve(config.position_truth_path)),
            "managed_position_registry": str(config.resolve(config.managed_position_registry_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "lifecycle_root": str(config.resolve(config.lifecycle_root)),
            "manifest_root": str(config.resolve(config.manifest_root)),
            "submit_ownership_jsonl": str(config.resolve(config.submit_ownership_jsonl_path)),
        },
    }


def _clean_flat_position_truth(
    *,
    now: datetime,
    config: TrackBPositionTruthMonitorConfig,
    open_order_truth: Mapping[str, Any],
    managed_order_registry: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    registry_diagnostics: Mapping[str, Any],
    fast_path: Mapping[str, Any],
    managed_position_registry: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    symbols = sorted(str(item).upper() for item in reconciliation.get("symbols") or [] if str(item).strip())
    position_states = [
        {
            "symbol": symbol,
            "classification": "CLEAN_FLAT",
            "detail": "No current broker, lifecycle, order, or registry exposure.",
            "broker_quantity": "0",
            "lifecycle_quantity": "0",
            "open_order_count": 0,
            "review_required_count": 0,
        }
        for symbol in symbols
    ]
    event_state = {
        "reconciliation_classification": reconciliation.get("classification"),
        "runtime_stopped_with_broker_exposure": False,
        "symbols": {row["symbol"]: row for row in position_states},
    }
    return {
        "schema_version": "track_b_position_truth_v1",
        "generated_at": now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": reconciliation.get("live_money_eligible") is True,
        "reconciliation": _reconciliation_summary(reconciliation),
        "broker_lease": {},
        "runtime_status": {},
        "broker_positions": [],
        "open_broker_orders": [],
        "lifecycle_open_positions": [],
        "review_required_positions": [],
        "unresolved_submit_ownership": [],
        "known_managed_exit_orders": [],
        "unknown_broker_open_orders": [],
        "open_order_truth": _authority_summary(open_order_truth, config.resolve(config.open_order_truth_path)),
        "managed_order_registry": _authority_summary(
            managed_order_registry,
            config.resolve(config.managed_order_registry_path),
        ),
        "managed_position_registry": _authority_summary(
            managed_position_registry or {},
            config.resolve(config.managed_order_registry_path),
        ),
        "position_states": position_states,
        "summary": {
            "overall_classification": "CLEAN_FLAT_READY",
            "broker_exposure_present": False,
            "open_order_present": False,
            "review_required_count": 0,
            "unresolved_submit_ownership_count": 0,
            "symbol_count": len(position_states),
        },
        "event_state": event_state,
        "bounded_current_scope_fast_path": dict(fast_path),
        "registry_truth_diagnostics": _registry_diagnostics_summary(registry_diagnostics),
        "artifact_paths": {
            "latest": str(config.resolve(config.output_path)),
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None,
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "broker_lease": str(config.resolve(config.broker_lease_path)),
            "runtime_truth": str(config.resolve(config.runtime_truth_path)),
            "headless_status": str(config.resolve(config.headless_status_path)),
            "live_position_status": str(config.resolve(config.live_position_status_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
        },
    }


def _clean_flat_managed_position_registry(
    *,
    now: datetime,
    config: TrackBManagedPositionRegistryConfig,
    open_order_truth: Mapping[str, Any],
    managed_order_registry: Mapping[str, Any],
    position_truth: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    registry_diagnostics: Mapping[str, Any],
    fast_path: Mapping[str, Any],
) -> dict[str, Any]:
    return {
        "schema_version": "track_b_managed_position_registry_v1",
        "generated_at": now.isoformat(),
        "mode": "PAPER",
        "read_only": True,
        "submit_authority": False,
        "paper_proof_invoked": False,
        "live_money_eligible": reconciliation.get("live_money_eligible") is True,
        "classification": NO_MANAGED_POSITIONS,
        "managed_positions": [],
        "broker_positions": [],
        "lifecycle_open_positions": [],
        "review_required_positions": [],
        "historical_review_positions": [],
        "superseded_lifecycle_projections": [],
        "projection_authority_diagnostics": {
            "classification": "PROJECTION_AUTHORITY_COHERENT",
            "authority_source": "CLEAN_FLAT_CURRENT_SCOPE_FAST_PATH",
            "repaired_missing_owner_count": 0,
            "divergence_count": 0,
        },
        "unresolved_submit_ownership": [],
        "pre_restart_exposure_resolution": _no_open_exposure_resolution(reconciliation),
        "source_freshness": {},
        "bounded_current_scope_fast_path": dict(fast_path),
        "registry_truth_diagnostics": _registry_diagnostics_summary(registry_diagnostics),
        "position_truth": _authority_summary(position_truth, config.resolve(config.position_truth_path)),
        "open_order_truth": _authority_summary(open_order_truth, config.resolve(config.open_order_truth_path)),
        "managed_order_registry": _authority_summary(
            managed_order_registry,
            config.resolve(config.managed_order_registry_path),
        ),
        "reconciliation": _reconciliation_summary(reconciliation),
        "summary": {
            "classification": NO_MANAGED_POSITIONS,
            "managed_position_count": 0,
            "attention_required_count": 0,
            "exit_due_count": 0,
            "close_working_count": 0,
            "suspicious_managed_order_count": 0,
            "duplicate_close_risk_count": 0,
            "broker_position_count": 0,
            "lifecycle_position_count": 0,
            "review_required_count": 0,
            "historical_review_position_count": 0,
            "pre_restart_resolved_managed_exposure_count": 0,
            "pre_restart_review_required_exposure_count": 0,
        },
        "event_state": {
            "classification": NO_MANAGED_POSITIONS,
            "signature": "NO_MANAGED_POSITIONS|0|0|0",
            "managed_position_count": 0,
        },
        "artifact_paths": {
            "authority": str(config.resolve(config.output_path)),
            "event_log": str(config.resolve(config.event_log_path)),
            "dashboard_projection": None,
            "position_truth": str(config.resolve(config.position_truth_path)),
            "open_order_truth": str(config.resolve(config.open_order_truth_path)),
            "managed_order_registry": str(config.resolve(config.managed_order_registry_path)),
            "reconciliation": str(config.resolve(config.reconciliation_path)),
            "live_position_status": str(config.resolve(config.live_position_status_path)),
            "lifecycle_root": str(config.resolve(config.lifecycle_root)),
            "manifest_root": str(config.resolve(config.manifest_root)),
            "market_data_root": str(config.resolve(config.market_data_root)),
            "position_intent_audit": str(config.resolve(config.position_intent_audit_path)),
            "hold_exit_shadow": str(config.resolve(config.hold_exit_shadow_output_path)),
        },
    }


def _registry_diagnostics_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": payload.get("classification"),
        "generated_at": payload.get("generated_at"),
        "current_scope_review_required_count": payload.get("current_scope_review_required_count"),
        "current_scope_trade_states": _list(payload.get("current_scope_trade_states")),
        "current_blockers": _list(payload.get("current_blockers")),
    }


def _registry_diagnostics_current_scope_clean(payload: Mapping[str, Any]) -> bool:
    classification = str(payload.get("classification") or "")
    if not classification:
        return False
    current_review_count = _int(payload.get("current_scope_review_required_count"))
    current_states = _list(payload.get("current_scope_trade_states"))
    current_blockers = _list(payload.get("current_blockers"))
    if current_review_count != 0 or current_states or current_blockers:
        return False
    if classification == TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE:
        return True
    return bool(payload.get("diagnostic_only") is True and "HISTORICAL" in classification)


def _reconciliation_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": payload.get("classification"),
        "broker_reconciled": payload.get("broker_reconciled"),
        "blockers": payload.get("blockers") or [],
        "review_required_count": payload.get("review_required_count"),
        "current_scope_review_required_count": payload.get("current_scope_review_required_count"),
        "unresolved_submit_intent_ownership_count": payload.get("unresolved_submit_intent_ownership_count"),
        "track_b_broker_open_order_count": payload.get("track_b_broker_open_order_count"),
        "track_b_broker_position_count": payload.get("track_b_broker_position_count"),
        "lifecycle_open_position_count": payload.get("lifecycle_open_position_count"),
        "lifecycle_open_order_count": payload.get("lifecycle_open_order_count"),
        "generated_at": payload.get("generated_at"),
    }


def _authority_summary(payload: Mapping[str, Any], path: Path) -> dict[str, Any]:
    summary = _mapping(payload.get("summary"))
    return {
        "classification": payload.get("classification") or summary.get("overall_classification"),
        "summary": summary,
        "generated_at": payload.get("generated_at"),
        "authority_generation_id": payload.get("authority_generation_id"),
        "authority_cycle_generated_at": payload.get("authority_cycle_generated_at"),
        "artifact_path": str(path),
    }


def _no_open_exposure_resolution(reconciliation: Mapping[str, Any]) -> dict[str, Any]:
    broker_open_order_count = _int(reconciliation.get("track_b_broker_open_order_count"))
    return {
        "classification": "NO_OPEN_EXPOSURE",
        "broker_position_count": 0,
        "broker_open_order_count": broker_open_order_count,
        "resolved_managed_exposure_count": 0,
        "review_required_exposure_count": 0,
        "resolved_lifecycle_positions": [],
        "managed_exposures": [],
        "review_required_exposures": [],
        "restart_with_owned_exposure_allowed": False,
        "no_broad_flatten_generated": True,
        "read_only": True,
        "current_exposure_owner_resolution": {
            "classification": "NO_OPEN_EXPOSURE",
            "broker_position_count": 0,
            "broker_open_order_count": broker_open_order_count,
            "owned_exposure_count": 0,
            "review_required_exposure_count": 0,
            "owned_exposures": [],
            "review_required_exposures": [],
            "resolved_lifecycle_positions": [],
        },
    }


def _first_present(payload: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = payload.get(key)
        if value not in {None, ""}:
            return value
    return 0


def _int(value: Any) -> int:
    try:
        return int(float(str(value)))
    except (TypeError, ValueError):
        return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Refresh read-only Track B PAPER shared-truth authority artifacts.")
    parser.add_argument("--repo-root", default=str(Path(__file__).resolve().parents[3]))
    parser.add_argument("--account", default=PAPER_ACCOUNT)
    parser.add_argument("--symbols", default=",".join(PHASE1_RUNTIME_TICKER_ORDER))
    parser.add_argument("--json", action="store_true", help="Print JSON instead of a compact table.")
    parser.add_argument("--no-broker-lease-history", action="store_true", help="Skip broker lease history append.")
    parser.add_argument(
        "--runtime-start-preflight",
        action="store_true",
        help="Require clean shared-truth classifications for a Track B PAPER runtime start.",
    )
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    symbols = tuple(symbol.strip().upper() for symbol in str(args.symbols).split(",") if symbol.strip())
    config = TrackBSharedTruthRefreshConfig(
        repo_root=repo_root,
        account=str(args.account),
        symbols=symbols,
        broker_lease_history_path=None if bool(args.no_broker_lease_history) else DEFAULT_LEASE_HISTORY,
    )
    result = refresh_track_b_shared_truth(config=config)
    if bool(args.runtime_start_preflight):
        result = {
            **result,
            "runtime_start_preflight": build_runtime_start_preflight_summary(result),
        }
        if result["runtime_start_preflight"]["blockers"]:
            result["exit_code"] = 2
    if bool(args.json):
        print(json.dumps(result, indent=2, sort_keys=True))
    else:
        print_classification_table(result)
        preflight = _mapping(result.get("runtime_start_preflight"))
        if preflight:
            print("")
            print(f"Runtime start preflight: {preflight.get('classification')}")
            for blocker in _list(preflight.get("blockers")):
                print(f"- {blocker.get('code')}: {blocker.get('detail')}")
    return int(result.get("exit_code") or 0)


def print_classification_table(result: Mapping[str, Any]) -> None:
    print("Track B Shared Truth Refresh")
    print(f"generated_at: {result.get('generated_at')}")
    print(f"refresh_generation_id: {result.get('refresh_generation_id')}")
    print("")
    print(f"{'Service':<30} {'Classification':<40} Artifact")
    print(f"{'-' * 30} {'-' * 40} {'-' * 8}")
    for row in _list(result.get("services")):
        print(f"{str(row.get('service') or ''):<30} {str(row.get('classification') or ''):<40} {row.get('artifact_path') or ''}")
    warnings = _list(result.get("warnings"))
    blockers = _list(result.get("unsafe_blockers"))
    if warnings:
        print("")
        print("Warnings:")
        for warning in warnings:
            print(f"- {warning.get('code')}: {warning.get('detail')}")
    if blockers:
        print("")
        print("Unsafe blockers:")
        for blocker in blockers:
            print(f"- {blocker.get('code')}: {blocker.get('detail')}")


def _refresh_broker_lease(
    *,
    config: TrackBSharedTruthRefreshConfig,
    reconciliation: Mapping[str, Any],
    now: datetime,
) -> dict[str, Any]:
    published_lease = _read_json(config.resolve(config.broker_lease_path))
    broker_status = _read_json(config.resolve(config.broker_truth_status_path))
    latest_attempt = _read_json(config.resolve(config.broker_truth_latest_attempt_path)) or _mapping(
        broker_status.get("latest_attempt_status")
    )
    if not broker_status and not reconciliation:
        return {}
    live_position_status = _read_json(config.resolve(config.live_position_status_path))
    trade_summary = _read_json(config.resolve(config.trade_summary_path))
    last_successful_broker_truth = _mapping(broker_status.get("last_successful_broker_truth")) or broker_status
    lease = classify_broker_truth_lease(
        {
            "account_id": config.account,
            "allowed_instruments": list(config.symbols),
            "current_time": now.isoformat(),
            "policy": {
                "max_entry_age_seconds": float(config.broker_lease_max_entry_age_seconds),
                "max_exit_age_seconds": float(config.broker_lease_max_exit_age_seconds),
                "degraded_refresh_grace_seconds": float(config.broker_lease_degraded_refresh_grace_seconds),
            },
            "last_successful_broker_truth": last_successful_broker_truth,
            "latest_attempt_status": latest_attempt,
            "reconciliation": reconciliation,
            "lifecycle": _lifecycle_summary(live_position_status),
            "order_state": _order_state_summary(trade_summary, reconciliation),
            "source_artifact_paths": {
                "broker_truth_status": str(config.resolve(config.broker_truth_status_path)),
                "latest_attempt": str(config.resolve(config.broker_truth_latest_attempt_path)),
                "reconciliation": str(config.resolve(DEFAULT_RECONCILIATION_ARTIFACT)),
                "lifecycle": str(config.resolve(config.live_position_status_path)),
                "order_state": str(config.resolve(config.trade_summary_path)),
            },
            "source_artifact_timestamps": {
                key: value
                for key, value in {
                    "broker_truth_status": _artifact_timestamp(config.resolve(config.broker_truth_status_path), broker_status),
                    "latest_attempt": _artifact_timestamp(config.resolve(config.broker_truth_latest_attempt_path), latest_attempt),
                    "reconciliation": _artifact_timestamp(config.resolve(DEFAULT_RECONCILIATION_ARTIFACT), reconciliation),
                    "lifecycle": _artifact_timestamp(config.resolve(config.live_position_status_path), live_position_status),
                    "order_state": _artifact_timestamp(config.resolve(config.trade_summary_path), trade_summary),
                }.items()
                if value is not None
            },
        }
    )
    lease = preserve_invalidated_previous_lease_diagnostic(
        lease=lease,
        previous_lease=published_lease,
        broker_truth=last_successful_broker_truth,
        open_order_truth=_read_json(config.resolve(DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT)),
    )
    if lease:
        _write_json_atomic(config.resolve(config.broker_lease_path), lease)
    return lease


def _refresh_paper_recovery_policy(*, repo_root: Path, now: datetime) -> tuple[dict[str, Any], Path]:
    from .track_b_paper_recovery_policy import (
        TrackBPaperRecoveryPolicyConfig,
        build_track_b_paper_recovery_policy,
        write_track_b_paper_recovery_policy,
    )

    policy_config = TrackBPaperRecoveryPolicyConfig(repo_root=repo_root, dashboard_projection_path=None)
    payload = build_track_b_paper_recovery_policy(config=policy_config, now=now)
    path = write_track_b_paper_recovery_policy(config=policy_config, payload=payload)
    return payload, path


def _refresh_recovery_budget_ledger(*, repo_root: Path, now: datetime) -> tuple[dict[str, Any], Path]:
    from .track_b_recovery_budget_ledger import (
        TrackBRecoveryBudgetLedgerConfig,
        build_track_b_recovery_budget_ledger,
        write_track_b_recovery_budget_ledger,
    )

    budget_config = TrackBRecoveryBudgetLedgerConfig(repo_root=repo_root)
    payload = build_track_b_recovery_budget_ledger(config=budget_config, now=now)
    path = write_track_b_recovery_budget_ledger(config=budget_config, payload=payload)
    return payload, path


def _refresh_autonomous_recovery_plan(*, repo_root: Path, now: datetime) -> tuple[dict[str, Any], Path]:
    from .track_b_paper_autonomous_recovery_planner import (
        TrackBPaperAutonomousRecoveryPlannerConfig,
        build_track_b_paper_autonomous_recovery_plan,
        write_track_b_paper_autonomous_recovery_plan,
    )

    plan_config = TrackBPaperAutonomousRecoveryPlannerConfig(repo_root=repo_root)
    payload = build_track_b_paper_autonomous_recovery_plan(config=plan_config, now=now)
    path = write_track_b_paper_autonomous_recovery_plan(config=plan_config, payload=payload)
    return payload, path


def _service_row(
    service: str,
    payload: Mapping[str, Any],
    artifact_path: Path,
    *,
    summary_key: str | None = None,
) -> dict[str, Any]:
    summary = _mapping(payload.get("summary"))
    classification = payload.get("classification")
    if summary_key is not None:
        classification = summary.get(summary_key) or classification
    return {
        "service": service,
        "classification": classification,
        "generated_at": payload.get("generated_at"),
        "authority_generation_id": payload.get("authority_generation_id"),
        "authority_cycle_generated_at": payload.get("authority_cycle_generated_at"),
        "source_generation_references": _mapping(payload.get("source_generation_references")),
        "artifact_path": str(artifact_path),
    }


def _with_authority_cycle(
    payload: Mapping[str, Any],
    *,
    generation_id: str,
    cycle_generated_at: datetime,
    **sources: Mapping[str, Any],
) -> dict[str, Any]:
    references = {
        **_mapping(payload.get("source_generation_references")),
        **_source_generation_references(**sources),
    }
    return {
        **dict(payload),
        "authority_generation_id": generation_id,
        "authority_cycle_generated_at": cycle_generated_at.isoformat(),
        "source_generation_references": references,
    }


def _source_generation_references(**sources: Mapping[str, Any]) -> dict[str, Any]:
    references: dict[str, Any] = {}
    for name, payload in sources.items():
        key = str(name)
        artifact = _mapping(payload)
        generated_at = artifact.get("generated_at")
        authority_generation_id = artifact.get("authority_generation_id")
        if generated_at not in {None, ""}:
            references[f"{key}_generated_at"] = generated_at
        if authority_generation_id not in {None, ""}:
            references[f"{key}_authority_generation_id"] = authority_generation_id
    return references


def _paper_recovery_policy_row(*, payload: Mapping[str, Any], artifact_path: Path) -> dict[str, Any]:
    return {
        "service": "PAPER Recovery Policy",
        "classification": payload.get("paper_action_policy") or payload.get("classification") or "MISSING",
        "generated_at": payload.get("generated_at"),
        "artifact_path": str(artifact_path),
    }


def _recovery_budget_ledger_row(*, payload: Mapping[str, Any], artifact_path: Path) -> dict[str, Any]:
    return {
        "service": "Recovery Budget Ledger",
        "classification": payload.get("classification") or "MISSING",
        "generated_at": payload.get("generated_at"),
        "artifact_path": str(artifact_path),
        "budget_exhausted": payload.get("budget_exhausted") is True,
    }


def _autonomous_recovery_plan_row(*, payload: Mapping[str, Any], artifact_path: Path) -> dict[str, Any]:
    return {
        "service": "PAPER Autonomous Recovery Planner",
        "classification": payload.get("classification") or "MISSING",
        "generated_at": payload.get("generated_at"),
        "artifact_path": str(artifact_path),
    }


def _reconciliation_row(*, config: TrackBSharedTruthRefreshConfig, reconciliation: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "service": "Reconciliation",
        "classification": reconciliation.get("classification") or "MISSING",
        "generated_at": reconciliation.get("generated_at"),
        "artifact_path": str(config.resolve(DEFAULT_RECONCILIATION_ARTIFACT)),
    }


def _broker_lease_row(*, config: TrackBSharedTruthRefreshConfig, broker_lease: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "service": "Broker Truth Lease",
        "classification": broker_lease.get("lease_state") or "MISSING",
        "generated_at": broker_lease.get("generated_at"),
        "artifact_path": str(config.resolve(config.broker_lease_path)),
    }


def _warnings(*, services: Sequence[Mapping[str, Any]], payloads: Mapping[str, Mapping[str, Any]]) -> list[dict[str, str]]:
    warnings: list[dict[str, str]] = []
    for row in services:
        if row.get("classification") in {"MISSING", "ORDER_STATE_UNKNOWN_REVIEW_REQUIRED", "STALE_MANAGED_POSITION_EVIDENCE"}:
            warnings.append(
                {
                    "code": f"{str(row.get('service') or 'service').lower().replace(' ', '_')}_attention",
                    "detail": f"{row.get('service')} classification is {row.get('classification')}.",
                }
            )
        if row.get("service") == "PAPER Autonomous Recovery Planner" and row.get("classification") in {
            "PLAN_BLOCKED_STALE_EVIDENCE",
            "MISSING",
        }:
            warnings.append(
                {
                    "code": "paper_autonomous_recovery_plan_advisory_stale",
                    "detail": (
                        "PAPER Autonomous Recovery Planner is unavailable or requesting evidence refresh; "
                        "this is advisory evidence and not a broker-unsafe classification."
                    ),
                }
            )
    for name, payload in payloads.items():
        stale_sources = _stale_sources(payload)
        if stale_sources:
            warnings.append(
                {
                    "code": f"{name}_stale_sources",
                    "detail": f"{name} reports stale source evidence: {', '.join(stale_sources)}.",
                }
            )
    return _dedupe_codes(warnings)


def _unsafe_blockers(
    *,
    open_order_truth: Mapping[str, Any],
    managed_order_registry: Mapping[str, Any],
    position_truth: Mapping[str, Any],
    runtime_environment_truth: Mapping[str, Any],
    managed_position_registry: Mapping[str, Any],
    reconciliation: Mapping[str, Any],
    broker_lease: Mapping[str, Any],
    broker_position_guardian: Mapping[str, Any],
) -> list[dict[str, str]]:
    blockers: list[dict[str, str]] = []
    position_class = str(_mapping(position_truth.get("summary")).get("overall_classification") or "")
    runtime_class = str(runtime_environment_truth.get("classification") or "")
    managed_position_class = str(managed_position_registry.get("classification") or "")
    managed_order_class = str(managed_order_registry.get("classification") or "")
    open_order_class = str(open_order_truth.get("classification") or "")
    reconciliation_class = str(reconciliation.get("classification") or "")
    lease_state = str(broker_lease.get("lease_state") or "")
    guardian_class = str(broker_position_guardian.get("classification") or "")
    current_flat_authority_clean = _current_flat_truth_clean(
        classifications={
            "Open Order Truth": open_order_class,
            "Managed Order Registry": managed_order_class,
            "Position Truth": position_class,
            "Managed Position Registry": managed_position_class,
        }
    )
    managed_position_clean_by_current_truth = _managed_position_registry_clean_for_runtime_start(
        observed=managed_position_class,
        managed_position_registry=_managed_position_registry_active_authority(managed_position_registry),
        classifications={
            "Open Order Truth": open_order_class,
            "Managed Order Registry": managed_order_class,
            "Position Truth": position_class,
        },
    )
    stale_derived_diagnostic_only = current_flat_authority_clean and (
        managed_position_class == NO_MANAGED_POSITIONS or managed_position_clean_by_current_truth
    )
    active_hold = _managed_active_hold_pending(
        open_order_class=open_order_class,
        managed_order_class=managed_order_class,
        position_class=position_class,
        managed_position_class=managed_position_class,
        reconciliation_class=reconciliation_class,
    )
    active_exit_due = _managed_exit_due(
        open_order_class=open_order_class,
        managed_order_class=managed_order_class,
        position_class=position_class,
        managed_position_class=managed_position_class,
        reconciliation_class=reconciliation_class,
    )
    managed_exit_context = active_hold or active_exit_due

    if (
        open_order_class not in {NO_OPEN_ORDERS, "OPEN_CLOSE_ORDER_WORKING", "OPEN_ENTRY_ORDER_WORKING"}
        and not managed_exit_context
    ):
        blockers.append({"code": "open_order_truth_blocked", "detail": f"Open Order Truth is {open_order_class}."})
    if managed_order_class not in {
        NO_MANAGED_ORDERS,
        "WORKING_CLOSE_ORDER",
        "WORKING_ENTRY_ORDER",
        "CLOSE_ORDER_MODIFIABLE",
    } and not managed_exit_context:
        blockers.append({"code": "managed_order_registry_blocked", "detail": f"Managed Order Registry is {managed_order_class}."})
    if position_class and position_class != "CLEAN_FLAT_READY" and not managed_exit_context:
        blockers.append({"code": "position_truth_attention_required", "detail": f"Position Truth is {position_class}."})
    if (
        runtime_class not in {RUNTIME_DOWN_CLEAN, RUNTIME_ACTIVE_TRADE_CAPABLE, RUNTIME_ACTIVE_OBSERVATION_ONLY}
        and not managed_exit_context
    ):
        blockers.append({"code": "runtime_environment_blocked", "detail": f"Runtime Environment Truth is {runtime_class}."})
    if managed_position_class not in {NO_MANAGED_POSITIONS, "OPEN_MANAGED_MATCHED", "OPEN_MANAGED_EXIT_DUE", "OPEN_MANAGED_CLOSE_WORKING"}:
        if not managed_position_clean_by_current_truth and not (
            managed_position_class == "STALE_MANAGED_POSITION_EVIDENCE" and position_class == "CLEAN_FLAT_READY"
        ):
            blockers.append(
                {"code": "managed_position_registry_blocked", "detail": f"Managed Position Registry is {managed_position_class}."}
            )
    if reconciliation_class and reconciliation_class not in {
        "BROKER_LIFECYCLE_RECONCILED",
        "TRACK_B_PAPER_BROKER_RECONCILED",
    }:
        if not stale_derived_diagnostic_only:
            blockers.append({"code": "reconciliation_blocked", "detail": f"Reconciliation is {reconciliation_class}."})
    if lease_state.startswith("INVALIDATED"):
        if not (
            stale_derived_diagnostic_only
            and _top_level_current_truth_invalidation_state(broker_lease).get("invalidated_diagnostic_only") is True
        ):
            blockers.append({"code": "broker_lease_invalidated", "detail": f"Broker Truth Lease is {lease_state}."})
    if lease_state == "OPERATOR_REQUIRED" and position_class != "CLEAN_FLAT_READY":
        blockers.append({"code": "broker_lease_operator_required", "detail": "Broker Truth Lease requires operator attention."})
    if guardian_class and guardian_class != BROKER_POSITION_GUARDIAN_READY:
        hard = ", ".join(str(item) for item in broker_position_guardian.get("hard_classifications") or [])
        blockers.append(
            {
                "code": "broker_position_guardian_hard_hold",
                "detail": f"Broker Position Guardian is {guardian_class}: {hard or 'hard hold'}.",
            }
        )
    return blockers


def _managed_active_hold_pending(
    *,
    open_order_class: str,
    managed_order_class: str,
    position_class: str,
    managed_position_class: str,
    reconciliation_class: str,
) -> bool:
    return (
        open_order_class in {NO_OPEN_ORDERS, BROKER_POSITION_WITHOUT_CLOSE_ORDER}
        and managed_order_class == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
        and position_class == ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING
        and managed_position_class == "OPEN_MANAGED_MATCHED"
        and reconciliation_class in {"BROKER_LIFECYCLE_RECONCILED", "TRACK_B_PAPER_BROKER_RECONCILED"}
    )


def _managed_exit_due(
    *,
    open_order_class: str,
    managed_order_class: str,
    position_class: str,
    managed_position_class: str,
    reconciliation_class: str,
) -> bool:
    return (
        open_order_class in {NO_OPEN_ORDERS, BROKER_POSITION_WITHOUT_CLOSE_ORDER}
        and managed_order_class == "POSITION_WITHOUT_CLOSE_ORDER"
        and position_class == "ATTENTION_REQUIRED"
        and managed_position_class == "OPEN_MANAGED_EXIT_DUE"
        and reconciliation_class in {"BROKER_LIFECYCLE_RECONCILED", "TRACK_B_PAPER_BROKER_RECONCILED"}
    )


def _stale_sources(payload: Mapping[str, Any]) -> list[str]:
    sources = _mapping(payload.get("source_freshness")).get("stale_sources")
    if isinstance(sources, list):
        return [str(item) for item in sources if str(item)]
    evidence_stale = []
    for key, value in payload.items():
        if key.endswith("_stale_or_missing") and value is True:
            evidence_stale.append(key)
    return evidence_stale


def _lifecycle_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    open_positions = payload.get("open_positions") or payload.get("positions") or payload.get("track_b_lifecycle_positions") or []
    return {
        **dict(payload),
        "open_positions": list(open_positions) if isinstance(open_positions, list) else [],
        "open_position_count": payload.get("open_position_count")
        or payload.get("lifecycle_open_position_count")
        or len(open_positions if isinstance(open_positions, list) else []),
    }


def _order_state_summary(payload: Mapping[str, Any], reconciliation: Mapping[str, Any]) -> dict[str, Any]:
    return {
        **dict(payload),
        "unknown_open_order_count": payload.get("unknown_open_order_count")
        or reconciliation.get("unknown_broker_open_order_count")
        or 0,
        "lifecycle_open_order_count": payload.get("lifecycle_open_order_count")
        or reconciliation.get("lifecycle_open_order_count")
        or 0,
        "unresolved_intent_count": payload.get("unresolved_intent_count")
        or reconciliation.get("unresolved_submit_intent_ownership_count")
        or 0,
    }


def _autonomous_recovery_next_action(payload: Mapping[str, Any]) -> str | None:
    for action in _list(payload.get("proposed_actions")):
        action_map = _mapping(action)
        value = str(action_map.get("action_type") or action_map.get("action_id") or "")
        if value:
            return value
    for action in _list(payload.get("blocked_actions")):
        action_map = _mapping(action)
        value = str(action_map.get("action_type") or action_map.get("action_id") or "")
        if value:
            return value
    return None


def _refresh_generation_id(value: datetime) -> str:
    return f"track-b-shared-truth-{value.strftime('%Y%m%dT%H%M%S%fZ')}"


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    write_json_atomic(path, payload)


def _artifact_timestamp(path: Path, payload: Mapping[str, Any]) -> str | None:
    if payload.get("generated_at"):
        return str(payload["generated_at"])
    try:
        return datetime.fromtimestamp(path.stat().st_mtime, tz=UTC).isoformat()
    except OSError:
        return None


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(Path(path).read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _normal_symbols(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list | tuple):
        return ()
    return tuple(text for symbol in value if (text := str(symbol).strip().upper()))


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _list(value: Any) -> list[Any]:
    return list(value) if isinstance(value, list) else []


def _ensure_utc(value: datetime) -> datetime:
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _dedupe_codes(rows: Sequence[Mapping[str, str]]) -> list[dict[str, str]]:
    seen: set[str] = set()
    result: list[dict[str, str]] = []
    for row in rows:
        code = str(row.get("code") or "")
        if not code or code in seen:
            continue
        seen.add(code)
        result.append({"code": code, "detail": str(row.get("detail") or "")})
    return result


if __name__ == "__main__":
    raise SystemExit(main())
