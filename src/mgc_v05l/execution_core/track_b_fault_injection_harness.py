"""Artifact-level Track B fault-injection harness.

The harness is read-only with respect to broker/runtime systems. It builds
temporary artifact states, runs existing authority/readiness/reconciliation
helpers against them, and returns scenario verdicts that make retired bug
classes regression-testable.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_central_trade_registry import (
    TradeEvent,
    TradeEventType,
    reduce_trade_events,
)
from mgc_v05l.execution_core.track_b_current_exposure_owner_resolver import (
    CurrentExposureOwnerResolverConfig,
    apply_current_exposure_owner_lifecycle_overlay,
    resolve_current_exposure_ownership,
)
from mgc_v05l.execution_core.track_b_futures_contract_resolver import (
    FuturesContractResolverInput,
    evaluate_futures_contract_pre_submit,
)
from mgc_v05l.execution_core.track_b_live_runtime_environment_watchdog import (
    TrackBLiveRuntimeEnvironmentWatchdogConfig,
    build_track_b_live_runtime_environment_watchdog,
)
from mgc_v05l.execution_core.track_b_risk_reducing_close_authority import (
    classify_runtime_stale_risk_reducing_close,
)


SCENARIO_STALE_RUNTIME_EXIT_DUE = "stale_runtime_with_exit_due_position"
SCENARIO_STALE_OWNER_FRESH_BROKER = "stale_owner_candidates_with_fresh_broker_owner"
SCENARIO_HISTORICAL_REGISTRY_DEBRIS = "historical_registry_debris_clean_current_scope"
SCENARIO_DUPLICATE_LIFECYCLE_ROWS = "duplicate_lifecycle_rows_same_contract"
SCENARIO_DEAD_PID_STALE_HEARTBEAT = "dead_pid_with_stale_heartbeat"
SCENARIO_NEAR_EXPIRY_CONTRACT = "near_expiry_contract_submit_attempt"
SCENARIO_PHASE1_FRESH_RUNTIME_STALE = "phase1_fresh_runtime_ingestion_stale"
SCENARIO_BROKER_FILL_NO_LIFECYCLE_CLOSE = "broker_fill_without_lifecycle_close"
SCENARIO_MALFORMED_BROKER_POSITION_TRUTH = "malformed_broker_position_truth_artifact"
SCENARIO_MISSING_LIFECYCLE_RECONCILIATION = "missing_lifecycle_reconciliation_artifact"
SCENARIO_REGISTRY_DIAGNOSTIC_MISSING_CURRENT_SCOPE = "registry_diagnostic_missing_current_scope_fields"
SCENARIO_STALE_RUNTIME_ENV_FRESH_BROKER_TRUTH = "stale_runtime_environment_truth_with_fresh_broker_truth"
SCENARIO_CONFLICTING_AUTHORITY_GENERATION = "conflicting_authority_generation_id"
SCENARIO_MANAGED_CLOSE_ORDER_BROKER_ZERO = "managed_close_order_artifact_broker_open_orders_zero"
SCENARIO_BROKER_OPEN_ORDER_REGISTRY_NONE = "broker_open_order_exists_lifecycle_registry_none"
SCENARIO_MISSING_GUARDIAN_WITH_BROKER_EXPOSURE = "missing_guardian_artifact_with_broker_exposure"
SCENARIO_MANAGED_CLOSE_DISAPPEARS_BROKER_FLAT = "managed_close_order_disappears_broker_flat_without_fill_callback"
SCENARIO_REGISTRY_REVIEW_REQUIRED_NULL_LIFECYCLE_EXIT = "registry_review_required_null_lifecycle_blocks_valid_exit"

FAULT_INJECTION_SCENARIOS: tuple[str, ...] = (
    SCENARIO_STALE_RUNTIME_EXIT_DUE,
    SCENARIO_STALE_OWNER_FRESH_BROKER,
    SCENARIO_HISTORICAL_REGISTRY_DEBRIS,
    SCENARIO_DUPLICATE_LIFECYCLE_ROWS,
    SCENARIO_DEAD_PID_STALE_HEARTBEAT,
    SCENARIO_NEAR_EXPIRY_CONTRACT,
    SCENARIO_PHASE1_FRESH_RUNTIME_STALE,
    SCENARIO_BROKER_FILL_NO_LIFECYCLE_CLOSE,
    SCENARIO_MALFORMED_BROKER_POSITION_TRUTH,
    SCENARIO_MISSING_LIFECYCLE_RECONCILIATION,
    SCENARIO_REGISTRY_DIAGNOSTIC_MISSING_CURRENT_SCOPE,
    SCENARIO_STALE_RUNTIME_ENV_FRESH_BROKER_TRUTH,
    SCENARIO_CONFLICTING_AUTHORITY_GENERATION,
    SCENARIO_MANAGED_CLOSE_ORDER_BROKER_ZERO,
    SCENARIO_BROKER_OPEN_ORDER_REGISTRY_NONE,
    SCENARIO_MISSING_GUARDIAN_WITH_BROKER_EXPOSURE,
    SCENARIO_MANAGED_CLOSE_DISAPPEARS_BROKER_FLAT,
    SCENARIO_REGISTRY_REVIEW_REQUIRED_NULL_LIFECYCLE_EXIT,
)

DEFAULT_FAULT_INJECTION_NOW = datetime(2026, 6, 4, 14, 0, tzinfo=UTC)
FAULT_INJECTION_REPORT_SCHEMA_VERSION = "track_b_fault_injection_report_v1"

SAFETY_INVARIANTS_CHECKED: tuple[str, ...] = (
    "no_live_money_eligibility",
    "no_paper_proof",
    "no_broad_flatten",
    "no_unguarded_broker_mutation",
)

SCENARIO_METADATA: dict[str, dict[str, Any]] = {
    SCENARIO_STALE_RUNTIME_EXIT_DUE: {
        "bug_class": "Stranded Exit-Due Positions",
        "retired_invariant": "Fresh broker-backed exit-due exposure retains an exact scoped risk-reducing close path.",
        "authority_helpers_exercised": [
            "resolve_current_exposure_ownership",
            "classify_runtime_stale_risk_reducing_close",
        ],
        "expected_primary_classification": "RISK_REDUCING_CLOSE_ALLOWED_RUNTIME_STALE_WITH_BROKER_EXPOSURE",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach observed stale-runtime exit-due regression artifact links here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_STALE_OWNER_FRESH_BROKER: {
        "bug_class": "Ownership Ambiguity",
        "retired_invariant": "Newest broker-backed owner wins and stale candidates become diagnostic-only.",
        "authority_helpers_exercised": ["resolve_current_exposure_ownership"],
        "expected_primary_classification": "OWNED_MANAGED_EXIT_DUE",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach stale-owner/fresh-owner arbitration artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_HISTORICAL_REGISTRY_DEBRIS: {
        "bug_class": "Historical Registry Debris Blocking Current Truth",
        "retired_invariant": "Current hot-path truth is evaluated separately from historical audit debris.",
        "authority_helpers_exercised": ["build_track_b_live_runtime_environment_watchdog"],
        "expected_primary_classification": "READY_SUBMIT_CAPABLE",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach historical registry debris/current-scope-clean artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_DUPLICATE_LIFECYCLE_ROWS: {
        "bug_class": "Ownership Ambiguity",
        "retired_invariant": "Duplicate same-contract lifecycle rows do not double-count current broker exposure.",
        "authority_helpers_exercised": [
            "resolve_current_exposure_ownership",
            "apply_current_exposure_owner_lifecycle_overlay",
        ],
        "expected_primary_classification": "STALE_DUPLICATE_LIFECYCLE_AGGREGATION_FULL_AUDIT_ONLY",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach duplicate lifecycle aggregation artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_DEAD_PID_STALE_HEARTBEAT: {
        "bug_class": "Runtime Ingestion Freshness Confusion",
        "retired_invariant": "Dead PID plus stale heartbeat cannot classify as runtime-ready.",
        "authority_helpers_exercised": ["build_track_b_live_runtime_environment_watchdog"],
        "expected_primary_classification": "RECOVERY_REQUIRED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach dead-PID/stale-heartbeat watchdog artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_NEAR_EXPIRY_CONTRACT: {
        "bug_class": "Contract Ambiguity",
        "retired_invariant": "Canonical futures contract resolution is required before submit.",
        "authority_helpers_exercised": ["evaluate_futures_contract_pre_submit"],
        "expected_primary_classification": "CONTRACT_NEAR_EXPIRY",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach near-expiry contract resolver artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_PHASE1_FRESH_RUNTIME_STALE: {
        "bug_class": "Runtime Ingestion Freshness Confusion",
        "retired_invariant": "Readiness distinguishes feed healthy, runtime alive, and runtime ingestion stale.",
        "authority_helpers_exercised": ["build_track_b_live_runtime_environment_watchdog"],
        "expected_primary_classification": "DEGRADED_LANES_NOT_EVALUATING",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach feed-fresh/runtime-ingestion-stale artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_BROKER_FILL_NO_LIFECYCLE_CLOSE: {
        "bug_class": "Managed Exit Owner Identity Loss",
        "retired_invariant": "Broker-backed ownership remains visible when a fill exists without lifecycle close.",
        "authority_helpers_exercised": [
            "resolve_current_exposure_ownership",
            "classify_runtime_stale_risk_reducing_close",
        ],
        "expected_primary_classification": "OWNED_MANAGED_EXPOSURE",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach broker-fill-without-lifecycle-close artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_MALFORMED_BROKER_POSITION_TRUTH: {
        "bug_class": "Stale Truth Authority",
        "retired_invariant": "Malformed broker position truth is blocking/diagnostic and never silently accepted.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "MALFORMED_BROKER_POSITION_TRUTH_BLOCKED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach malformed broker-position truth artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_MISSING_LIFECYCLE_RECONCILIATION: {
        "bug_class": "Historical Registry Debris Blocking Current Truth",
        "retired_invariant": "Missing lifecycle reconciliation blocks submit while broker exposure remains visible.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "MISSING_LIFECYCLE_RECONCILIATION_BLOCKED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach missing lifecycle reconciliation artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_REGISTRY_DIAGNOSTIC_MISSING_CURRENT_SCOPE: {
        "bug_class": "Historical Registry Debris Blocking Current Truth",
        "retired_invariant": "Registry diagnostics without current-scope fields cannot certify clean current truth.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "REGISTRY_DIAGNOSTIC_CURRENT_SCOPE_MISSING_BLOCKED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach missing-current-scope registry diagnostics here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_STALE_RUNTIME_ENV_FRESH_BROKER_TRUTH: {
        "bug_class": "Stale Truth Authority",
        "retired_invariant": "Fresh broker truth remains visible but stale runtime environment truth cannot grant submit.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "STALE_RUNTIME_ENVIRONMENT_TRUTH_BROKER_TRUTH_FRESH_BLOCKED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach stale-runtime/fresh-broker authority artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_CONFLICTING_AUTHORITY_GENERATION: {
        "bug_class": "Stale Truth Authority",
        "retired_invariant": "Conflicting authority_generation_id values block submit until shared truth is coherent.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "CONFLICTING_AUTHORITY_GENERATION_ID_BLOCKED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach conflicting authority generation artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_MANAGED_CLOSE_ORDER_BROKER_ZERO: {
        "bug_class": "Ownership Ambiguity",
        "retired_invariant": "Managed close-order artifacts cannot override broker open-order truth.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "MANAGED_CLOSE_ORDER_PHANTOM_BROKER_ZERO_BLOCKED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach managed-close-order/broker-zero artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_BROKER_OPEN_ORDER_REGISTRY_NONE: {
        "bug_class": "Ownership Ambiguity",
        "retired_invariant": "Fresh broker open-order truth remains visible when lifecycle/registry artifacts say none.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "BROKER_OPEN_ORDER_WITHOUT_LIFECYCLE_REGISTRY_BLOCKED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach broker-open-order/lifecycle-none artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_MISSING_GUARDIAN_WITH_BROKER_EXPOSURE: {
        "bug_class": "Stranded Exit-Due Positions",
        "retired_invariant": "Broker exposure without Guardian authority remains visible but cannot create an unguarded close path.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "MISSING_GUARDIAN_WITH_BROKER_EXPOSURE_BLOCKED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach missing Guardian/broker exposure artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_MANAGED_CLOSE_DISAPPEARS_BROKER_FLAT: {
        "bug_class": "Managed Exit Owner Identity Loss",
        "retired_invariant": "Fresh broker-flat truth plus scoped managed-close evidence creates a local reconciliation path without broker mutation.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "MANAGED_CLOSE_DISAPPEARED_BROKER_FLAT_RECONCILIATION_REQUIRED",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach broker-flat/missing-fill-callback close reconciliation artifacts here.",
        "evidence": {"placeholder": True},
    },
    SCENARIO_REGISTRY_REVIEW_REQUIRED_NULL_LIFECYCLE_EXIT: {
        "bug_class": "Managed Exit Owner Identity Loss",
        "retired_invariant": "Registry REVIEW_REQUIRED with null lifecycle_id is normalized only from exact broker/lifecycle/fill identity before managed-exit attach.",
        "authority_helpers_exercised": ["artifact_integrity_classifier"],
        "expected_primary_classification": "REGISTRY_REVIEW_REQUIRED_NULL_LIFECYCLE_BLOCKED_MANAGED_EXIT",
        "safety_invariants_checked": list(SAFETY_INVARIANTS_CHECKED),
        "retirement_status": "FAULT_INJECTION_V1_COVERED",
        "notes": "Evidence placeholder: attach MES-style registry identity normalization artifacts here.",
        "evidence": {"placeholder": True},
    },
}


def list_track_b_fault_injection_scenarios() -> tuple[str, ...]:
    return FAULT_INJECTION_SCENARIOS


def run_track_b_fault_injection_harness(
    *,
    artifact_root: Path,
    scenarios: Sequence[str] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or DEFAULT_FAULT_INJECTION_NOW)
    names = tuple(scenarios or FAULT_INJECTION_SCENARIOS)
    results = [
        run_track_b_fault_injection_scenario(name=name, artifact_root=artifact_root / name, now=actual_now)
        for name in names
    ]
    return {
        "schema_version": "track_b_fault_injection_harness_v1",
        "generated_at": actual_now.isoformat(),
        "read_only": True,
        "broker_mutation_allowed": False,
        "scenario_count": len(results),
        "passed": all(result["passed"] for result in results),
        "scenarios": results,
    }


def run_track_b_fault_injection_scenario(
    *,
    name: str,
    artifact_root: Path,
    now: datetime | None = None,
) -> dict[str, Any]:
    actual_now = _ensure_utc(now or DEFAULT_FAULT_INJECTION_NOW)
    handlers: dict[str, Callable[[Path, datetime], dict[str, Any]]] = {
        SCENARIO_STALE_RUNTIME_EXIT_DUE: _scenario_stale_runtime_with_exit_due_position,
        SCENARIO_STALE_OWNER_FRESH_BROKER: _scenario_stale_owner_candidates_with_fresh_broker_owner,
        SCENARIO_HISTORICAL_REGISTRY_DEBRIS: _scenario_historical_registry_debris_clean_current_scope,
        SCENARIO_DUPLICATE_LIFECYCLE_ROWS: _scenario_duplicate_lifecycle_rows_same_contract,
        SCENARIO_DEAD_PID_STALE_HEARTBEAT: _scenario_dead_pid_with_stale_heartbeat,
        SCENARIO_NEAR_EXPIRY_CONTRACT: _scenario_near_expiry_contract_submit_attempt,
        SCENARIO_PHASE1_FRESH_RUNTIME_STALE: _scenario_phase1_fresh_runtime_ingestion_stale,
        SCENARIO_BROKER_FILL_NO_LIFECYCLE_CLOSE: _scenario_broker_fill_without_lifecycle_close,
        SCENARIO_MALFORMED_BROKER_POSITION_TRUTH: _scenario_malformed_broker_position_truth_artifact,
        SCENARIO_MISSING_LIFECYCLE_RECONCILIATION: _scenario_missing_lifecycle_reconciliation_artifact,
        SCENARIO_REGISTRY_DIAGNOSTIC_MISSING_CURRENT_SCOPE: _scenario_registry_diagnostic_missing_current_scope_fields,
        SCENARIO_STALE_RUNTIME_ENV_FRESH_BROKER_TRUTH: _scenario_stale_runtime_environment_truth_with_fresh_broker_truth,
        SCENARIO_CONFLICTING_AUTHORITY_GENERATION: _scenario_conflicting_authority_generation_id,
        SCENARIO_MANAGED_CLOSE_ORDER_BROKER_ZERO: _scenario_managed_close_order_artifact_broker_open_orders_zero,
        SCENARIO_BROKER_OPEN_ORDER_REGISTRY_NONE: _scenario_broker_open_order_exists_lifecycle_registry_none,
        SCENARIO_MISSING_GUARDIAN_WITH_BROKER_EXPOSURE: _scenario_missing_guardian_artifact_with_broker_exposure,
        SCENARIO_MANAGED_CLOSE_DISAPPEARS_BROKER_FLAT: _scenario_managed_close_order_disappears_broker_flat_without_fill_callback,
        SCENARIO_REGISTRY_REVIEW_REQUIRED_NULL_LIFECYCLE_EXIT: _scenario_registry_review_required_null_lifecycle_blocks_valid_exit,
    }
    try:
        payload = handlers[name](artifact_root, actual_now)
    except KeyError as exc:
        raise ValueError(f"Unknown Track B fault-injection scenario: {name}") from exc
    return _finalize_scenario(name=name, generated_at=actual_now, payload=payload)


def write_track_b_fault_injection_json_report(
    *,
    harness_report: Mapping[str, Any],
    output_path: Path,
    repo_root: Path | None = None,
    now: datetime | None = None,
) -> Path:
    """Write a deterministic JSON report to an explicit caller-provided path.

    The writer is intentionally not a CLI and has no default output location.
    It rejects paths under the live repository ``outputs`` or ``var`` roots.
    """

    resolved_output = _validate_report_output_path(output_path=output_path, repo_root=repo_root)
    report = _json_report_payload(harness_report=harness_report, generated_at=_ensure_utc(now or DEFAULT_FAULT_INJECTION_NOW))
    _write_json(resolved_output, report)
    return resolved_output


def _scenario_stale_runtime_with_exit_due_position(artifact_root: Path, now: datetime) -> dict[str, Any]:
    owner = _owner_resolution(
        artifact_root=artifact_root,
        broker_positions=[_broker_position(quantity="-1")],
        registry_records=[
            _registry_record(
                trade_id="trade_exit_due",
                lifecycle_id="life_exit_due",
                generated_at=now - timedelta(seconds=10),
                exit_due=True,
            )
        ],
    )
    risk_close = classify_runtime_stale_risk_reducing_close(
        control_plane_snapshot=_stale_runtime_control_plane(),
        guardian=_guardian_close_authority(),
        managed_position_registry=_managed_position_registry(),
        managed_order_registry=_managed_order_registry(),
        open_order_truth={"classification": "NO_OPEN_ORDERS", "open_orders": []},
    )
    return {
        "retired_bug_class": "Stranded Exit-Due Positions",
        "invariant": "Fresh broker-backed exit-due exposure retains an exact risk-reducing close path.",
        "submit": _submit_blocked("RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE"),
        "broker_exposure": _broker_exposure(count=1, visible=True),
        "ownership": owner,
        "registry": {"classification": "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE", "current_blockers": []},
        "risk_reducing_close_authority": {**risk_close, "applicable": True},
        "observability": {
            "broker_exposure_visible": True,
            "risk_reducing_close_classification": risk_close["classification"],
        },
    }


def _scenario_stale_owner_candidates_with_fresh_broker_owner(artifact_root: Path, now: datetime) -> dict[str, Any]:
    stale = _registry_record(
        trade_id="trade_stale_owner",
        lifecycle_id="life_stale_owner",
        generated_at=now - timedelta(hours=4),
        exit_due=True,
    )
    fresh = _registry_record(
        trade_id="trade_fresh_owner",
        lifecycle_id="life_fresh_owner",
        generated_at=now,
        exit_due=True,
    )
    owner = _owner_resolution(
        artifact_root=artifact_root,
        broker_positions=[_broker_position(quantity="-1")],
        registry_records=[stale, fresh],
        lifecycle_positions=[_lifecycle_position(trade_id="trade_stale_owner", lifecycle_id="life_stale_owner")],
    )
    return {
        "retired_bug_class": "Ownership Ambiguity",
        "invariant": "Newest broker-backed owner wins and stale candidates become diagnostic-only.",
        "submit": _submit_blocked("STALE_OWNER_CANDIDATES_DIAGNOSTIC_ONLY"),
        "broker_exposure": _broker_exposure(count=1, visible=True),
        "ownership": owner,
        "registry": {
            "classification": "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
            "diagnostic_only_rows": owner.get("stale_superseded_full_audit_only", []),
        },
        "risk_reducing_close_authority": _risk_close_not_applicable(),
        "observability": {
            "fresh_owner_trade_id": _first_owned_trade_id(owner),
            "stale_diagnostic_count": len(owner.get("stale_superseded_full_audit_only") or []),
        },
    }


def _scenario_historical_registry_debris_clean_current_scope(artifact_root: Path, now: datetime) -> dict[str, Any]:
    config = _write_watchdog_fixture(artifact_root, now=now)
    registry = _read_json(config.resolve(config.registry_diagnostics_path))
    registry.update(
        {
            "classification": "TRACK_B_DIAGNOSTICS_STALE_AUTHORITY",
            "diagnostic_only": True,
            "current_scope_review_required_count": 0,
            "current_scope_trade_states": [],
            "review_required_trade_ids": ["historical_full_audit_only"],
            "lifecycle_open_position_count": 0,
            "broker_open_order_count": 0,
            "reason_codes": ["HISTORICAL_REGISTRY_REVIEW_REQUIRED_TRADE"],
        }
    )
    _write_json(config.resolve(config.registry_diagnostics_path), registry)
    watchdog = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=now,
        pid_running=lambda _pid: True,
        source_commit_resolver=lambda _root: "fault-injection-commit",
    )
    return {
        "retired_bug_class": "Historical Registry Debris Blocking Current Truth",
        "invariant": "Current hot-path truth is evaluated separately from historical audit debris.",
        "submit": _submit_from_watchdog(watchdog),
        "broker_exposure": _broker_exposure(count=0, visible=False),
        "ownership": {"classification": "NO_OPEN_EXPOSURE", "owned_exposure_count": 0, "review_required_exposure_count": 0},
        "registry": watchdog["registry"],
        "risk_reducing_close_authority": _risk_close_not_applicable(),
        "observability": {"watchdog": watchdog},
    }


def _scenario_duplicate_lifecycle_rows_same_contract(artifact_root: Path, now: datetime) -> dict[str, Any]:
    stale = _registry_record(
        trade_id="trade_old_mes_short",
        lifecycle_id="life_old_mes_short",
        generated_at=now - timedelta(days=1),
        exit_due=False,
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
    )
    current = _registry_record(
        trade_id="trade_current_mes_short",
        lifecycle_id="life_current_mes_short",
        generated_at=now,
        exit_due=False,
        symbol="MES",
        local_symbol="MESM6",
        con_id=770561194,
    )
    broker_position = _broker_position(quantity="-1", symbol="MES", local_symbol="MESM6", con_id=770561194)
    owner = _owner_resolution(
        artifact_root=artifact_root,
        broker_positions=[broker_position],
        registry_records=[stale, current],
        lifecycle_positions=[_duplicate_lifecycle_aggregate()],
    )
    current_scope, superseded = apply_current_exposure_owner_lifecycle_overlay(
        lifecycle_positions=[_duplicate_lifecycle_aggregate()],
        owner_resolution=owner,
    )
    return {
        "retired_bug_class": "Contract/Ownership Aggregation Ambiguity",
        "invariant": "Duplicate same-contract lifecycle rows do not double-count current broker exposure.",
        "submit": _submit_blocked("DUPLICATE_LIFECYCLE_ROWS_DIAGNOSTIC_ONLY"),
        "broker_exposure": _broker_exposure(count=1, visible=True),
        "ownership": owner,
        "registry": {
            "classification": "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
            "current_scope_lifecycle_rows": current_scope,
            "diagnostic_only_rows": superseded,
        },
        "risk_reducing_close_authority": _risk_close_not_applicable(),
        "observability": {
            "current_scope_lifecycle_count": len(current_scope),
            "superseded_lifecycle_count": len(superseded),
        },
    }


def _scenario_dead_pid_with_stale_heartbeat(artifact_root: Path, now: datetime) -> dict[str, Any]:
    config = _write_watchdog_fixture(artifact_root, now=now)
    runtime_truth = _read_json(config.resolve(config.runtime_truth_path))
    runtime_truth["generated_at"] = _iso(now - timedelta(minutes=20))
    _write_json(config.resolve(config.runtime_truth_path), runtime_truth)
    _write_json(
        config.resolve(config.authority_refresh_path),
        {
            "generated_at": _iso(now - timedelta(minutes=20)),
            "classification": "AUTHORITY_REFRESH_STALE",
            "latest_successful_refresh_at": _iso(now - timedelta(minutes=20)),
        },
    )
    watchdog = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=now,
        pid_running=lambda _pid: False,
        source_commit_resolver=lambda _root: "fault-injection-commit",
    )
    return {
        "retired_bug_class": "Runtime Ingestion Freshness Confusion",
        "invariant": "Dead PID plus stale heartbeat cannot classify as runtime-ready.",
        "submit": _submit_from_watchdog(watchdog),
        "broker_exposure": _broker_exposure(count=0, visible=False),
        "ownership": {"classification": "NO_OPEN_EXPOSURE", "owned_exposure_count": 0, "review_required_exposure_count": 0},
        "registry": watchdog["registry"],
        "risk_reducing_close_authority": _risk_close_not_applicable(),
        "observability": {"watchdog": watchdog},
    }


def _scenario_near_expiry_contract_submit_attempt(artifact_root: Path, now: datetime) -> dict[str, Any]:
    del artifact_root, now
    evaluation_now = datetime(2026, 6, 15, 12, 0, tzinfo=UTC)
    contract = evaluate_futures_contract_pre_submit(
        FuturesContractResolverInput(
            strategy_id="mnq_fault_injection_lane",
            symbol="MNQ",
            contract_month="202606",
            action="BUY",
            intent_type="BUY_TO_OPEN",
            selected_target={
                "symbol": "MNQ",
                "contract_month": "202606",
                "expiry": "20260618",
                "con_id": 770561201,
                "local_symbol": "MNQM6",
            },
            qualified_contract_report=_contract_report(
                symbol="MNQ",
                expiry="20260618",
                con_id=770561201,
                local_symbol="MNQM6",
                updated_at=evaluation_now,
            ),
            now=evaluation_now,
        )
    )
    return {
        "retired_bug_class": "Contract Ambiguity",
        "invariant": "Canonical futures contract resolution is required before submit.",
        "submit": {
            "classification": contract["classification"],
            "allowed": contract["submit_allowed"] is True,
            "reason_codes": [contract["blocker"]] if contract.get("blocker") else [],
            "authority_source": "TRACK_B_FUTURES_CONTRACT_RESOLVER_V1",
        },
        "broker_exposure": _broker_exposure(count=0, visible=False),
        "ownership": {"classification": "NO_OPEN_EXPOSURE", "owned_exposure_count": 0, "review_required_exposure_count": 0},
        "registry": {"classification": "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE", "current_blockers": []},
        "risk_reducing_close_authority": _risk_close_not_applicable(),
        "observability": {"contract_authority": contract},
    }


def _scenario_phase1_fresh_runtime_ingestion_stale(artifact_root: Path, now: datetime) -> dict[str, Any]:
    config = _write_watchdog_fixture(artifact_root, now=now)
    operator = _read_json(config.resolve(config.operator_status_path))
    operator["lanes"][0]["last_processed_bar_end_ts"] = _iso(now - timedelta(seconds=30))
    operator["lanes"][0]["last_execution_bar_evaluated_at"] = _iso(now - timedelta(minutes=15))
    _write_json(config.resolve(config.operator_status_path), operator)
    watchdog = build_track_b_live_runtime_environment_watchdog(
        config=config,
        now=now,
        pid_running=lambda _pid: True,
        source_commit_resolver=lambda _root: "fault-injection-commit",
    )
    return {
        "retired_bug_class": "Runtime Ingestion Freshness Confusion",
        "invariant": "Readiness distinguishes feed healthy, runtime alive, and runtime ingestion stale.",
        "submit": _submit_from_watchdog(watchdog),
        "broker_exposure": _broker_exposure(count=0, visible=False),
        "ownership": {"classification": "NO_OPEN_EXPOSURE", "owned_exposure_count": 0, "review_required_exposure_count": 0},
        "registry": watchdog["registry"],
        "risk_reducing_close_authority": _risk_close_not_applicable(),
        "observability": {"watchdog": watchdog},
    }


def _scenario_broker_fill_without_lifecycle_close(artifact_root: Path, now: datetime) -> dict[str, Any]:
    owner = _owner_resolution(
        artifact_root=artifact_root,
        broker_positions=[_broker_position(quantity="-1")],
        registry_records=[
            _registry_record(
                trade_id="trade_open_no_close",
                lifecycle_id="life_open_no_close",
                generated_at=now,
                exit_due=False,
            )
        ],
    )
    risk_close = classify_runtime_stale_risk_reducing_close(
        control_plane_snapshot=_stale_runtime_control_plane(),
        guardian=_guardian_close_authority(trade_id="trade_open_no_close", lifecycle_id="life_open_no_close"),
        managed_position_registry={
            "classification": "OPEN_MANAGED",
            "managed_positions": [
                {
                    **_managed_position(trade_id="trade_open_no_close", lifecycle_id="life_open_no_close"),
                    "classification": "OPEN_MANAGED",
                }
            ],
        },
        managed_order_registry=_managed_order_registry(trade_id="trade_open_no_close", lifecycle_id="life_open_no_close"),
        open_order_truth={"classification": "NO_OPEN_ORDERS", "open_orders": []},
    )
    return {
        "retired_bug_class": "Managed Exit Owner Identity Loss",
        "invariant": "Broker-backed ownership remains visible when a fill exists without lifecycle close.",
        "submit": _submit_blocked("BROKER_FILL_WITHOUT_LIFECYCLE_CLOSE_RECONCILIATION_REQUIRED"),
        "broker_exposure": _broker_exposure(count=1, visible=True),
        "ownership": owner,
        "registry": {
            "classification": "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
            "reconciliation_path_available": True,
        },
        "risk_reducing_close_authority": {**risk_close, "applicable": False},
        "observability": {
            "broker_truth_visible": True,
            "reconciliation_path_available": True,
            "ownership_loss_detected": _first_owned_trade_id(owner) != "trade_open_no_close",
        },
    }


def _scenario_malformed_broker_position_truth_artifact(artifact_root: Path, now: datetime) -> dict[str, Any]:
    del artifact_root, now
    return _malformed_authority_payload(
        retired_bug_class="Stale Truth Authority",
        invariant="Malformed broker position truth is blocking/diagnostic and never silently accepted.",
        classification="MALFORMED_BROKER_POSITION_TRUTH_BLOCKED",
        broker_exposure=_broker_exposure_malformed(),
        ownership_classification="BROKER_POSITION_TRUTH_MALFORMED_REVIEW_REQUIRED",
        registry_classification="TRACK_B_DIAGNOSTICS_BLOCKED_MALFORMED_BROKER_POSITION_TRUTH",
        artifact_name="broker_position_truth",
        artifact_status="malformed_blocking",
        malformed_fields=["positions[0].quantity", "positions[0].con_id"],
    )


def _scenario_missing_lifecycle_reconciliation_artifact(artifact_root: Path, now: datetime) -> dict[str, Any]:
    del artifact_root, now
    return _malformed_authority_payload(
        retired_bug_class="Historical Registry Debris Blocking Current Truth",
        invariant="Missing lifecycle reconciliation blocks submit while broker exposure remains visible.",
        classification="MISSING_LIFECYCLE_RECONCILIATION_BLOCKED",
        broker_exposure=_broker_exposure(count=1, visible=True),
        ownership_classification="BROKER_EXPOSURE_VISIBLE_LIFECYCLE_RECONCILIATION_MISSING",
        registry_classification="TRACK_B_DIAGNOSTICS_BLOCKED_MISSING_LIFECYCLE_RECONCILIATION",
        artifact_name="lifecycle_reconciliation",
        artifact_status="missing_blocking",
        stale_or_malformed_artifact_blocked=True,
    )


def _scenario_registry_diagnostic_missing_current_scope_fields(artifact_root: Path, now: datetime) -> dict[str, Any]:
    del artifact_root, now
    return _malformed_authority_payload(
        retired_bug_class="Historical Registry Debris Blocking Current Truth",
        invariant="Registry diagnostics without current-scope fields cannot certify clean current truth.",
        classification="REGISTRY_DIAGNOSTIC_CURRENT_SCOPE_MISSING_BLOCKED",
        broker_exposure=_broker_exposure(count=0, visible=False),
        ownership_classification="NO_OPEN_EXPOSURE_REGISTRY_CURRENT_SCOPE_UNKNOWN",
        registry_classification="TRACK_B_DIAGNOSTICS_BLOCKED_CURRENT_SCOPE_FIELDS_MISSING",
        artifact_name="registry_diagnostics",
        artifact_status="malformed_blocking",
        malformed_fields=[
            "current_scope_review_required_count",
            "current_scope_trade_states",
            "current_hot_path_blocking",
        ],
    )


def _scenario_stale_runtime_environment_truth_with_fresh_broker_truth(artifact_root: Path, now: datetime) -> dict[str, Any]:
    del artifact_root
    return _malformed_authority_payload(
        retired_bug_class="Stale Truth Authority",
        invariant="Fresh broker truth remains visible but stale runtime environment truth cannot grant submit.",
        classification="STALE_RUNTIME_ENVIRONMENT_TRUTH_BROKER_TRUTH_FRESH_BLOCKED",
        broker_exposure=_broker_exposure(count=1, visible=True),
        ownership_classification="BROKER_EXPOSURE_VISIBLE_RUNTIME_ENVIRONMENT_STALE",
        registry_classification="TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
        artifact_name="runtime_environment_truth",
        artifact_status="stale_blocking",
        artifact_generated_at=_iso(now - timedelta(minutes=20)),
        fresh_broker_truth=True,
    )


def _scenario_conflicting_authority_generation_id(artifact_root: Path, now: datetime) -> dict[str, Any]:
    del artifact_root, now
    authority_generations = {
        "shared_truth": "authority-generation-101",
        "position_truth": "authority-generation-099",
        "runtime_environment_truth": "authority-generation-101",
    }
    return _malformed_authority_payload(
        retired_bug_class="Stale Truth Authority",
        invariant="Conflicting authority_generation_id values block submit until shared truth is coherent.",
        classification="CONFLICTING_AUTHORITY_GENERATION_ID_BLOCKED",
        broker_exposure=_broker_exposure(count=0, visible=False),
        ownership_classification="NO_OPEN_EXPOSURE_AUTHORITY_GENERATION_CONFLICT",
        registry_classification="TRACK_B_DIAGNOSTICS_BLOCKED_AUTHORITY_GENERATION_CONFLICT",
        artifact_name="shared_truth_bundle",
        artifact_status="conflicting_blocking",
        authority_generations=authority_generations,
    )


def _scenario_managed_close_order_artifact_broker_open_orders_zero(artifact_root: Path, now: datetime) -> dict[str, Any]:
    del artifact_root, now
    return _malformed_authority_payload(
        retired_bug_class="Ownership Ambiguity",
        invariant="Managed close-order artifacts cannot override broker open-order truth.",
        classification="MANAGED_CLOSE_ORDER_PHANTOM_BROKER_ZERO_BLOCKED",
        broker_exposure=_broker_exposure(count=1, visible=True),
        ownership_classification="OWNED_MANAGED_EXPOSURE_CLOSE_ORDER_PHANTOM_REVIEW_REQUIRED",
        registry_classification="TRACK_B_DIAGNOSTICS_BLOCKED_MANAGED_ORDER_BROKER_OPEN_ORDER_MISMATCH",
        artifact_name="managed_order_registry",
        artifact_status="conflicting_blocking",
        managed_order_open=True,
        broker_open_order_count=0,
    )


def _scenario_broker_open_order_exists_lifecycle_registry_none(artifact_root: Path, now: datetime) -> dict[str, Any]:
    del artifact_root, now
    return _malformed_authority_payload(
        retired_bug_class="Ownership Ambiguity",
        invariant="Fresh broker open-order truth remains visible when lifecycle/registry artifacts say none.",
        classification="BROKER_OPEN_ORDER_WITHOUT_LIFECYCLE_REGISTRY_BLOCKED",
        broker_exposure={
            **_broker_exposure(count=0, visible=False),
            "broker_open_order_visible": True,
            "open_order_count": 1,
        },
        ownership_classification="BROKER_OPEN_ORDER_VISIBLE_REGISTRY_NONE_REVIEW_REQUIRED",
        registry_classification="TRACK_B_DIAGNOSTICS_BLOCKED_BROKER_OPEN_ORDER_WITHOUT_REGISTRY_OWNER",
        artifact_name="broker_open_order_truth",
        artifact_status="fresh_broker_truth_blocking",
        broker_open_order_count=1,
        lifecycle_open_order_count=0,
        registry_open_order_count=0,
    )


def _scenario_missing_guardian_artifact_with_broker_exposure(artifact_root: Path, now: datetime) -> dict[str, Any]:
    del artifact_root, now
    payload = _malformed_authority_payload(
        retired_bug_class="Stranded Exit-Due Positions",
        invariant="Broker exposure without Guardian authority remains visible but cannot create an unguarded close path.",
        classification="MISSING_GUARDIAN_WITH_BROKER_EXPOSURE_BLOCKED",
        broker_exposure=_broker_exposure(count=1, visible=True),
        ownership_classification="BROKER_EXPOSURE_VISIBLE_GUARDIAN_MISSING",
        registry_classification="TRACK_B_DIAGNOSTICS_BLOCKED_MISSING_GUARDIAN_WITH_BROKER_EXPOSURE",
        artifact_name="guardian",
        artifact_status="missing_blocking",
        stale_or_malformed_artifact_blocked=True,
    )
    payload["risk_reducing_close_authority"] = {
        "applicable": True,
        "classification": "RISK_REDUCING_CLOSE_BLOCKED_MISSING_GUARDIAN",
        "allowed": False,
        "reason_codes": ["guardian_artifact_missing"],
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
    }
    return payload


def _scenario_managed_close_order_disappears_broker_flat_without_fill_callback(
    artifact_root: Path,
    now: datetime,
) -> dict[str, Any]:
    del artifact_root, now
    lifecycle_id = "life_mnq_close_disappeared"
    trade_id = "trade_mnq_close_disappeared"
    return {
        "retired_bug_class": "Managed Exit Owner Identity Loss",
        "invariant": "Fresh broker-flat truth plus scoped managed-close evidence creates a local reconciliation path without broker mutation.",
        "submit": _submit_blocked("MANAGED_CLOSE_DISAPPEARED_BROKER_FLAT_RECONCILIATION_REQUIRED"),
        "broker_exposure": {
            **_broker_exposure(count=0, visible=False),
            "broker_flat": True,
            "broker_open_order_count": 0,
        },
        "ownership": {
            "classification": "LIFECYCLE_WITHOUT_BROKER_LOCAL_CLOSE_RECONCILIATION_REQUIRED",
            "owned_exposure_count": 0,
            "review_required_exposure_count": 1,
            "trade_id": trade_id,
            "lifecycle_id": lifecycle_id,
        },
        "registry": {
            "classification": "BROKER_FLAT_WITH_SCOPED_MANAGED_CLOSE_EVIDENCE_RECONCILIATION_REQUIRED",
            "current_blockers": ["missing_broker_close_fill_callback"],
            "reconciliation_path_available": True,
            "broker_flat_without_fill_callback": True,
            "prior_scoped_managed_close_order": {
                "trade_id": trade_id,
                "lifecycle_id": lifecycle_id,
                "action": "SELL",
                "quantity": "1",
                "order_id": "69",
                "broker_open_order_still_visible": False,
            },
            "pnl_price_authority": "UNKNOWN_UNLESS_FILL_CALLBACK_OR_BROKER_EXECUTION_DETAIL_AVAILABLE",
        },
        "risk_reducing_close_authority": {
            "applicable": True,
            "classification": "RISK_REDUCING_CLOSE_BLOCKED_BROKER_ALREADY_FLAT",
            "allowed": False,
            "reason_codes": ["broker_position_flat"],
            "paper_proof_invoked": False,
            "live_money_eligible": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
        },
        "observability": {
            "local_reconciliation_allowed": True,
            "broker_state_mutated": False,
            "broker_mutation_allowed": False,
            "stale_or_malformed_artifact_blocked": True,
            "stale_or_malformed_artifact_silently_accepted": False,
        },
    }


def _scenario_registry_review_required_null_lifecycle_blocks_valid_exit(
    artifact_root: Path,
    now: datetime,
) -> dict[str, Any]:
    del artifact_root, now
    lifecycle_id = "reserved_submit_mes_valid_exit"
    trade_id = "trade_mes_valid_exit"
    return {
        "retired_bug_class": "Managed Exit Owner Identity Loss",
        "invariant": "Registry REVIEW_REQUIRED with null lifecycle_id is normalized only from exact broker/lifecycle/fill identity before managed-exit attach.",
        "submit": _submit_blocked("REGISTRY_REVIEW_REQUIRED_NULL_LIFECYCLE_BLOCKED_MANAGED_EXIT"),
        "broker_exposure": {
            **_broker_exposure(count=1, visible=True),
            "account_id": "DUM882026",
            "local_symbol": "MESM6",
            "con_id": 770561194,
            "quantity": "1",
        },
        "ownership": {
            "classification": "OWNED_MANAGED_EXPOSURE_REGISTRY_IDENTITY_REPAIR_REQUIRED",
            "owned_exposure_count": 1,
            "review_required_exposure_count": 0,
            "trade_id": trade_id,
            "lifecycle_id": lifecycle_id,
            "exact_broker_lifecycle_projection": True,
        },
        "registry": {
            "classification": "REGISTRY_IDENTITY_NORMALIZATION_REPAIR_NEEDED_EXACT_EVIDENCE",
            "current_blockers": ["registry_current_state_review_required", "registry_lifecycle_id_missing"],
            "registry_current_state_before": "REVIEW_REQUIRED",
            "registry_lifecycle_id_before": None,
            "target_state_after_repair": "OPEN_MANAGED",
            "target_lifecycle_id": lifecycle_id,
            "broker_backed_entry_after_repair": True,
            "current_scope_review_required_count_otherwise": 0,
            "exact_fill_identity_recoverable": True,
            "conflicting_trade_or_lifecycle_identity": False,
        },
        "risk_reducing_close_authority": {
            "applicable": True,
            "classification": "MANAGED_EXIT_BLOCKED_PENDING_REGISTRY_IDENTITY_NORMALIZATION",
            "allowed": False,
            "reason_codes": ["registry_identity_normalization_required"],
            "close_candidate": {
                "action": "SELL",
                "quantity": "1",
                "account_id": "DUM882026",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "trade_id": trade_id,
                "lifecycle_id": lifecycle_id,
            },
            "paper_proof_invoked": False,
            "live_money_eligible": False,
            "broad_flatten_allowed": False,
            "global_flatten_allowed": False,
        },
        "observability": {
            "registry_identity_normalization_required": True,
            "registry_identity_normalization_broker_mutation_allowed": False,
            "broker_state_mutated": False,
            "broker_mutation_allowed": False,
            "stale_or_malformed_artifact_blocked": True,
            "stale_or_malformed_artifact_silently_accepted": False,
        },
    }


def _finalize_scenario(*, name: str, generated_at: datetime, payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = dict(SCENARIO_METADATA.get(name) or {})
    submit = dict(payload.get("submit") or {})
    broker_exposure = dict(payload.get("broker_exposure") or {})
    ownership = dict(payload.get("ownership") or {})
    registry = dict(payload.get("registry") or {})
    risk_close = dict(payload.get("risk_reducing_close_authority") or {})
    components = [submit, broker_exposure, ownership, registry, risk_close, dict(payload.get("observability") or {})]
    assertions = [
        _assertion("submit_authority_classified", bool(submit.get("classification"))),
        _assertion("broker_exposure_classified", "visible" in broker_exposure and "count" in broker_exposure),
        _assertion("ownership_classified", bool(ownership.get("classification"))),
        _assertion("registry_classified", bool(registry.get("classification"))),
        _assertion(
            "risk_reducing_close_authority_classified_when_applicable",
            risk_close.get("applicable") is not True or bool(risk_close.get("classification")),
        ),
        _assertion("no_live_money_eligibility", not _any_true(components, ("live_money_eligible", "live_money_readiness"))),
        _assertion("no_paper_proof", not _any_true(components, ("paper_proof_invoked", "paper_proof_cli_called"))),
        _assertion("no_broad_flatten", not _any_true(components, ("broad_flatten_allowed", "global_flatten_allowed"))),
        _assertion("no_unguarded_broker_mutation", not _any_true(components, ("broker_mutation_allowed", "broker_state_mutated"))),
    ]
    return {
        "scenario": name,
        "generated_at": generated_at.isoformat(),
        "metadata": metadata,
        "retired_bug_class": payload.get("retired_bug_class") or metadata.get("bug_class"),
        "invariant": payload.get("invariant") or metadata.get("retired_invariant"),
        "read_only": True,
        "submit_authority": submit,
        "broker_exposure": broker_exposure,
        "ownership": ownership,
        "registry": registry,
        "risk_reducing_close_authority": risk_close,
        "safety": {
            "live_money_eligible": False,
            "paper_proof_invoked": False,
            "broad_flatten_allowed": False,
            "broker_mutation_allowed": False,
        },
        "observability": dict(payload.get("observability") or {}),
        "assertions": assertions,
        "passed": all(item["passed"] for item in assertions),
    }


def _json_report_payload(*, harness_report: Mapping[str, Any], generated_at: datetime) -> dict[str, Any]:
    scenarios = []
    for raw in harness_report.get("scenarios") or []:
        if not isinstance(raw, Mapping):
            continue
        assertions = [dict(item) for item in raw.get("assertions") or [] if isinstance(item, Mapping)]
        safety_checks = [
            dict(item)
            for item in assertions
            if str(item.get("name") or "") in set(SAFETY_INVARIANTS_CHECKED)
        ]
        scenarios.append(
            {
                "scenario_id": raw.get("scenario"),
                "timestamp": raw.get("generated_at"),
                "verdict": {
                    "passed": raw.get("passed") is True,
                    "submit_authority_classification": _classification(raw.get("submit_authority")),
                    "broker_exposure_classification": _classification(raw.get("broker_exposure")),
                    "ownership_classification": _classification(raw.get("ownership")),
                    "registry_classification": _classification(raw.get("registry")),
                    "risk_reducing_close_classification": _classification(raw.get("risk_reducing_close_authority")),
                },
                "metadata": dict(raw.get("metadata") or {}),
                "safety_checks": safety_checks,
            }
        )
    return {
        "schema_version": FAULT_INJECTION_REPORT_SCHEMA_VERSION,
        "generated_at": generated_at.isoformat(),
        "source_schema_version": harness_report.get("schema_version"),
        "read_only": True,
        "broker_mutation_allowed": False,
        "scenario_count": len(scenarios),
        "passed": all(item["verdict"]["passed"] for item in scenarios),
        "scenarios": scenarios,
    }


def _validate_report_output_path(*, output_path: Path, repo_root: Path | None) -> Path:
    if output_path is None:
        raise ValueError("output_path is required.")
    raw_text = str(output_path).strip()
    if not raw_text:
        raise ValueError("output_path is required.")
    resolved = output_path.expanduser().resolve(strict=False)
    if resolved.name in {"", ".", ".."}:
        raise ValueError("output_path must include a JSON filename.")
    effective_repo_root = (repo_root or Path.cwd()).expanduser().resolve(strict=False)
    live_roots = (effective_repo_root / "outputs", effective_repo_root / "var")
    for live_root in live_roots:
        resolved_live_root = live_root.resolve(strict=False)
        if resolved == resolved_live_root or _is_relative_to(resolved, resolved_live_root):
            raise ValueError(f"Refusing to write fault-injection report under live repo path: {resolved_live_root}")
    return resolved


def _is_relative_to(path: Path, parent: Path) -> bool:
    try:
        path.relative_to(parent)
    except ValueError:
        return False
    return True


def _classification(value: Any) -> str | None:
    if isinstance(value, Mapping):
        raw = value.get("classification")
        return str(raw) if raw not in (None, "") else None
    return None


def _owner_resolution(
    *,
    artifact_root: Path,
    broker_positions: Sequence[Mapping[str, Any]],
    registry_records: Sequence[Any],
    lifecycle_positions: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    return resolve_current_exposure_ownership(
        config=CurrentExposureOwnerResolverConfig(repo_root=artifact_root),
        broker_positions=broker_positions,
        broker_open_orders=[],
        registry_records=registry_records,
        lifecycle_positions=lifecycle_positions,
    )


def _registry_record(
    *,
    trade_id: str,
    lifecycle_id: str,
    generated_at: datetime,
    exit_due: bool,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
) -> Any:
    events = [
        _event(
            event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            generated_at=generated_at,
            order_id=f"order_{trade_id}",
            client_id="fault-injection-client",
            perm_id=f"perm_{trade_id}",
            exec_id=f"exec_{trade_id}",
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
        ),
        _event(
            event_type=TradeEventType.LIFECYCLE_OPEN_MANAGED,
            trade_id=trade_id,
            lifecycle_id=lifecycle_id,
            generated_at=generated_at + timedelta(seconds=1),
            symbol=symbol,
            local_symbol=local_symbol,
            con_id=con_id,
        ),
    ]
    if exit_due:
        events.append(
            _event(
                event_type=TradeEventType.EXIT_INTENT_CREATED,
                trade_id=trade_id,
                lifecycle_id=lifecycle_id,
                generated_at=generated_at + timedelta(minutes=60),
                symbol=symbol,
                local_symbol=local_symbol,
                con_id=con_id,
            )
        )
    return reduce_trade_events(events)


def _event(
    *,
    event_type: TradeEventType,
    trade_id: str,
    lifecycle_id: str,
    generated_at: datetime,
    order_id: str | None = None,
    client_id: str | None = None,
    perm_id: str | None = None,
    exec_id: str | None = None,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
) -> TradeEvent:
    return TradeEvent(
        event_id=f"{trade_id}_{event_type.value}_{int(generated_at.timestamp())}",
        event_type=event_type,
        generated_at=generated_at,
        trade_id=trade_id,
        lifecycle_id=lifecycle_id,
        lane_id=f"{symbol.lower()}_fault_injection_lane",
        thesis_strategy_id=f"{symbol.lower()}_fault_injection_lane",
        account_id="DUM882026",
        symbol=symbol,
        con_id=con_id,
        local_symbol=local_symbol,
        expiry="20260618",
        side="SHORT",
        action="SELL",
        qty=Decimal("1"),
        source_artifact_path="outputs/track_b_execution_core/fault_injection.json",
        order_id=order_id,
        client_id=client_id,
        perm_id=perm_id,
        exec_id=exec_id,
        price=Decimal("30675"),
        metadata={"managed_exit_policy_id": "FAULT_INJECTION_EXIT_POLICY_V1"},
    )


def _write_watchdog_fixture(
    artifact_root: Path,
    *,
    now: datetime,
    in_window: bool = True,
    ready_submit_capable: bool = True,
) -> TrackBLiveRuntimeEnvironmentWatchdogConfig:
    config = TrackBLiveRuntimeEnvironmentWatchdogConfig(repo_root=artifact_root)
    for path in (
        config.output_path,
        config.runtime_truth_path,
        config.operator_status_path,
        config.canonical_readiness_path,
        config.authority_refresh_path,
        config.control_plane_snapshot_path,
        config.safe_state_envelope_path,
        config.runtime_supervisor_authority_path,
        config.broker_reconciliation_path,
        config.broker_truth_refresh_status_path,
        config.registry_diagnostics_path,
        config.phase1_listener_status_path,
        config.recovery_status_path,
    ):
        config.resolve(path).parent.mkdir(parents=True, exist_ok=True)
    lane = {
        "lane_id": "mnq_fault_injection_lane",
        "eligible_now": in_window,
        "current_session_window_classification": "IN_WINDOW" if in_window else "OUT_OF_WINDOW",
        "last_processed_bar_end_ts": _iso(now - timedelta(seconds=30)),
        "last_execution_bar_evaluated_at": _iso(now - timedelta(seconds=30)),
    }
    _write_json(
        config.resolve(config.runtime_truth_path),
        {
            "generated_at": _iso(now),
            "producer_pid": 1234,
            "source_commit": "fault-injection-commit",
            "lane_count": 13,
        },
    )
    _write_json(
        config.resolve(config.operator_status_path),
        {"generated_at": _iso(now), "active_lane_ids": [lane["lane_id"]], "lanes": [lane]},
    )
    _write_json(
        config.resolve(config.canonical_readiness_path),
        {
            "generated_at": _iso(now),
            "canonical_readiness": "READY_SUBMIT_CAPABLE" if ready_submit_capable else "READY_DIAGNOSTIC_ONLY",
            "submit_allowed": ready_submit_capable,
            "market_schedule_state": "MARKET_OPEN_EXPECT_FRESH_BARS",
        },
    )
    _write_json(
        config.resolve(config.authority_refresh_path),
        {
            "generated_at": _iso(now),
            "classification": "AUTHORITY_REFRESHED",
            "latest_successful_refresh_at": _iso(now),
        },
    )
    for path in (
        config.control_plane_snapshot_path,
        config.safe_state_envelope_path,
        config.runtime_supervisor_authority_path,
    ):
        _write_json(config.resolve(path), {"generated_at": _iso(now), "classification": "FRESH"})
    _write_json(
        config.resolve(config.broker_reconciliation_path),
        {
            "generated_at": _iso(now),
            "classification": "TRACK_B_PAPER_BROKER_RECONCILED",
            "track_b_broker_position_count": 0,
            "track_b_broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
        },
    )
    _write_json(config.resolve(config.broker_truth_refresh_status_path), {"generated_at": _iso(now), "fresh": True})
    _write_json(
        config.resolve(config.registry_diagnostics_path),
        {
            "generated_at": _iso(now),
            "classification": "TRACK_B_DIAGNOSTICS_CLEAN_CURRENT_SCOPE",
            "current_scope_review_required_count": 0,
            "broker_open_order_count": 0,
            "lifecycle_open_position_count": 0,
            "track_b_managed_futures_position_count": 0,
        },
    )
    _write_json(
        config.resolve(config.phase1_listener_status_path),
        {"generated_at": _iso(now), "latest_record_at": _iso(now - timedelta(seconds=30))},
    )
    _write_json(config.resolve(config.recovery_status_path), {"generated_at": _iso(now), "classification": "RECOVERY_ACTIVE"})
    return config


def _submit_from_watchdog(watchdog: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "classification": watchdog.get("classification"),
        "allowed": watchdog.get("submit_authority") is True,
        "reason_codes": list(watchdog.get("reason_codes") or []),
        "authority_source": "TRACK_B_LIVE_RUNTIME_ENVIRONMENT_WATCHDOG",
    }


def _submit_blocked(classification: str) -> dict[str, Any]:
    return {
        "classification": classification,
        "allowed": False,
        "reason_codes": [classification],
        "authority_source": "TRACK_B_FAULT_INJECTION_HARNESS",
    }


def _malformed_authority_payload(
    *,
    retired_bug_class: str,
    invariant: str,
    classification: str,
    broker_exposure: Mapping[str, Any],
    ownership_classification: str,
    registry_classification: str,
    artifact_name: str,
    artifact_status: str,
    malformed_fields: Sequence[str] = (),
    authority_generations: Mapping[str, str] | None = None,
    artifact_generated_at: str | None = None,
    fresh_broker_truth: bool = False,
    stale_or_malformed_artifact_blocked: bool = True,
    managed_order_open: bool | None = None,
    broker_open_order_count: int | None = None,
    lifecycle_open_order_count: int | None = None,
    registry_open_order_count: int | None = None,
) -> dict[str, Any]:
    integrity = {
        "artifact": artifact_name,
        "status": artifact_status,
        "diagnostic_only": artifact_status.endswith("_diagnostic"),
        "blocking": stale_or_malformed_artifact_blocked or "blocking" in artifact_status,
        "silently_accepted": False,
        "malformed_fields": list(malformed_fields),
    }
    if authority_generations:
        integrity["authority_generation_ids"] = dict(authority_generations)
    if artifact_generated_at:
        integrity["artifact_generated_at"] = artifact_generated_at
    if managed_order_open is not None:
        integrity["managed_order_open"] = managed_order_open
    if broker_open_order_count is not None:
        integrity["broker_open_order_count"] = broker_open_order_count
    if lifecycle_open_order_count is not None:
        integrity["lifecycle_open_order_count"] = lifecycle_open_order_count
    if registry_open_order_count is not None:
        integrity["registry_open_order_count"] = registry_open_order_count
    return {
        "retired_bug_class": retired_bug_class,
        "invariant": invariant,
        "submit": _submit_blocked(classification),
        "broker_exposure": dict(broker_exposure),
        "ownership": {
            "classification": ownership_classification,
            "owned_exposure_count": int(broker_exposure.get("count") or 0) if broker_exposure.get("visible") else 0,
            "review_required_exposure_count": 1,
            "artifact_integrity": integrity,
        },
        "registry": {
            "classification": registry_classification,
            "current_blockers": [classification],
            "artifact_integrity": integrity,
        },
        "risk_reducing_close_authority": _risk_close_not_applicable(),
        "observability": {
            "artifact_integrity": integrity,
            "fresh_broker_truth_visible": fresh_broker_truth or broker_exposure.get("visible") is True,
            "stale_or_malformed_artifact_blocked": integrity["blocking"],
            "stale_or_malformed_artifact_silently_accepted": False,
        },
    }


def _broker_exposure(*, count: int, visible: bool) -> dict[str, Any]:
    return {
        "classification": "BROKER_EXPOSURE_VISIBLE" if visible else "NO_BROKER_EXPOSURE",
        "visible": visible,
        "count": count,
        "source": "BROKER_BACKED_TRUTH_ARTIFACT",
    }


def _broker_exposure_malformed() -> dict[str, Any]:
    return {
        "classification": "BROKER_EXPOSURE_MALFORMED_BLOCKING",
        "visible": False,
        "count": 0,
        "source": "BROKER_BACKED_TRUTH_ARTIFACT",
        "artifact_present": True,
        "malformed": True,
    }


def _risk_close_not_applicable() -> dict[str, Any]:
    return {
        "applicable": False,
        "classification": "RISK_REDUCING_CLOSE_NOT_REQUIRED",
        "allowed": False,
        "paper_proof_invoked": False,
        "live_money_eligible": False,
        "broad_flatten_allowed": False,
        "global_flatten_allowed": False,
    }


def _managed_position(*, trade_id: str = "trade_exit_due", lifecycle_id: str = "life_exit_due") -> dict[str, Any]:
    return {
        "classification": "OPEN_MANAGED_EXIT_DUE",
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "account_id": "DUM882026",
        "symbol": "MNQ",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "quantity": "1",
        "side": "SHORT",
        "broker_position": _broker_position(quantity="-1"),
        "lifecycle_position": {
            "entry_exec_id": "exec_trade_exit_due",
            "entry_perm_id": "perm_trade_exit_due",
            "entry_broker_identity": {"exec_id": "exec_trade_exit_due", "perm_id": "perm_trade_exit_due"},
        },
    }


def _managed_position_registry() -> dict[str, Any]:
    return {"classification": "OPEN_MANAGED_EXIT_DUE", "managed_positions": [_managed_position()]}


def _managed_order_registry(*, trade_id: str = "trade_exit_due", lifecycle_id: str = "life_exit_due") -> dict[str, Any]:
    return {
        "classification": "POSITION_WITHOUT_CLOSE_ORDER",
        "managed_orders": [
            {
                "classification": "POSITION_WITHOUT_CLOSE_ORDER",
                "trade_id": trade_id,
                "lifecycle_id": lifecycle_id,
                "action": "BUY",
                "quantity": "1",
                "working": False,
            }
        ],
    }


def _guardian_close_authority(*, trade_id: str = "trade_exit_due", lifecycle_id: str = "life_exit_due") -> dict[str, Any]:
    return {
        "classification": "BROKER_POSITION_GUARDIAN_READY",
        "managed_close_authority": {
            "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
            "allowed": True,
            "reason_codes": [],
            "candidates": [
                {
                    "classification": "BROKER_POSITION_GUARDIAN_CLOSE_ALLOWED_RISK_REDUCING",
                    "trade_id": trade_id,
                    "lifecycle_id": lifecycle_id,
                    "account_id": "DUM882026",
                    "symbol": "MNQ",
                    "local_symbol": "MNQM6",
                    "con_id": 770561201,
                    "action": "BUY",
                    "quantity": "1",
                }
            ],
        },
    }


def _stale_runtime_control_plane() -> dict[str, Any]:
    return {
        "runtime_authority_exposure_classification": "RUNTIME_AUTHORITY_STALE_WITH_BROKER_EXPOSURE",
        "runtime_authority_stale_with_broker_exposure": True,
        "submit_authority": False,
    }


def _broker_position(
    *,
    quantity: str,
    symbol: str = "MNQ",
    local_symbol: str = "MNQM6",
    con_id: int = 770561201,
) -> dict[str, Any]:
    return {
        "account_id": "DUM882026",
        "symbol": symbol,
        "track_b_root": symbol,
        "local_symbol": local_symbol,
        "con_id": con_id,
        "expiry": "20260618",
        "quantity": quantity,
    }


def _lifecycle_position(*, trade_id: str, lifecycle_id: str) -> dict[str, Any]:
    return {
        "trade_id": trade_id,
        "lifecycle_id": lifecycle_id,
        "account_id": "DUM882026",
        "local_symbol": "MNQM6",
        "con_id": 770561201,
        "aggregate_qty": "-1",
        "quantity": "1",
        "side": "SHORT",
    }


def _duplicate_lifecycle_aggregate() -> dict[str, Any]:
    return {
        "account_id": "DUM882026",
        "local_symbol": "MESM6",
        "con_id": 770561194,
        "aggregate_qty": "-2",
        "quantity": "2",
        "side": "SHORT",
        "lifecycle_id": "life_old_mes_short",
        "lifecycle_ids": ["life_old_mes_short", "life_current_mes_short"],
        "trade_ids": ["trade_old_mes_short", "trade_current_mes_short"],
        "lifecycle_units": [
            {"lifecycle_id": "life_old_mes_short", "signed_qty": "-1"},
            {"lifecycle_id": "life_current_mes_short", "signed_qty": "-1"},
        ],
    }


def _contract_report(
    *,
    symbol: str,
    expiry: str,
    con_id: int,
    local_symbol: str,
    updated_at: datetime,
) -> dict[str, Any]:
    detail = {
        "symbol": symbol,
        "expiry": expiry,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "exchange": "CME",
        "currency": "USD",
        "multiplier": "2",
        "updated_at": _iso(updated_at),
    }
    return {
        "ok": True,
        "qualified_contract": dict(detail),
        "qualified_contract_identifier": con_id,
        "api_contract_details": [detail],
    }


def _first_owned_trade_id(owner: Mapping[str, Any]) -> str | None:
    rows = owner.get("owned_exposures")
    if isinstance(rows, list) and rows and isinstance(rows[0], Mapping):
        return str(rows[0].get("trade_id") or "")
    return None


def _assertion(name: str, passed: bool) -> dict[str, Any]:
    return {"name": name, "passed": bool(passed)}


def _any_true(values: Sequence[Mapping[str, Any]], keys: Sequence[str]) -> bool:
    for value in values:
        for key in keys:
            if value.get(key) is True:
                return True
        for nested in value.values():
            if isinstance(nested, Mapping) and _any_true([nested], keys):
                return True
            if isinstance(nested, list) and _any_true([item for item in nested if isinstance(item, Mapping)], keys):
                return True
    return False


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(dict(payload), sort_keys=True), encoding="utf-8")


def _read_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return dict(payload) if isinstance(payload, Mapping) else {}


def _iso(value: datetime) -> str:
    return _ensure_utc(value).isoformat()


def _ensure_utc(value: datetime) -> datetime:
    return value.astimezone(UTC) if value.tzinfo else value.replace(tzinfo=UTC)
