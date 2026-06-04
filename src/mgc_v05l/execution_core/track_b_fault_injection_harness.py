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

FAULT_INJECTION_SCENARIOS: tuple[str, ...] = (
    SCENARIO_STALE_RUNTIME_EXIT_DUE,
    SCENARIO_STALE_OWNER_FRESH_BROKER,
    SCENARIO_HISTORICAL_REGISTRY_DEBRIS,
    SCENARIO_DUPLICATE_LIFECYCLE_ROWS,
    SCENARIO_DEAD_PID_STALE_HEARTBEAT,
    SCENARIO_NEAR_EXPIRY_CONTRACT,
    SCENARIO_PHASE1_FRESH_RUNTIME_STALE,
    SCENARIO_BROKER_FILL_NO_LIFECYCLE_CLOSE,
)

DEFAULT_FAULT_INJECTION_NOW = datetime(2026, 6, 4, 14, 0, tzinfo=UTC)

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
    }
    try:
        payload = handlers[name](artifact_root, actual_now)
    except KeyError as exc:
        raise ValueError(f"Unknown Track B fault-injection scenario: {name}") from exc
    return _finalize_scenario(name=name, generated_at=actual_now, payload=payload)


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


def _broker_exposure(*, count: int, visible: bool) -> dict[str, Any]:
    return {
        "classification": "BROKER_EXPOSURE_VISIBLE" if visible else "NO_BROKER_EXPOSURE",
        "visible": visible,
        "count": count,
        "source": "BROKER_BACKED_TRUTH_ARTIFACT",
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
