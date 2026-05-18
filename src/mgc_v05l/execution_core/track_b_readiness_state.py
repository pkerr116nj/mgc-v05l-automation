"""Canonical Track B PAPER readiness state and root/process guard.

This module is intentionally independent of the operator dashboard server. It
only reads local artifacts/process metadata and can be used by CLIs, launch
scripts, watchdogs, or the dashboard as a consumer.
"""

from __future__ import annotations

import argparse
import errno
import json
import os
import subprocess
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.execution_core.track_b_live_market_data_symbols import (
    DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH,
    TrackBLiveMarketDataSymbol,
    load_track_b_live_market_data_symbols,
)

REPO_ROOT = Path(__file__).resolve().parents[3]
TRACK_B_EXPECTED_ACTIVE_ROOT_ENV = "MGC_TRACK_B_EXPECTED_ACTIVE_ROOT"
DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT = Path("/Users/patrick/Dev/MGC-v05l-automation")
DEFAULT_CANONICAL_READINESS_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
)
DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
)
DEFAULT_PHASE1_DATABENTO_LIVE_LISTENER_STATUS_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "phase1_databento_live_runtime_candles"
    / "latest_phase1_databento_live_listener_status.json"
)
CANONICAL_READINESS_STATES = {
    "READY_SUBMIT_CAPABLE",
    "READY_OBSERVATION_ONLY",
    "DEGRADED_NO_SUBMIT",
    "NOT_READY_DEPENDENCY",
    "NOT_READY_RECONCILIATION",
    "NOT_READY_CONFIG",
    "NOT_READY_WRONG_ROOT",
}
BROKER_FRESHNESS_DEFAULT_SECONDS = 150.0
RECONCILIATION_FRESHNESS_DEFAULT_SECONDS = 180.0
MARKET_DATA_FRESHNESS_DEFAULT_SECONDS = 180.0
MARKET_DATA_REQUIRED_SOURCE = "DATABENTO_REALTIME_PHASE1"
BROKER_TRUTH_LEASE_READY_STATES = {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"}
BROKER_TRUTH_LEASE_EXPIRED_STATES = {"EXPIRED_BLOCK_NEW_ENTRIES", "EXPIRED_EXITS_ONLY"}


def build_canonical_readiness(
    *,
    repo_root: Path = REPO_ROOT,
    expected_root: Path | None = None,
    now: datetime | None = None,
    process_cwd_resolver: Callable[[int], Path | None] | None = None,
) -> dict[str, Any]:
    """Build canonical readiness from local Dev-root artifacts only."""

    repo_root = repo_root.resolve()
    expected_root = _expected_root(expected_root).resolve()
    now = _ensure_utc(now or datetime.now(timezone.utc))
    artifacts = _load_readiness_artifacts(repo_root)
    root_guard = build_root_process_guard(
        repo_root=repo_root,
        expected_root=expected_root,
        artifacts=artifacts,
        process_cwd_resolver=process_cwd_resolver,
        now=now,
    )
    inputs = build_readiness_inputs(
        repo_root=repo_root,
        expected_root=expected_root,
        artifacts=artifacts,
        root_guard=root_guard,
        now=now,
    )
    return classify_canonical_readiness(inputs)


def classify_canonical_readiness(inputs: Mapping[str, Any]) -> dict[str, Any]:
    """Pure canonical readiness classifier.

    The classifier is PAPER-only and never grants live-money eligibility. It
    returns one state plus reason/blocker/warning lists.
    """

    generated_at = str(inputs.get("generated_at") or datetime.now(timezone.utc).isoformat())
    root_guard = _mapping(inputs.get("root_guard_summary"))
    broker_truth = _mapping(inputs.get("broker_truth"))
    broker_truth_lease = _mapping(inputs.get("broker_truth_lease"))
    latest_attempt = _mapping(broker_truth.get("latest_attempt_status"))
    reconciliation = _mapping(inputs.get("phase1_reconciliation"))
    runtime = _mapping(inputs.get("runtime"))
    backend = _mapping(inputs.get("backend"))
    market_data = _mapping(inputs.get("market_data"))
    lane_quarantine = _mapping(inputs.get("lane_quarantine"))
    submit_bridge = _mapping(inputs.get("submit_bridge"))

    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    reasons: list[str] = []

    def block(code: str, detail: str, *, source: str | None = None) -> None:
        blockers.append({"code": code, "detail": detail, "source": source})
        reasons.append(detail)

    def warn(code: str, detail: str, *, source: str | None = None) -> None:
        warnings.append({"code": code, "detail": detail, "source": source})

    if root_guard.get("root_match") is False:
        block(
            "wrong_root_process",
            "One or more active Track B processes are not running from the expected Dev root.",
            source="root_guard",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_WRONG_ROOT",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )
    if root_guard.get("unknown_root_processes"):
        warn(
            "process_root_unknown",
            "One or more active Track B process roots could not be determined; wrong-root blocking applies only to confirmed mismatches.",
            source="root_guard",
        )

    if (
        _bool(inputs.get("live_money_eligible"))
        or _bool(broker_truth.get("live_money_eligible"))
        or _bool(broker_truth_lease.get("live_money_eligible"))
        or _bool(reconciliation.get("live_money_eligible"))
    ):
        block(
            "live_money_eligible_true",
            "live_money_eligible=true is forbidden for Track B PAPER readiness.",
            source="paper_safety",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_CONFIG",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    broker_ambiguous = (
        not _bool(broker_truth.get("available"))
        or not _bool(broker_truth.get("fresh"))
        or not _bool(broker_truth.get("positions_complete"))
        or not _bool(broker_truth.get("open_orders_complete"))
    )
    latest_attempt_failed = _bool(latest_attempt.get("last_failure")) or str(
        latest_attempt.get("classification") or ""
    ).upper().endswith("FAILED")
    if latest_attempt_failed:
        warn(
            "latest_broker_attempt_failed",
            "Latest broker-truth attempt failed; preserved last-good truth is used only while fresh.",
            source="broker_truth",
        )
    broker_lease_available = _bool(broker_truth_lease.get("available"))
    broker_lease_state = str(broker_truth_lease.get("lease_state") or "").upper()
    broker_lease_satisfies_dependency = False
    if broker_lease_available:
        if broker_lease_state in BROKER_TRUTH_LEASE_READY_STATES:
            broker_lease_satisfies_dependency = True
            if broker_lease_state == "ACTIVE_DEGRADED_REFRESH_FAILING":
                warn(
                    "broker_truth_lease_degraded_refresh_failing",
                    "Broker-truth lease remains valid while latest broker refresh attempts are failing.",
                    source="broker_truth_lease",
                )
        elif broker_lease_state in BROKER_TRUTH_LEASE_EXPIRED_STATES:
            block(
                "broker_truth_lease_expired",
                "Broker-truth lease is expired; submit-capable PAPER readiness is blocked.",
                source="broker_truth_lease",
            )
            return _readiness_result(
                generated_at=generated_at,
                state="NOT_READY_DEPENDENCY",
                reasons=reasons,
                blockers=blockers,
                warnings=warnings,
                inputs=inputs,
            )
        elif broker_lease_state == "INVALIDATED_UNKNOWN_OPEN_ORDERS":
            block(
                "broker_truth_lease_unknown_open_orders",
                "Broker-truth lease is invalidated by unknown open orders.",
                source="broker_truth_lease",
            )
            return _readiness_result(
                generated_at=generated_at,
                state="NOT_READY_DEPENDENCY",
                reasons=reasons,
                blockers=blockers,
                warnings=warnings,
                inputs=inputs,
            )
        elif broker_lease_state.startswith("INVALIDATED_"):
            block(
                "broker_truth_lease_invalidated",
                "Broker-truth lease is invalidated by a broker/lifecycle contradiction.",
                source="broker_truth_lease",
            )
            return _readiness_result(
                generated_at=generated_at,
                state="NOT_READY_DEPENDENCY",
                reasons=reasons,
                blockers=blockers,
                warnings=warnings,
                inputs=inputs,
            )
        elif broker_lease_state == "OPERATOR_REQUIRED":
            block(
                "broker_truth_lease_operator_required",
                "Broker-truth lease requires operator review before submit-capable PAPER readiness.",
                source="broker_truth_lease",
            )
            return _readiness_result(
                generated_at=generated_at,
                state="NOT_READY_DEPENDENCY",
                reasons=reasons,
                blockers=blockers,
                warnings=warnings,
                inputs=inputs,
            )
        else:
            block(
                "broker_truth_lease_unknown_state",
                "Broker-truth lease artifact has an unknown state; preserving fail-closed readiness.",
                source="broker_truth_lease",
            )
            return _readiness_result(
                generated_at=generated_at,
                state="NOT_READY_DEPENDENCY",
                reasons=reasons,
                blockers=blockers,
                warnings=warnings,
                inputs=inputs,
            )
    if broker_ambiguous and not broker_lease_satisfies_dependency:
        block(
            "broker_truth_not_fresh_or_complete",
            "Fresh complete broker truth is required before Track B PAPER submit capability.",
            source="broker_truth",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_DEPENDENCY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    if (
        not _bool(reconciliation.get("available"))
        or not _bool(reconciliation.get("fresh"))
        or str(reconciliation.get("classification") or "") != "TRACK_B_PAPER_BROKER_RECONCILED"
        or not _bool(reconciliation.get("broker_reconciled"))
        or int(reconciliation.get("review_required_count") or 0) != 0
        or int(reconciliation.get("lifecycle_open_position_count") or 0) != 0
    ):
        block(
            "phase1_reconciliation_not_clean",
            "Fresh clean Phase-1 PAPER broker reconciliation is required before submit capability.",
            source="phase1_reconciliation",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_RECONCILIATION",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    if not _bool(backend.get("healthy")):
        warn(
            "backend_not_healthy",
            "Dashboard/backend is not healthy; classifier remains authoritative from local artifacts.",
            source="backend",
        )

    runtime_running = _bool(runtime.get("running"))
    runtime_healthy = _bool(runtime.get("healthy"))
    loaded_lane_count = int(runtime.get("loaded_lane_count") or 0)
    eligible_lane_count = int(runtime.get("eligible_lane_count") or 0)
    live_bars_fresh = _bool(market_data.get("fresh"))
    submit_route_ready = _bool(submit_bridge.get("submit_route_ready"))
    submit_authority_explicit = _bool(submit_bridge.get("submit_authority_explicit"))
    quarantine_count = int(lane_quarantine.get("quarantine_count") or 0)

    for row in list(market_data.get("warnings") or []):
        if isinstance(row, Mapping):
            warn(
                str(row.get("code") or "market_data_warning"),
                str(row.get("detail") or "Market-data warning."),
                source=str(row.get("source") or "market_data"),
            )

    if quarantine_count > 0:
        warn(
            "lane_quarantine_active",
            f"{quarantine_count} lane(s) are quarantined; quarantined lanes are excluded from submit eligibility.",
            source="lane_quarantine",
        )

    if not runtime_running:
        reasons.append("Dependencies are clean, but the PAPER runtime is not running.")
        return _readiness_result(
            generated_at=generated_at,
            state="READY_OBSERVATION_ONLY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    if not runtime_healthy:
        block(
            "runtime_not_healthy",
            "PAPER runtime is present but not healthy.",
            source="runtime",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_DEPENDENCY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    if not live_bars_fresh:
        market_data_blockers = [row for row in list(market_data.get("blockers") or []) if isinstance(row, Mapping)]
        if market_data_blockers:
            primary_market_data_blocker = market_data_blockers[0]
            code = str(primary_market_data_blocker.get("code") or "market_data_not_fresh")
            detail = str(primary_market_data_blocker.get("detail") or "Runtime market-data/live-bar freshness is not current.")
        else:
            code = "market_data_not_fresh"
            detail = "Runtime market-data/live-bar freshness is not current."
        block(
            code,
            detail,
            source="market_data",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_DEPENDENCY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    if loaded_lane_count <= 0 or eligible_lane_count <= 0:
        reasons.append("PAPER runtime is healthy, but no lane is currently eligible for submit.")
        return _readiness_result(
            generated_at=generated_at,
            state="READY_OBSERVATION_ONLY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    if submit_route_ready and submit_authority_explicit:
        reasons.append("PAPER runtime, broker truth, reconciliation, live bars, and submit route are ready.")
        return _readiness_result(
            generated_at=generated_at,
            state="READY_SUBMIT_CAPABLE",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    reasons.append("PAPER runtime has eligible lanes, but submit/bridge authority is not explicit.")
    return _readiness_result(
        generated_at=generated_at,
        state="DEGRADED_NO_SUBMIT",
        reasons=reasons,
        blockers=blockers,
        warnings=warnings,
        inputs=inputs,
    )


def build_readiness_inputs(
    *,
    repo_root: Path,
    expected_root: Path,
    artifacts: Mapping[str, Any],
    root_guard: Mapping[str, Any],
    now: datetime | None = None,
) -> dict[str, Any]:
    now = _ensure_utc(now or datetime.now(timezone.utc))
    broker_truth = _broker_truth_input(_mapping(artifacts.get("broker_truth_status")), now=now)
    broker_truth_lease = _broker_truth_lease_input(_mapping(artifacts.get("broker_truth_lease")), now=now)
    reconciliation = _reconciliation_input(_mapping(artifacts.get("phase1_reconciliation")), now=now)
    operator_status = _mapping(artifacts.get("operator_status"))
    config_in_force = _mapping(artifacts.get("config_in_force"))
    lane_quarantine = _lane_quarantine_input(_mapping(artifacts.get("lane_quarantine")))
    runtime = _runtime_input(operator_status, config_in_force, root_guard)
    market_data = _market_data_input(
        operator_status,
        _mapping(artifacts.get("market_data_probe")),
        _mapping(artifacts.get("phase1_databento_live_listener_status")),
        repo_root=repo_root,
        now=now,
    )
    submit_bridge = _submit_bridge_input(repo_root, operator_status, _mapping(artifacts.get("live_timing_summary")))
    backend = _backend_input(_mapping(artifacts.get("dashboard_health")), root_guard)
    live_money_eligible = any(
        _bool(source.get("live_money_eligible"))
        for source in (
            broker_truth,
            broker_truth_lease,
            reconciliation,
            lane_quarantine,
            submit_bridge,
            runtime,
        )
        if isinstance(source, Mapping)
    )
    return {
        "schema_version": "track_b_canonical_readiness_inputs_v1",
        "generated_at": now.isoformat(),
        "paper_only": True,
        "expected_root": str(expected_root),
        "active_root": str(repo_root),
        "root_guard_summary": dict(root_guard),
        "backend": backend,
        "runtime": runtime,
        "broker_truth": broker_truth,
        "broker_truth_lease": broker_truth_lease,
        "phase1_reconciliation": reconciliation,
        "market_data": market_data,
        "lane_quarantine": lane_quarantine,
        "submit_bridge": submit_bridge,
        "live_money_eligible": live_money_eligible,
    }


def build_root_process_guard(
    *,
    repo_root: Path,
    expected_root: Path,
    artifacts: Mapping[str, Any] | None = None,
    process_cwd_resolver: Callable[[int], Path | None] | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    now = _ensure_utc(now or datetime.now(timezone.utc))
    repo_root = repo_root.resolve()
    expected_root = expected_root.resolve()
    process_cwd_resolver = process_cwd_resolver or _process_cwd
    artifacts = artifacts or _load_readiness_artifacts(repo_root)
    process_specs = _process_specs(repo_root, artifacts)
    processes: list[dict[str, Any]] = []
    wrong_root_processes: list[dict[str, Any]] = []
    unknown_root_processes: list[dict[str, Any]] = []

    for spec in process_specs:
        pid = spec.get("pid")
        try:
            pid_int = int(pid) if pid is not None else None
        except (TypeError, ValueError):
            pid_int = None
        running = bool(pid_int is not None and _pid_running(pid_int))
        cwd = process_cwd_resolver(pid_int) if running and pid_int is not None else None
        artifact_root = _path_or_none(spec.get("artifact_root"))
        detected_root = cwd or artifact_root
        root_match = _path_matches_root(detected_root, expected_root) if detected_root is not None else None
        record = {
            "name": spec.get("name"),
            "pid": pid_int,
            "running": running,
            "pid_source": spec.get("pid_source"),
            "cwd": str(cwd) if cwd is not None else None,
            "artifact_root": str(artifact_root) if artifact_root is not None else None,
            "detected_root": str(detected_root) if detected_root is not None else None,
            "root_match": root_match,
        }
        processes.append(record)
        if running and root_match is False:
            wrong_root_processes.append(record)
        elif running and root_match is None:
            unknown_root_processes.append(record)

    return {
        "schema_version": "track_b_root_process_guard_v1",
        "generated_at": now.isoformat(),
        "expected_root": str(expected_root),
        "active_root": str(repo_root),
        "root_match": not wrong_root_processes,
        "wrong_root_processes": wrong_root_processes,
        "unknown_root_processes": unknown_root_processes,
        "processes": processes,
        "operator_action_required": bool(wrong_root_processes),
    }


def write_canonical_readiness_artifact(
    *,
    repo_root: Path = REPO_ROOT,
    expected_root: Path | None = None,
    output_path: Path | None = None,
    now: datetime | None = None,
) -> dict[str, Any]:
    repo_root = repo_root.resolve()
    output_path = output_path or repo_root / DEFAULT_CANONICAL_READINESS_ARTIFACT
    payload = build_canonical_readiness(repo_root=repo_root, expected_root=expected_root, now=now)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    _atomic_write_json(output_path, payload)
    return payload


def load_canonical_readiness_artifact(repo_root: Path = REPO_ROOT) -> dict[str, Any]:
    return _read_json(repo_root / DEFAULT_CANONICAL_READINESS_ARTIFACT)


def _load_readiness_artifacts(repo_root: Path) -> dict[str, Any]:
    paper_root = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    runtime_dir = paper_root / "runtime"
    reports_root = repo_root / "outputs" / "reports"
    dashboard_runtime = repo_root / "outputs" / "operator_dashboard" / "runtime"
    return {
        "operator_status": _read_json(paper_root / "operator_status.json"),
        "config_in_force": _read_json(runtime_dir / "paper_config_in_force.json"),
        "lane_quarantine": _read_json(runtime_dir / "paper_lane_quarantine_status.json"),
        "market_data_probe": _read_json(runtime_dir / "market_data_transport_probe.json"),
        "live_timing_summary": _read_json(paper_root / "live_timing_summary_latest.json"),
        "broker_truth_status": _read_json(
            reports_root / "ibkr_read_only_verification" / "ibkr_broker_truth_refresh_status.json"
        ),
        "phase1_reconciliation": _read_json(
            reports_root
            / "track_b_paper_broker_reconciliation"
            / "latest_track_b_paper_broker_reconciliation.json"
        ),
        "dashboard_health": _read_json(dashboard_runtime / "headless_supervised_paper_health.json"),
        "broker_truth_lease": _read_json(repo_root / DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT),
        "phase1_databento_live_listener_status": _read_json(
            repo_root / DEFAULT_PHASE1_DATABENTO_LIVE_LISTENER_STATUS_ARTIFACT
        ),
    }


def _process_specs(repo_root: Path, artifacts: Mapping[str, Any]) -> list[dict[str, Any]]:
    paper_root = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session"
    runtime_dir = paper_root / "runtime"
    dashboard_runtime = repo_root / "outputs" / "operator_dashboard" / "runtime"
    operator_status = _mapping(artifacts.get("operator_status"))
    config_in_force = _mapping(artifacts.get("config_in_force"))
    lane_quarantine = _mapping(artifacts.get("lane_quarantine"))
    dashboard_health = _mapping(artifacts.get("dashboard_health"))
    specs = [
        {
            "name": "paper_runtime",
            "pid": _first_int(
                operator_status.get("source_runtime_pid"),
                lane_quarantine.get("source_runtime_pid"),
                config_in_force.get("source_runtime_pid"),
                _read_pid(runtime_dir / "probationary_paper.pid"),
            ),
            "pid_source": "paper runtime artifact/pid file",
            "artifact_root": _first_path(
                operator_status.get("source_runtime_repo_root"),
                operator_status.get("source_runtime_cwd"),
                lane_quarantine.get("source_runtime_repo_root"),
                lane_quarantine.get("source_runtime_cwd"),
                config_in_force.get("source_runtime_cwd"),
            ),
        },
        {
            "name": "operator_dashboard_backend",
            "pid": _first_int(dashboard_health.get("pid"), _read_pid(dashboard_runtime / "operator_dashboard.pid")),
            "pid_source": "dashboard health/pid file",
            "artifact_root": repo_root,
        },
        {
            "name": "paper_strategy_monitor",
            "pid": _read_pid(repo_root / "var" / "paper_strategy_monitor_service.pid"),
            "pid_source": "paper strategy monitor pid file",
            "artifact_root": repo_root,
        },
        {
            "name": "broker_truth_refresher",
            "pid": _read_pid(repo_root / "var" / "track_b_broker_truth_refresh_service.pid"),
            "pid_source": "broker truth refresher pid file",
            "artifact_root": repo_root,
        },
        {
            "name": "operator_readiness_refresher",
            "pid": _read_pid(repo_root / "var" / "track_b_operator_readiness_refresh_service.pid"),
            "pid_source": "operator readiness refresher pid file",
            "artifact_root": repo_root,
        },
    ]
    return specs


def _backend_input(health: Mapping[str, Any], root_guard: Mapping[str, Any]) -> dict[str, Any]:
    backend_processes = [
        process
        for process in list(root_guard.get("processes") or [])
        if isinstance(process, Mapping) and process.get("name") == "operator_dashboard_backend"
    ]
    process_running = any(_bool(process.get("running")) for process in backend_processes)
    return {
        "healthy": bool(health.get("ready") is True or health.get("status") == "ok"),
        "running": process_running,
        "status": health.get("status"),
        "ready": health.get("ready"),
        "pid": health.get("pid") or (backend_processes[0].get("pid") if backend_processes else None),
        "dashboard_attached": health.get("dashboard_attached"),
    }


def _runtime_input(
    operator_status: Mapping[str, Any],
    config_in_force: Mapping[str, Any],
    root_guard: Mapping[str, Any],
) -> dict[str, Any]:
    active_lane_ids = [str(value) for value in list(operator_status.get("active_lane_ids") or []) if str(value)]
    lane_rows = [row for row in list(operator_status.get("lanes") or []) if isinstance(row, Mapping)]
    configured_lanes = [row for row in list(config_in_force.get("lanes") or []) if isinstance(row, Mapping)]
    eligible_count = sum(1 for row in lane_rows if row.get("eligible_now") is True)
    if eligible_count == 0:
        try:
            eligible_count = int(operator_status.get("usable_lane_count") or 0)
        except (TypeError, ValueError):
            eligible_count = 0
    paper_processes = [
        process
        for process in list(root_guard.get("processes") or [])
        if isinstance(process, Mapping) and process.get("name") == "paper_runtime"
    ]
    process_running = any(_bool(process.get("running")) for process in paper_processes)
    health = _mapping(operator_status.get("health"))
    health_status = str(health.get("health_status") or operator_status.get("strategy_status") or "").upper()
    faulted = bool(operator_status.get("fault_code")) or health_status.startswith("FAULT")
    return {
        "running": process_running,
        "healthy": bool(process_running and not faulted and operator_status.get("operator_halt") is not True),
        "health_status": health.get("health_status"),
        "entries_enabled": operator_status.get("entries_enabled") is True,
        "operator_halt": operator_status.get("operator_halt") is True,
        "loaded_lane_count": len(active_lane_ids) or len(configured_lanes),
        "eligible_lane_count": eligible_count,
        "active_lane_ids": active_lane_ids,
        "live_money_eligible": operator_status.get("live_money_eligible") is True,
    }


def _broker_truth_input(payload: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    if not payload:
        return {
            "available": False,
            "fresh": False,
            "classification": "BROKER_TRUTH_REFRESH_STATUS_MISSING",
            "positions_complete": False,
            "open_orders_complete": False,
            "live_money_eligible": False,
        }
    last_good_candidate = _mapping(payload.get("last_successful_broker_truth"))
    last_good = (
        last_good_candidate
        if last_good_candidate.get("positions_complete") is True
        and last_good_candidate.get("open_orders_complete") is True
        and not str(last_good_candidate.get("classification") or "").upper().endswith("FAILED")
        else {}
    )
    source = last_good or payload
    using_last_good = bool(last_good)
    generated_at = source.get("generated_at") or payload.get("generated_at")
    age_seconds = _age_seconds(generated_at, now)
    threshold = _float_value(payload.get("freshness_threshold_seconds"), BROKER_FRESHNESS_DEFAULT_SECONDS)
    positions_complete = source.get("positions_complete") is True
    open_orders_complete = source.get("open_orders_complete") is True
    fresh = bool(
        positions_complete
        and open_orders_complete
        and age_seconds is not None
        and age_seconds <= threshold
        and (using_last_good or payload.get("last_success") is not False)
    )
    return {
        "available": True,
        "fresh": fresh,
        "classification": "BROKER_TRUTH_REFRESH_FRESH" if fresh else str(payload.get("classification") or ""),
        "source_classification": payload.get("classification"),
        "generated_at": generated_at,
        "age_seconds": age_seconds,
        "freshness_threshold_seconds": threshold,
        "positions_complete": positions_complete,
        "open_orders_complete": open_orders_complete,
        "open_order_count": source.get("open_order_count"),
        "position_count": source.get("position_count"),
        "latest_attempt_status": _mapping(payload.get("latest_attempt_status")),
        "last_successful_broker_truth": dict(last_good_candidate),
        "using_last_successful_broker_truth": using_last_good,
        "live_money_eligible": payload.get("live_money_eligible") is True or source.get("live_money_eligible") is True,
        "submit_authority": payload.get("submit_authority") is True or source.get("submit_authority") is True,
    }


def _broker_truth_lease_input(payload: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    if not payload:
        return {
            "available": False,
            "lease_state": "BROKER_TRUTH_LEASE_MISSING",
            "live_money_eligible": False,
        }
    lease_state = str(payload.get("lease_state") or payload.get("state") or "").upper()
    generated_at = payload.get("generated_at")
    valid_until = payload.get("valid_until")
    entry_valid_until = payload.get("entry_valid_until") or valid_until
    exit_valid_until = payload.get("exit_valid_until") or valid_until
    return {
        "available": True,
        "lease_state": lease_state,
        "generated_at": generated_at,
        "age_seconds": _age_seconds(generated_at, now),
        "valid_until": valid_until,
        "entry_valid_until": entry_valid_until,
        "exit_valid_until": exit_valid_until,
        "entry_seconds_remaining": _seconds_until(entry_valid_until, now),
        "exit_seconds_remaining": _seconds_until(exit_valid_until, now),
        "submit_entry_allowed": payload.get("submit_entry_allowed") is True,
        "submit_exit_allowed": payload.get("submit_exit_allowed") is True,
        "warnings": list(payload.get("warnings") or []),
        "blockers": list(payload.get("blockers") or []),
        "contradiction_details": list(payload.get("contradiction_details") or []),
        "operator_action_required": payload.get("operator_action_required") is True,
        "account_id": payload.get("account_id"),
        "source_artifacts": dict(_mapping(payload.get("source_artifacts"))),
        "live_money_eligible": payload.get("live_money_eligible") is True,
    }


def _reconciliation_input(payload: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    if not payload:
        return {
            "available": False,
            "fresh": False,
            "classification": "TRACK_B_PAPER_BROKER_RECONCILIATION_MISSING",
            "broker_reconciled": False,
            "live_money_eligible": False,
        }
    age_seconds = _age_seconds(payload.get("generated_at"), now)
    threshold = max(_float_value(payload.get("max_age_seconds"), 120.0) * 1.5, RECONCILIATION_FRESHNESS_DEFAULT_SECONDS)
    return {
        "available": True,
        "fresh": bool(age_seconds is not None and age_seconds <= threshold),
        "classification": payload.get("classification"),
        "generated_at": payload.get("generated_at"),
        "age_seconds": age_seconds,
        "freshness_threshold_seconds": threshold,
        "broker_reconciled": payload.get("broker_reconciled") is True,
        "review_required_count": payload.get("review_required_count"),
        "lifecycle_open_position_count": payload.get("lifecycle_open_position_count"),
        "track_b_broker_open_order_count": payload.get("track_b_broker_open_order_count"),
        "blockers": list(payload.get("blockers") or []) if isinstance(payload.get("blockers"), list) else [],
        "live_money_eligible": payload.get("live_money_eligible") is True,
        "submit_authority": payload.get("submit_authority") is True,
    }


def _market_data_input(
    operator_status: Mapping[str, Any],
    market_probe: Mapping[str, Any],
    phase1_listener_status: Mapping[str, Any] | None = None,
    *,
    repo_root: Path = REPO_ROOT,
    now: datetime,
) -> dict[str, Any]:
    fallback = _fallback_market_data_input(operator_status, market_probe, now=now)
    listener_payload = _mapping(phase1_listener_status)
    if not listener_payload:
        fallback["source"] = "operator_runtime_fallback"
        fallback["listener_available"] = False
        fallback["fallback_used"] = True
        return fallback

    listener = _phase1_listener_market_data_input(listener_payload, repo_root=repo_root, now=now)
    if listener["fresh"]:
        return listener
    listener_global_issue = listener.get("listener_global_issue") is True
    if listener_global_issue and fallback["fresh"]:
        fallback["source"] = "operator_runtime_fallback"
        fallback["listener_available"] = True
        fallback["listener_status"] = listener.get("listener_status")
        fallback["listener_global_issue"] = True
        fallback["listener_blockers"] = list(listener.get("blockers") or [])
        fallback["fallback_used"] = True
        fallback["warnings"] = [
            {
                "code": "phase1_listener_status_fallback_used",
                "detail": "Phase-1 listener status is stale/down; fresh explicit runtime fallback evidence is being used.",
                "source": "market_data",
            }
        ]
        return fallback
    return listener


def _fallback_market_data_input(
    operator_status: Mapping[str, Any],
    market_probe: Mapping[str, Any],
    *,
    now: datetime,
) -> dict[str, Any]:
    last_bar = operator_status.get("last_processed_bar_end_ts") or operator_status.get("updated_at")
    age_seconds = _age_seconds(last_bar, now)
    probe_ready = market_probe.get("runtime_ready") is True or market_probe.get("status") == "ok"
    health = _mapping(operator_status.get("health"))
    market_data_ok = health.get("market_data_ok") is True or probe_ready
    fresh = bool(market_data_ok and age_seconds is not None and age_seconds <= MARKET_DATA_FRESHNESS_DEFAULT_SECONDS)
    return {
        "source": "operator_runtime_fallback",
        "fresh": fresh,
        "market_data_ok": market_data_ok,
        "last_processed_bar_end_ts": last_bar,
        "age_seconds": age_seconds,
        "freshness_threshold_seconds": MARKET_DATA_FRESHNESS_DEFAULT_SECONDS,
        "probe_status": market_probe.get("status"),
        "probe_runtime_ready": market_probe.get("runtime_ready"),
        "warnings": [],
        "blockers": [],
    }


def _phase1_listener_market_data_input(
    payload: Mapping[str, Any],
    *,
    repo_root: Path,
    now: datetime,
) -> dict[str, Any]:
    source = str(payload.get("source") or payload.get("source_id") or "").strip()
    generated_at = payload.get("generated_at")
    status_age_seconds = _age_seconds(generated_at, now)
    live_rows = _phase1_live_symbol_rows(payload, repo_root=repo_root)
    required_symbols = _symbols_from_payload_or_rows(payload.get("required_for_readiness_symbols"), live_rows, required=True)
    optional_symbols = _symbols_from_payload_or_rows(payload.get("optional_symbols"), live_rows, required=False)
    listener_threshold = _listener_status_freshness_threshold(live_rows)
    provider_status = str(payload.get("provider_status") or "").upper()
    listener_status_fresh = bool(status_age_seconds is not None and status_age_seconds <= listener_threshold)
    listener_down = provider_status in {
        "BLOCKED_MISSING_CREDENTIALS",
        "ERROR",
        "STOPPED_WITH_ERRORS",
        "PROVIDER_LIVE_UNAVAILABLE",
    }
    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    rows: list[dict[str, Any]] = []

    if source != MARKET_DATA_REQUIRED_SOURCE:
        blockers.append(
            {
                "code": "market_data_invalid_provenance",
                "detail": "Phase-1 listener market-data source is not DATABENTO_REALTIME_PHASE1.",
                "source": "phase1_databento_live_listener",
            }
        )
    if _provenance_forbidden(payload):
        blockers.append(
            {
                "code": "market_data_invalid_provenance",
                "detail": "Phase-1 listener status indicates historical/replay/research/archive market data.",
                "source": "phase1_databento_live_listener",
            }
        )
    if not listener_status_fresh:
        blockers.append(
            {
                "code": "phase1_listener_status_stale",
                "detail": "Phase-1 Databento listener status artifact is stale.",
                "source": "phase1_databento_live_listener",
            }
        )
    if listener_down:
        blockers.append(
            {
                "code": "phase1_listener_status_down",
                "detail": "Phase-1 Databento listener status reports a down/error state.",
                "source": "phase1_databento_live_listener",
            }
        )

    row_by_symbol = {str(row.get("symbol") or "").strip().upper(): row for row in live_rows if str(row.get("symbol") or "").strip()}
    for symbol in [*required_symbols, *optional_symbols]:
        row = _mapping(row_by_symbol.get(symbol))
        evaluated = _evaluate_phase1_listener_symbol(symbol=symbol, row=row, required=symbol in required_symbols, now=now)
        rows.append(evaluated)
        if evaluated["ready"] is True:
            continue
        issue = {
            "code": "market_data_not_fresh" if evaluated["required"] else "optional_market_data_degraded",
            "detail": evaluated["block_reason"],
            "source": "phase1_databento_live_listener",
            "symbol": symbol,
        }
        if evaluated["required"]:
            blockers.append(issue)
        else:
            warnings.append(issue)

    fresh = bool(required_symbols and not blockers)
    return {
        "source": "phase1_databento_live_listener",
        "available": True,
        "fresh": fresh,
        "market_data_ok": fresh,
        "listener_available": True,
        "listener_status": provider_status,
        "listener_alive": payload.get("listener_alive") is True,
        "listener_status_fresh": listener_status_fresh,
        "listener_global_issue": bool(not listener_status_fresh or listener_down),
        "generated_at": generated_at,
        "age_seconds": status_age_seconds,
        "freshness_threshold_seconds": listener_threshold,
        "required_symbols": required_symbols,
        "optional_symbols": optional_symbols,
        "required_blocked_symbols": [row["symbol"] for row in rows if row["required"] and row["ready"] is not True],
        "optional_degraded_symbols": [row["symbol"] for row in rows if not row["required"] and row["ready"] is not True],
        "rows": rows,
        "warnings": warnings,
        "blockers": blockers,
        "historical_seed_ready": payload.get("historical_seed_ready") is True,
        "research_artifact_used": payload.get("research_artifact_used") is True,
        "archive_artifact_used": payload.get("archive_artifact_used") is True,
        "databento_live_api_replay": payload.get("databento_live_api_replay") is True,
    }


def _phase1_live_symbol_rows(payload: Mapping[str, Any], *, repo_root: Path) -> list[dict[str, Any]]:
    namelist_rows = _load_market_data_namelist_rows(repo_root)
    by_symbol: dict[str, dict[str, Any]] = {row["symbol"]: row for row in namelist_rows}
    status_rows = [row for row in list(payload.get("rows") or []) if isinstance(row, Mapping)]
    for status_row in status_rows:
        symbol = str(status_row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        merged = dict(by_symbol.get(symbol) or {})
        merged.update(dict(status_row))
        merged["symbol"] = symbol
        by_symbol[symbol] = merged
    ordered_symbols = [
        str(symbol).strip().upper()
        for symbol in list(payload.get("symbols") or by_symbol)
        if str(symbol).strip().upper() in by_symbol
    ]
    return [by_symbol[symbol] for symbol in ordered_symbols]


def _load_market_data_namelist_rows(repo_root: Path) -> list[dict[str, Any]]:
    config_path = repo_root / DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH
    if not config_path.exists():
        config_path = DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH
    try:
        namelist = load_track_b_live_market_data_symbols(config_path)
    except Exception:
        return []
    return [_live_market_data_symbol_row(row) for row in namelist.enabled_symbols()]


def _live_market_data_symbol_row(row: TrackBLiveMarketDataSymbol) -> dict[str, Any]:
    return {
        "symbol": row.symbol,
        "required_for_readiness": row.required_for_readiness,
        "databento_symbol": row.databento_symbol,
        "dataset": row.dataset,
        "schema": row.schema,
        "freshness_threshold_seconds": row.freshness_threshold_seconds,
        "min_confirmed_bars": row.min_confirmed_bars,
    }


def _symbols_from_payload_or_rows(value: Any, rows: Sequence[Mapping[str, Any]], *, required: bool) -> list[str]:
    symbols = [str(symbol).strip().upper() for symbol in list(value or []) if str(symbol).strip()]
    if symbols:
        return symbols
    return [
        str(row.get("symbol") or "").strip().upper()
        for row in rows
        if str(row.get("symbol") or "").strip()
        and (row.get("required_for_readiness") is True) is required
    ]


def _evaluate_phase1_listener_symbol(
    *,
    symbol: str,
    row: Mapping[str, Any],
    required: bool,
    now: datetime,
) -> dict[str, Any]:
    if not row:
        return _phase1_symbol_evaluation(symbol=symbol, row=row, required=required, ready=False, block_reason=f"{symbol} listener row missing.")
    if _provenance_forbidden(row):
        return _phase1_symbol_evaluation(
            symbol=symbol,
            row=row,
            required=required,
            ready=False,
            block_reason=f"{symbol} listener row uses historical/replay/research/archive data.",
        )
    if row.get("source") not in {None, "", MARKET_DATA_REQUIRED_SOURCE}:
        return _phase1_symbol_evaluation(
            symbol=symbol,
            row=row,
            required=required,
            ready=False,
            block_reason=f"{symbol} listener row has invalid provenance source.",
        )
    latest_completed = (
        row.get("latest_completed_bar_ts")
        or row.get("last_completed_bar_ts")
        or row.get("bar_end")
        or row.get("generated_at")
    )
    age_seconds = _age_seconds(latest_completed, now)
    threshold = _float_value(row.get("freshness_threshold_seconds"), MARKET_DATA_FRESHNESS_DEFAULT_SECONDS)
    bar_count = _first_int(row.get("bar_count"), row.get("confirmed_bar_count"), row.get("bars_available")) or 0
    min_bars = _first_int(row.get("min_confirmed_bars"), row.get("minimum_bar_count")) or 1
    confirmed = row.get("realtime_feed_confirmed") is True
    fresh = age_seconds is not None and age_seconds <= threshold
    ready = bool(confirmed and fresh and bar_count >= min_bars)
    if ready:
        block_reason = "READY"
    elif not confirmed:
        block_reason = f"{symbol} realtime feed is not confirmed."
    elif bar_count < min_bars:
        block_reason = f"{symbol} has {bar_count} confirmed bar(s), below required {min_bars}."
    elif not fresh:
        block_reason = f"{symbol} latest confirmed bar is stale."
    else:
        block_reason = f"{symbol} market data is not ready."
    return _phase1_symbol_evaluation(
        symbol=symbol,
        row=row,
        required=required,
        ready=ready,
        block_reason=block_reason,
        age_seconds=age_seconds,
        threshold=threshold,
        bar_count=bar_count,
        min_bars=min_bars,
    )


def _phase1_symbol_evaluation(
    *,
    symbol: str,
    row: Mapping[str, Any],
    required: bool,
    ready: bool,
    block_reason: str,
    age_seconds: float | None = None,
    threshold: float | None = None,
    bar_count: int | None = None,
    min_bars: int | None = None,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "required": required,
        "ready": ready,
        "realtime_feed_confirmed": row.get("realtime_feed_confirmed") is True,
        "latest_completed_bar_ts": row.get("latest_completed_bar_ts") or row.get("last_completed_bar_ts"),
        "age_seconds": age_seconds,
        "freshness_threshold_seconds": threshold,
        "bar_count": bar_count,
        "min_confirmed_bars": min_bars,
        "databento_symbol": row.get("databento_symbol"),
        "dataset": row.get("dataset"),
        "schema": row.get("schema"),
        "block_reason": block_reason,
    }


def _listener_status_freshness_threshold(rows: Sequence[Mapping[str, Any]]) -> float:
    thresholds = [
        _float_value(row.get("freshness_threshold_seconds"), MARKET_DATA_FRESHNESS_DEFAULT_SECONDS)
        for row in rows
        if row.get("required_for_readiness") is True
    ]
    return max(thresholds or [MARKET_DATA_FRESHNESS_DEFAULT_SECONDS])


def _provenance_forbidden(payload: Mapping[str, Any]) -> bool:
    return bool(
        payload.get("historical_seed_ready") is True
        or payload.get("historical_seed_used") is True
        or payload.get("databento_live_api_replay") is True
        or payload.get("replay_artifact_used") is True
        or payload.get("research_artifact_used") is True
        or payload.get("archive_artifact_used") is True
    )


def _lane_quarantine_input(payload: Mapping[str, Any]) -> dict[str, Any]:
    quarantine_count = int(payload.get("quarantine_count") or 0) if payload else 0
    return {
        "available": bool(payload),
        "classification": payload.get("classification") if payload else "PAPER_LANE_QUARANTINE_STATUS_MISSING",
        "quarantine_count": quarantine_count,
        "healthy_lane_ids": list(payload.get("healthy_lane_ids") or []) if payload else [],
        "quarantined_lane_ids": list(payload.get("quarantined_lane_ids") or []) if payload else [],
        "operator_action_required": payload.get("operator_action_required") is True if payload else False,
        "live_money_eligible": payload.get("live_money_eligible") is True if payload else False,
    }


def _submit_bridge_input(
    repo_root: Path,
    operator_status: Mapping[str, Any],
    live_timing_summary: Mapping[str, Any],
) -> dict[str, Any]:
    active_lane_ids = {
        str(value)
        for value in list(operator_status.get("active_lane_ids") or [])
        if str(value).strip()
    }
    summaries = []
    seen_summary_lane_ids: set[str] = set()
    if live_timing_summary and _summary_matches_active_lane(
        live_timing_summary,
        active_lane_ids=active_lane_ids,
    ):
        summaries.append(live_timing_summary)
        lane_id = _summary_lane_id(live_timing_summary)
        if lane_id:
            seen_summary_lane_ids.add(lane_id)
    lanes_root = repo_root / "outputs" / "probationary_pattern_engine" / "paper_session" / "lanes"
    if lanes_root.exists():
        if active_lane_ids:
            summary_paths = [
                lanes_root / lane_id / "live_timing_summary_latest.json"
                for lane_id in sorted(active_lane_ids)
            ]
        else:
            summary_paths = sorted(lanes_root.glob("*/live_timing_summary_latest.json"))
        for path in summary_paths:
            payload = _read_json(path)
            lane_id = _summary_lane_id(payload, fallback_lane_id=path.parent.name) if payload else ""
            if (
                payload
                and lane_id not in seen_summary_lane_ids
                and _summary_matches_active_lane(
                    payload,
                    active_lane_ids=active_lane_ids,
                    fallback_lane_id=path.parent.name,
                )
            ):
                summaries.append(payload)
                if lane_id:
                    seen_summary_lane_ids.add(lane_id)
    route_ready_rows = []
    for payload in summaries:
        account_health = _mapping(_mapping(payload.get("broker_truth")).get("account_health"))
        route_destination = str(account_health.get("route_destination") or "")
        bridge_proxy_mode = str(account_health.get("bridge_proxy_mode") or "")
        healthy = str(account_health.get("status") or "").upper() == "HEALTHY"
        route_ready = bool(healthy and "paper_bridge_submit_capable" in route_destination)
        route_ready_rows.append(
            {
                "lane_id": payload.get("lane_id"),
                "route_destination": route_destination,
                "bridge_proxy_mode": bridge_proxy_mode,
                "account_health_status": account_health.get("status"),
                "submit_route_ready": route_ready,
            }
        )
    submit_route_ready = any(row["submit_route_ready"] for row in route_ready_rows)
    eligible_lane_count = sum(
        1
        for row in list(operator_status.get("lanes") or [])
        if isinstance(row, Mapping) and row.get("eligible_now") is True
    )
    if eligible_lane_count == 0:
        try:
            eligible_lane_count = int(operator_status.get("usable_lane_count") or 0)
        except (TypeError, ValueError):
            eligible_lane_count = 0
    return {
        "submit_route_ready": submit_route_ready,
        "submit_authority_explicit": bool(submit_route_ready and eligible_lane_count > 0),
        "route_rows": route_ready_rows,
        "eligible_lane_count": eligible_lane_count,
        "live_money_eligible": False,
    }


def _summary_matches_active_lane(
    payload: Mapping[str, Any],
    *,
    active_lane_ids: set[str],
    fallback_lane_id: str | None = None,
) -> bool:
    if not active_lane_ids:
        return True
    lane_id = _summary_lane_id(payload, fallback_lane_id=fallback_lane_id)
    return bool(lane_id and lane_id in active_lane_ids)


def _summary_lane_id(payload: Mapping[str, Any], *, fallback_lane_id: str | None = None) -> str:
    return str(payload.get("lane_id") or fallback_lane_id or "").strip()


def _readiness_result(
    *,
    generated_at: str,
    state: str,
    reasons: Sequence[str],
    blockers: Sequence[Mapping[str, Any]],
    warnings: Sequence[Mapping[str, Any]],
    inputs: Mapping[str, Any],
) -> dict[str, Any]:
    if state not in CANONICAL_READINESS_STATES:
        state = "NOT_READY_CONFIG"
    root_guard = _mapping(inputs.get("root_guard_summary"))
    return {
        "schema_version": "track_b_canonical_readiness_v1",
        "generated_at": generated_at,
        "paper_only": True,
        "canonical_readiness": state,
        "state": state,
        "ready_submit_capable": state == "READY_SUBMIT_CAPABLE",
        "readiness_reasons": list(reasons),
        "readiness_blockers": [dict(row) for row in blockers],
        "readiness_warnings": [dict(row) for row in warnings],
        "operator_action_required": bool(blockers or root_guard.get("operator_action_required")),
        "root_guard_summary": dict(root_guard),
        "backend": _mapping(inputs.get("backend")),
        "runtime": _mapping(inputs.get("runtime")),
        "broker_truth": _mapping(inputs.get("broker_truth")),
        "broker_truth_lease": _mapping(inputs.get("broker_truth_lease")),
        "phase1_reconciliation": _mapping(inputs.get("phase1_reconciliation")),
        "market_data": _mapping(inputs.get("market_data")),
        "lane_quarantine": _mapping(inputs.get("lane_quarantine")),
        "submit_bridge": _mapping(inputs.get("submit_bridge")),
        "live_money_eligible": _bool(inputs.get("live_money_eligible")),
    }


def _expected_root(explicit: Path | None) -> Path:
    if explicit is not None:
        return explicit.expanduser()
    raw = str(os.environ.get(TRACK_B_EXPECTED_ACTIVE_ROOT_ENV) or "").strip()
    return Path(raw).expanduser() if raw else DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT


def _process_cwd(pid: int) -> Path | None:
    proc_cwd = Path("/proc") / str(pid) / "cwd"
    try:
        if proc_cwd.exists():
            return proc_cwd.resolve()
    except OSError:
        pass
    try:
        result = subprocess.run(
            ["lsof", "-a", "-d", "cwd", "-p", str(pid), "-Fn"],
            check=False,
            stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL,
            text=True,
            timeout=2.0,
        )
    except (OSError, subprocess.TimeoutExpired):
        return None
    if result.returncode != 0:
        return None
    for line in result.stdout.splitlines():
        if line.startswith("n") and len(line) > 1:
            return Path(line[1:]).resolve()
    return None


def _pid_running(pid: int) -> bool:
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except OSError as exc:
        if exc.errno == errno.EPERM:
            return True
        return False
    return True


def _path_matches_root(path: Path | None, root: Path) -> bool | None:
    if path is None:
        return None
    try:
        path_resolved = path.resolve()
        root_resolved = root.resolve()
        common = os.path.commonpath([str(path_resolved), str(root_resolved)])
    except (OSError, ValueError):
        return False
    return common == str(root_resolved)


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _read_pid(path: Path) -> int | None:
    if not path.exists():
        return None
    try:
        return int(path.read_text(encoding="utf-8").strip())
    except (OSError, ValueError):
        return None


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    tmp_path = path.with_suffix(path.suffix + ".tmp")
    tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp_path.replace(path)


def _mapping(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def _bool(value: Any) -> bool:
    return value is True


def _ensure_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc)


def _parse_iso(value: Any) -> datetime | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None
    return _ensure_utc(parsed)


def _age_seconds(value: Any, now: datetime) -> float | None:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    return max((now - parsed).total_seconds(), 0.0)


def _seconds_until(value: Any, now: datetime) -> float | None:
    parsed = _parse_iso(value)
    if parsed is None:
        return None
    return (parsed - now).total_seconds()


def _float_value(value: Any, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _first_int(*values: Any) -> int | None:
    for value in values:
        try:
            if value is not None and str(value).strip():
                return int(value)
        except (TypeError, ValueError):
            continue
    return None


def _first_path(*values: Any) -> Path | None:
    for value in values:
        path = _path_or_none(value)
        if path is not None:
            return path
    return None


def _path_or_none(value: Any) -> Path | None:
    text = str(value or "").strip()
    if not text:
        return None
    return Path(text).expanduser()


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Build Track B canonical PAPER readiness artifact.")
    parser.add_argument("--repo-root", default=str(REPO_ROOT))
    parser.add_argument("--expected-root", default=None)
    parser.add_argument("--output", default=None)
    args = parser.parse_args(argv)
    repo_root = Path(args.repo_root).expanduser().resolve()
    expected_root = Path(args.expected_root).expanduser().resolve() if args.expected_root else None
    output = Path(args.output).expanduser() if args.output else None
    payload = write_canonical_readiness_artifact(
        repo_root=repo_root,
        expected_root=expected_root,
        output_path=output,
    )
    print(json.dumps(payload, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
