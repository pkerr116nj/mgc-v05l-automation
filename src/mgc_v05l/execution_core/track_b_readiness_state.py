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
from datetime import datetime, time, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence

from mgc_v05l.paths import PROJECT_ROOT, is_archived_project_root
from mgc_v05l.execution_core.track_b_broker_truth_lease import classify_broker_truth_lease
from mgc_v05l.execution_core.track_b_live_market_data_symbols import (
    DEFAULT_TRACK_B_LIVE_MARKET_DATA_SYMBOLS_PATH,
    TrackBLiveMarketDataSymbol,
    load_track_b_live_market_data_symbols,
)
from mgc_v05l.execution_core.track_b_pre_action_snapshot_validator import (
    DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT,
)
from mgc_v05l.execution_core.track_b_paper_minimal_startup import (
    TrackBPaperMinimalStartupConfig,
    build_track_b_paper_minimal_startup,
)
from mgc_v05l.execution_core.track_b_strategy_exit_coverage import (
    DEFAULT_EXIT_COVERAGE_REPORT_PATH,
    TrackBStrategyExitCoverageConfig,
    build_track_b_strategy_exit_coverage_report,
    exit_coverage_blocks_submit,
)
from mgc_v05l.market_data.phase1_market_session import classify_phase1_futures_market_session
from mgc_v05l.session_phase_labels import NEW_YORK

REPO_ROOT = PROJECT_ROOT
TRACK_B_EXPECTED_ACTIVE_ROOT_ENV = "MGC_TRACK_B_EXPECTED_ACTIVE_ROOT"
DEFAULT_TRACK_B_EXPECTED_ACTIVE_ROOT = PROJECT_ROOT
DEFAULT_CANONICAL_READINESS_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_canonical_readiness.json"
)
DEFAULT_BROKER_TRUTH_LEASE_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_truth_lease.json"
)
DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT = (
    Path("outputs") / "operator_dashboard" / "runtime" / "latest_broker_session_authority.json"
)
DEFAULT_PHASE1_DATABENTO_LIVE_LISTENER_STATUS_ARTIFACT = (
    Path("outputs")
    / "reports"
    / "phase1_databento_live_runtime_candles"
    / "latest_phase1_databento_live_listener_status.json"
)
DEFAULT_PAPER_RUNTIME_TRUTH_ARTIFACT = (
    Path("outputs")
    / "probationary_pattern_engine"
    / "paper_session"
    / "runtime"
    / "paper_runtime_truth.json"
)
DEFAULT_LIFECYCLE_STATUS_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_live_position_status.json"
)
DEFAULT_ORDER_STATE_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "paper_trade_ledger" / "latest_track_b_paper_trade_summary.json"
)
DEFAULT_PROOF_READINESS_ARTIFACT = (
    Path("outputs")
    / "track_b_execution_core"
    / "proof_readiness"
    / "latest_track_b_paper_proof_readiness.json"
)
DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json"
)
DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_managed_orders.json"
)
DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_orders" / "latest_order_adjustment_plan.json"
)
DEFAULT_POSITION_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "position_truth" / "latest_position_truth.json"
)
DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "runtime_truth" / "latest_runtime_environment_truth.json"
)
DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT = (
    Path("outputs") / "track_b_execution_core" / "managed_positions" / "latest_managed_positions.json"
)
DEFAULT_STRATEGY_EXIT_COVERAGE_ARTIFACT = DEFAULT_EXIT_COVERAGE_REPORT_PATH
CANONICAL_READINESS_STATES = {
    "READY_SUBMIT_CAPABLE",
    "READY_TO_START_DIAGNOSTIC_ONLY",
    "READY_OBSERVATION_ONLY",
    "WAITING_FOR_MARKET_REOPEN",
    "DEGRADED_NO_SUBMIT",
    "NOT_READY_DEPENDENCY",
    "NOT_READY_RECONCILIATION",
    "NOT_READY_CONFIG",
    "NOT_READY_WRONG_ROOT",
}
RECONCILIATION_CLEAN_CLASSIFICATIONS = {
    "BROKER_LIFECYCLE_RECONCILED",
    "TRACK_B_PAPER_BROKER_RECONCILED",
}
BROKER_FRESHNESS_DEFAULT_SECONDS = 150.0
RECONCILIATION_FRESHNESS_DEFAULT_SECONDS = 180.0
MARKET_DATA_FRESHNESS_DEFAULT_SECONDS = 180.0
MARKET_DATA_POST_REOPEN_GRACE_SECONDS = 10 * 60.0
PROOF_CLASSIFICATION_MAX_AGE_SECONDS = 300.0
CONTROL_PLANE_SNAPSHOT_MAX_AGE_SECONDS = 300.0
MARKET_DATA_REQUIRED_SOURCE = "DATABENTO_REALTIME_PHASE1"
BROKER_TRUTH_LEASE_READY_STATES = {"ACTIVE", "ACTIVE_DEGRADED_REFRESH_FAILING"}
BROKER_TRUTH_LEASE_EXPIRED_STATES = {"EXPIRED_BLOCK_NEW_ENTRIES", "EXPIRED_EXITS_ONLY"}
MARKET_CLOSED_NO_FRESH_BARS = "MARKET_CLOSED_NO_FRESH_BARS"
MARKET_SCHEDULE_OPEN = "MARKET_OPEN_EXPECT_FRESH_BARS"
MARKET_SCHEDULED_HALT = "SCHEDULED_MARKET_HALT"
MARKET_POST_REOPEN_GRACE = "POST_REOPEN_GRACE"


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
    broker_session_authority = _mapping(inputs.get("broker_session_authority"))
    execution_core_shared_truth = _mapping(inputs.get("execution_core_shared_truth"))
    latest_attempt = _mapping(broker_truth.get("latest_attempt_status"))
    reconciliation = _mapping(inputs.get("phase1_reconciliation"))
    runtime = _mapping(inputs.get("runtime"))
    runtime_truth_heartbeat = _mapping(inputs.get("runtime_truth_heartbeat"))
    backend = _mapping(inputs.get("backend"))
    market_data = _mapping(inputs.get("market_data"))
    schedule = _mapping(market_data.get("market_schedule"))
    lane_quarantine = _mapping(inputs.get("lane_quarantine"))
    submit_bridge = _mapping(inputs.get("submit_bridge"))
    control_plane_authorization = _mapping(inputs.get("control_plane_authorization"))
    strategy_exit_coverage = _mapping(inputs.get("strategy_exit_coverage"))
    paper_minimal_startup = _mapping(inputs.get("paper_minimal_startup"))

    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []
    reasons: list[str] = []

    def block(code: str, detail: str, *, source: str | None = None, **extra: Any) -> None:
        row = {"code": code, "detail": detail, "source": source}
        row.update(extra)
        blockers.append(row)
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
        or _bool(execution_core_shared_truth.get("live_money_eligible"))
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

    runtime_running = _bool(runtime.get("running"))
    runtime_healthy = _bool(runtime.get("healthy"))
    if runtime_running and runtime_healthy and paper_minimal_startup.get("allowed") is True:
        for row in list(paper_minimal_startup.get("warnings") or []):
            if isinstance(row, Mapping):
                warn(
                    str(row.get("code") or "paper_minimal_startup_warning"),
                    str(row.get("detail") or "PAPER minimal startup diagnostic warning."),
                    source=str(row.get("source") or "paper_minimal_startup"),
                )
        reasons.append("PAPER_MINIMAL_STARTUP_V1 is allowed from current broker, order, price, profile, route, and size facts.")
        return _readiness_result(
            generated_at=generated_at,
            state="READY_SUBMIT_CAPABLE",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    managed_exit_pending = _shared_truth_has_managed_exit_pending(execution_core_shared_truth)
    managed_exit_due = _shared_truth_has_managed_exit_due(execution_core_shared_truth)
    managed_exit_submit_allowed = (
        (managed_exit_pending or managed_exit_due)
        and _bool(broker_truth_lease.get("available"))
        and _bool(broker_truth_lease.get("submit_exit_allowed"))
        and _bool(broker_truth_lease.get("broker_reconciled"))
        and int(broker_truth_lease.get("unknown_broker_open_order_count") or 0) == 0
        and int(broker_truth_lease.get("review_required_count") or 0) == 0
    )
    shared_truth_decision = _execution_core_shared_truth_decision(
        execution_core_shared_truth,
        allow_managed_exit_pending=managed_exit_submit_allowed,
    )
    for warning in shared_truth_decision["warnings"]:
        warn(
            str(warning.get("code") or "execution_core_shared_truth_warning"),
            str(warning.get("detail") or "Execution-core shared truth warning."),
            source=str(warning.get("source") or "execution_core_shared_truth"),
        )
    if shared_truth_decision["blockers"]:
        primary = shared_truth_decision["blockers"][0]
        block(
            str(primary.get("code") or "execution_core_shared_truth_blocked"),
            str(primary.get("detail") or "Execution-core shared truth blocked submit-capable readiness."),
            source=str(primary.get("source") or "execution_core_shared_truth"),
        )
        return _readiness_result(
            generated_at=generated_at,
            state=str(primary.get("state") or "NOT_READY_DEPENDENCY"),
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

    lifecycle_open_position_count = int(reconciliation.get("lifecycle_open_position_count") or 0)
    reconciliation_has_allowed_managed_exit = managed_exit_submit_allowed and lifecycle_open_position_count > 0
    if (
        not _bool(reconciliation.get("available"))
        or not _bool(reconciliation.get("fresh"))
        or not _reconciliation_classification_clean(str(reconciliation.get("classification") or ""))
        or not _bool(reconciliation.get("broker_reconciled"))
        or int(reconciliation.get("review_required_count") or 0) != 0
        or (lifecycle_open_position_count != 0 and not reconciliation_has_allowed_managed_exit)
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

    control_plane_available = _bool(control_plane_authorization.get("available"))
    control_plane_fresh = _bool(control_plane_authorization.get("fresh"))
    control_plane_coherent = str(control_plane_authorization.get("shared_truth_coherence_status") or "") == "COHERENT"
    if not control_plane_available:
        block(
            "control_plane_snapshot_missing",
            "Execution-core Control Plane Snapshot authority is missing; bridge pre-action authorization would fail.",
            source="control_plane_authorization",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_DEPENDENCY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )
    if not control_plane_fresh:
        block(
            "control_plane_snapshot_stale",
            "Execution-core Control Plane Snapshot authority is stale for bridge pre-action authorization.",
            source="control_plane_authorization",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_DEPENDENCY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )
    if not control_plane_coherent:
        block(
            "control_plane_snapshot_incoherent",
            "Execution-core Control Plane Snapshot authority is not coherent.",
            source="control_plane_authorization",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_DEPENDENCY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )
    if _bool(control_plane_authorization.get("live_money_eligible")):
        block(
            "control_plane_live_money_eligible_true",
            "Control Plane Snapshot exposed live_money_eligible=true.",
            source="control_plane_authorization",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_CONFIG",
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
    if runtime_truth_heartbeat.get("available") is True and runtime_truth_heartbeat.get("fresh") is False:
        warn(
            "runtime_truth_heartbeat_stale",
            "PAPER runtime truth heartbeat is present but stale; canonical readiness keeps existing submit gates authoritative.",
            source="runtime_truth_heartbeat",
        )

    loaded_lane_count = int(runtime.get("loaded_lane_count") or 0)
    eligible_lane_count = int(runtime.get("eligible_lane_count") or 0)
    live_bars_fresh = _bool(market_data.get("fresh"))
    if not live_bars_fresh and _proof_readiness_allows_market_data_startup(
        execution_core_shared_truth=execution_core_shared_truth,
        market_data=market_data,
    ):
        warn(
            "market_data_freshness_delegated_to_proof_readiness",
            "Proof readiness is clean; required listener freshness blockers are startup warnings only.",
            source="execution_core_proof_readiness",
        )
        live_bars_fresh = True
    scheduled_market_data_wait = _scheduled_market_data_wait(market_data)
    if _proof_readiness_market_closed(execution_core_shared_truth):
        live_bars_fresh = False
        scheduled_market_data_wait = True
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
    if managed_exit_submit_allowed:
        warn(
            "managed_exit_due_submit_capable" if managed_exit_due else "managed_exit_pending_submit_capable",
            "A broker-backed managed position is awaiting its governed exit; submit capability is limited by downstream lifecycle/exposure/bridge gates.",
            source="execution_core_shared_truth",
        )

    if not live_bars_fresh and scheduled_market_data_wait and not runtime_running:
        market_schedule_state = str(schedule.get("market_schedule_state") or MARKET_SCHEDULED_HALT)
        reasons.append(
            "Fresh market data is not expected during the scheduled futures market halt/reopen grace window."
        )
        reasons.append("Dependencies are clean enough to start the PAPER runtime in diagnostic-only mode.")
        warn(
            "scheduled_market_halt_runtime_start_allowed",
            "Runtime start is allowed before reopen so listeners and authority refresh can warm up; submit remains disabled.",
            source="market_data",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="READY_TO_START_DIAGNOSTIC_ONLY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
            market_schedule_override={
                "market_schedule_state": market_schedule_state,
                "stale_market_data_expected": True,
                "next_expected_reopen_time": schedule.get("next_expected_reopen_time"),
                "market_data_grace_until": schedule.get("market_data_grace_until"),
                "readiness_block_is_scheduled_halt": True,
            },
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

    if not live_bars_fresh and scheduled_market_data_wait:
        market_schedule_state = str(schedule.get("market_schedule_state") or MARKET_SCHEDULED_HALT)
        reasons.append(
            "Fresh market data is not expected during the scheduled futures market halt/reopen grace window."
        )
        warn(
            "scheduled_market_halt_waiting_for_fresh_bars",
            "Fresh candles are intentionally not required until the scheduled futures market reopens and the grace period expires.",
            source="market_data",
        )
        return _readiness_result(
            generated_at=generated_at,
            state="WAITING_FOR_MARKET_REOPEN",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
            market_schedule_override={
                "market_schedule_state": market_schedule_state,
                "stale_market_data_expected": True,
                "next_expected_reopen_time": schedule.get("next_expected_reopen_time"),
                "market_data_grace_until": schedule.get("market_data_grace_until"),
                "readiness_block_is_scheduled_halt": True,
            },
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

    if not _bool(runtime.get("runtime_ingestion_fresh")):
        block(
            "runtime_ingestion_not_fresh",
            "PAPER runtime has eligible lanes but has not ingested a fresh runtime bar; Phase-1 producer freshness alone is not submit-capable proof.",
            source="runtime",
            **_runtime_ingestion_not_fresh_context(runtime=runtime, market_data=market_data),
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_DEPENDENCY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    if exit_coverage_blocks_submit(strategy_exit_coverage):
        blocked_lanes = list(strategy_exit_coverage.get("blocked_lanes") or [])
        block(
            "strategy_exit_coverage_incomplete",
            "Every submit-capable Track B PAPER lane must have complete managed-exit coverage before new entries are allowed.",
            source="strategy_exit_coverage",
            blocked_lanes=blocked_lanes,
            strategy_exit_coverage_classification=strategy_exit_coverage.get("classification"),
        )
        return _readiness_result(
            generated_at=generated_at,
            state="NOT_READY_DEPENDENCY",
            reasons=reasons,
            blockers=blockers,
            warnings=warnings,
            inputs=inputs,
        )

    if not _broker_session_new_entry_allowed(broker_session_authority):
        authority_blockers = [
            dict(row)
            for row in list(broker_session_authority.get("authority_blockers") or [])
            if isinstance(row, Mapping)
        ]
        block(
            "BROKER_SESSION_NEW_ENTRY_NOT_ALLOWED",
            "Broker Session Authority does not allow new Track B PAPER entries.",
            source="broker_session_authority",
            broker_session_authority_classification=broker_session_authority.get("classification"),
            broker_session_connection_mode=broker_session_authority.get("connection_mode"),
            broker_session_authority_blockers=authority_blockers,
        )
        return _readiness_result(
            generated_at=generated_at,
            state="DEGRADED_NO_SUBMIT",
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
    broker_truth_lease_artifact = _effective_broker_truth_lease_artifact(artifacts, repo_root=repo_root, now=now)
    broker_truth_lease = _broker_truth_lease_input(broker_truth_lease_artifact, now=now)
    broker_session_authority = _broker_session_authority_input(
        _mapping(artifacts.get("broker_session_authority"))
    )
    reconciliation = _reconciliation_input(_mapping(artifacts.get("phase1_reconciliation")), now=now)
    operator_status = _operator_status_with_lane_artifacts(
        _mapping(artifacts.get("operator_status")),
        artifacts.get("lane_operator_statuses"),
    )
    config_in_force = _mapping(artifacts.get("config_in_force"))
    lane_quarantine = _lane_quarantine_input(_mapping(artifacts.get("lane_quarantine")))
    runtime = _runtime_input(operator_status, config_in_force, root_guard, now=now)
    runtime_truth_heartbeat = _runtime_truth_heartbeat_input(
        _mapping(artifacts.get("paper_runtime_truth")),
        now=now,
    )
    market_data = _market_data_input(
        operator_status,
        _mapping(artifacts.get("market_data_probe")),
        _mapping(artifacts.get("phase1_databento_live_listener_status")),
        repo_root=repo_root,
        now=now,
    )
    runtime = _runtime_with_phase1_relative_ingestion(runtime, market_data)
    execution_core_shared_truth = _execution_core_shared_truth_input(artifacts, now=now)
    control_plane_authorization = _control_plane_authorization_input(
        _mapping(artifacts.get("control_plane_snapshot")),
        now=now,
    )
    strategy_exit_coverage = build_track_b_strategy_exit_coverage_report(
        config=TrackBStrategyExitCoverageConfig(repo_root=repo_root),
        now=now,
        config_in_force=config_in_force,
    )
    paper_minimal_startup = build_track_b_paper_minimal_startup(
        config=TrackBPaperMinimalStartupConfig(repo_root=repo_root),
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
            runtime_truth_heartbeat,
            execution_core_shared_truth,
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
        "runtime_truth_heartbeat": runtime_truth_heartbeat,
        "broker_truth": broker_truth,
        "broker_truth_lease": broker_truth_lease,
        "broker_session_authority": broker_session_authority,
        "phase1_reconciliation": reconciliation,
        "execution_core_shared_truth": execution_core_shared_truth,
        "control_plane_authorization": control_plane_authorization,
        "strategy_exit_coverage": strategy_exit_coverage,
        "paper_minimal_startup": paper_minimal_startup,
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
    _write_refreshed_broker_truth_lease_if_present(repo_root=repo_root, payload=payload)
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
        "lane_operator_statuses": _read_lane_operator_statuses(paper_root / "lanes"),
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
        "broker_session_authority": _read_json(repo_root / DEFAULT_BROKER_SESSION_AUTHORITY_ARTIFACT),
        "phase1_databento_live_listener_status": _read_json(
            repo_root / DEFAULT_PHASE1_DATABENTO_LIVE_LISTENER_STATUS_ARTIFACT
        ),
        "paper_runtime_truth": _read_json(repo_root / DEFAULT_PAPER_RUNTIME_TRUTH_ARTIFACT),
        "lifecycle_status": _read_json(repo_root / DEFAULT_LIFECYCLE_STATUS_ARTIFACT),
        "order_state": _read_json(repo_root / DEFAULT_ORDER_STATE_ARTIFACT),
        "proof_readiness": _read_json(repo_root / DEFAULT_PROOF_READINESS_ARTIFACT),
        "open_order_truth": _read_json(repo_root / DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT),
        "managed_order_registry": _read_json(repo_root / DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT),
        "order_adjustment_plan": _read_json(repo_root / DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT),
        "position_truth": _read_json(repo_root / DEFAULT_POSITION_TRUTH_ARTIFACT),
        "runtime_environment_truth": _read_json(repo_root / DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT),
        "managed_position_registry": _read_json(repo_root / DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT),
        "control_plane_snapshot": _read_json(repo_root / DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT),
        "strategy_exit_coverage": _read_json(repo_root / DEFAULT_STRATEGY_EXIT_COVERAGE_ARTIFACT),
    }


def _read_lane_operator_statuses(lanes_root: Path) -> list[dict[str, Any]]:
    if not lanes_root.exists():
        return []
    rows: list[dict[str, Any]] = []
    for path in sorted(lanes_root.glob("*/operator_status.json")):
        payload = _read_json(path)
        if payload:
            rows.append(payload)
    return rows


def _operator_status_with_lane_artifacts(
    operator_status: Mapping[str, Any],
    lane_operator_statuses: Any,
) -> dict[str, Any]:
    merged = dict(operator_status)
    lane_rows = [dict(row) for row in list(merged.get("lanes") or []) if isinstance(row, Mapping)]
    if not isinstance(lane_operator_statuses, list) or not lane_rows:
        return merged
    lane_by_id: dict[str, dict[str, Any]] = {
        str(row.get("lane_id") or row.get("id") or "").strip(): row
        for row in lane_rows
        if str(row.get("lane_id") or row.get("id") or "").strip()
    }
    for lane_payload in lane_operator_statuses:
        if not isinstance(lane_payload, Mapping):
            continue
        lane_id = str(lane_payload.get("lane_id") or lane_payload.get("id") or "").strip()
        current = lane_by_id.get(lane_id)
        if current is None:
            continue
        current_ts = _parse_iso(current.get("last_processed_bar_end_ts"))
        candidate_ts = _parse_iso(lane_payload.get("last_processed_bar_end_ts"))
        if candidate_ts is None or (current_ts is not None and candidate_ts <= current_ts):
            continue
        lane_by_id[lane_id] = {**current, **dict(lane_payload)}
    merged_lanes = [lane_by_id.get(str(row.get("lane_id") or row.get("id") or "").strip(), row) for row in lane_rows]
    merged["lanes"] = merged_lanes
    latest = _latest_runtime_processed_bar_ts(merged)
    if latest is not None:
        merged["last_processed_bar_end_ts"] = latest
    return merged


def _control_plane_authorization_input(payload: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    age_seconds = _age_seconds(payload.get("generated_at"), now)
    fresh = bool(age_seconds is not None and age_seconds <= CONTROL_PLANE_SNAPSHOT_MAX_AGE_SECONDS)
    return {
        "available": bool(payload),
        "fresh": fresh,
        "generated_at": payload.get("generated_at"),
        "age_seconds": age_seconds,
        "max_age_seconds": CONTROL_PLANE_SNAPSHOT_MAX_AGE_SECONDS,
        "source_artifact_path": str(DEFAULT_CONTROL_PLANE_SNAPSHOT_ARTIFACT),
        "control_plane_snapshot_id": payload.get("control_plane_snapshot_id"),
        "shared_truth_refresh_generation_id": payload.get("shared_truth_refresh_generation_id"),
        "runtime_supervisor_decision_id": payload.get("runtime_supervisor_decision_id"),
        "shared_truth_coherence_status": payload.get("shared_truth_coherence_status"),
        "classification": payload.get("classification"),
        "live_money_eligible": payload.get("live_money_eligible") is True,
        "bridge_pre_action_authority": True,
    }


def _effective_broker_truth_lease_artifact(
    artifacts: Mapping[str, Any], *, repo_root: Path, now: datetime
) -> dict[str, Any]:
    """Return a broker lease derived from current source truth when possible.

    The broker-truth lease is a derived safety artifact. Canonical readiness
    should not stay blocked by an older lease file after newer authoritative
    broker truth and reconciliation artifacts have already converged cleanly.
    Recomputing the lease here keeps the same fail-closed lease classifier for
    stale or unsafe broker truth while removing stale derived-artifact drift.
    """

    broker_status = _mapping(artifacts.get("broker_truth_status"))
    reconciliation = _mapping(artifacts.get("phase1_reconciliation"))
    existing_lease = _mapping(artifacts.get("broker_truth_lease"))
    if _published_broker_authority_lease_healthy(existing_lease, now=now):
        return existing_lease
    if not broker_status and not reconciliation:
        return existing_lease

    latest_attempt = _mapping(broker_status.get("latest_attempt_status"))
    last_success = _broker_truth_with_connection_report(
        repo_root=repo_root,
        broker_status=broker_status,
        broker_truth=_mapping(broker_status.get("last_successful_broker_truth")) or broker_status,
    )
    lease = classify_broker_truth_lease(
        {
            "account_id": "DUM882026",
            "allowed_instruments": ["MGC", "MNQ", "GC"],
            "current_time": now.isoformat(),
            "policy": {
                "max_entry_age_seconds": 300.0,
                "max_exit_age_seconds": 900.0,
                "degraded_refresh_grace_seconds": 120.0,
            },
            "last_successful_broker_truth": last_success,
            "latest_attempt_status": latest_attempt,
            "reconciliation": reconciliation,
            "lifecycle": _lifecycle_summary(_mapping(artifacts.get("lifecycle_status"))),
            "order_state": _order_state_summary(_mapping(artifacts.get("order_state")), reconciliation),
            "source_artifact_paths": {
                "broker_truth_status": str(
                    Path("outputs")
                    / "reports"
                    / "ibkr_read_only_verification"
                    / "ibkr_broker_truth_refresh_status.json"
                ),
                "reconciliation": str(
                    Path("outputs")
                    / "reports"
                    / "track_b_paper_broker_reconciliation"
                    / "latest_track_b_paper_broker_reconciliation.json"
                ),
                "lifecycle": str(DEFAULT_LIFECYCLE_STATUS_ARTIFACT),
                "order_state": str(DEFAULT_ORDER_STATE_ARTIFACT),
            },
            "source_artifact_timestamps": {
                key: value
                for key, value in {
                    "broker_truth_status": _artifact_generated_at(broker_status),
                    "latest_attempt": _artifact_generated_at(latest_attempt),
                    "reconciliation": _artifact_generated_at(reconciliation),
                    "lifecycle": _artifact_generated_at(_mapping(artifacts.get("lifecycle_status"))),
                    "order_state": _artifact_generated_at(_mapping(artifacts.get("order_state"))),
                    "previous_lease": _artifact_generated_at(existing_lease),
                }.items()
                if value is not None
            },
        }
    )
    lease["refreshed_by_canonical_readiness"] = True
    lease["previous_lease_state"] = existing_lease.get("lease_state") or existing_lease.get("state")
    lease["previous_lease_generated_at"] = existing_lease.get("generated_at")
    return lease


def _published_broker_authority_lease_healthy(payload: Mapping[str, Any], *, now: datetime) -> bool:
    if not payload:
        return False
    if str(payload.get("authority_writer") or "") != "ibkr_broker_truth_refresher":
        return False
    lease_state = str(payload.get("lease_state") or "").upper()
    if lease_state not in BROKER_TRUTH_LEASE_READY_STATES:
        return False
    entry_valid_until = payload.get("entry_valid_until") or payload.get("valid_until")
    seconds_remaining = _seconds_until(entry_valid_until, now)
    return bool(seconds_remaining is not None and seconds_remaining > 0)


def _broker_truth_with_connection_report(
    *,
    repo_root: Path,
    broker_status: Mapping[str, Any],
    broker_truth: Mapping[str, Any],
) -> dict[str, Any]:
    result = dict(broker_truth)
    connection_report_path = result.get("connection_report_path") or broker_status.get("connection_report_path")
    connection_report = _read_json(_repo_scoped_optional_path(repo_root, connection_report_path))
    connection_check = _mapping(connection_report.get("connection_check"))
    if not connection_check:
        return result
    result.setdefault("connection_check", connection_check)
    result.setdefault("server_version", connection_check.get("server_version"))
    result.setdefault(
        "submit_session_readiness",
        {
            "source": "ibkr_read_only_connection_report",
            "client_id": connection_check.get("client_id"),
            "connected": connection_check.get("connected") is True,
            "server_version": connection_check.get("server_version"),
            "connection_started_at": connection_check.get("connection_timestamp") or connection_report.get("started_at"),
        },
    )
    return result


def _repo_scoped_optional_path(repo_root: Path, value: Any) -> Path:
    text = str(value or "").strip()
    if not text:
        return repo_root / "__missing__"
    path = Path(text).expanduser()
    return path if path.is_absolute() else repo_root / path


def _write_refreshed_broker_truth_lease_if_present(*, repo_root: Path, payload: Mapping[str, Any]) -> None:
    # Canonical readiness may recompute an effective lease for its own
    # dependency decision, but broker truth lease publication belongs to the
    # broker truth/BSA publisher.  Rewriting the canonical lease here creates
    # mixed-generation authority artifacts.
    _ = repo_root, payload
    return


def _execution_core_shared_truth_input(artifacts: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    proof_readiness = _mapping(artifacts.get("proof_readiness"))
    open_order_truth = _mapping(artifacts.get("open_order_truth"))
    managed_order_registry = _mapping(artifacts.get("managed_order_registry"))
    order_adjustment_plan = _mapping(artifacts.get("order_adjustment_plan"))
    position_truth = _mapping(artifacts.get("position_truth"))
    position_truth_summary = _mapping(position_truth.get("summary"))
    runtime_environment_truth = _mapping(artifacts.get("runtime_environment_truth"))
    managed_position_registry = _mapping(artifacts.get("managed_position_registry"))
    proof_age_seconds = _age_seconds(proof_readiness.get("generated_at"), now)
    proof_classifications = (
        _mapping(proof_readiness.get("shared_truth_classifications"))
        if proof_age_seconds is not None and proof_age_seconds <= PROOF_CLASSIFICATION_MAX_AGE_SECONDS
        else {}
    )
    classifications = {
        "Open Order Truth": _classification(open_order_truth)
        or proof_classifications.get("Open Order Truth"),
        "Managed Order Registry": _classification(managed_order_registry)
        or proof_classifications.get("Managed Order Registry"),
        "Order Adjustment Planner": _classification(order_adjustment_plan),
        "Position Truth": _classification(position_truth, "overall_classification", "classification")
        or _classification(position_truth_summary, "overall_classification", "classification")
        or proof_classifications.get("Position Truth"),
        "Runtime Environment Truth": _classification(runtime_environment_truth)
        or proof_classifications.get("Runtime Environment Truth"),
        "Managed Position Registry": _classification(managed_position_registry)
        or proof_classifications.get("Managed Position Registry"),
        "Reconciliation": proof_classifications.get("Reconciliation"),
        "Broker Truth Lease": proof_classifications.get("Broker Truth Lease"),
    }
    available = any(
        bool(payload)
        for payload in (
            proof_readiness,
            open_order_truth,
            managed_order_registry,
            order_adjustment_plan,
            position_truth,
            runtime_environment_truth,
            managed_position_registry,
        )
    )
    return {
        "available": available,
        "evidence_only": True,
        "readiness_authority": True,
        "source": "execution_core_authority",
        "generated_at": proof_readiness.get("generated_at"),
        "proof_readiness": {
            "available": bool(proof_readiness),
            "classification": proof_readiness.get("classification"),
            "ready_for_proof": proof_readiness.get("ready_for_proof") is True,
            "primary_blocker": _mapping(proof_readiness.get("primary_blocker")),
            "secondary_warnings": list(proof_readiness.get("secondary_warnings") or []),
            "broker_lease_warning": _mapping(proof_readiness.get("broker_lease_warning")),
            "phase1_session_reason": proof_readiness.get("phase1_session_reason"),
            "blockers": list(proof_readiness.get("blockers") or []),
            "age_seconds": proof_age_seconds,
        },
        "classifications": {key: value for key, value in classifications.items() if value},
        "artifact_paths": {
            "proof_readiness": str(DEFAULT_PROOF_READINESS_ARTIFACT),
            "open_order_truth": str(DEFAULT_OPEN_ORDER_TRUTH_ARTIFACT),
            "managed_order_registry": str(DEFAULT_MANAGED_ORDER_REGISTRY_ARTIFACT),
            "order_adjustment_plan": str(DEFAULT_ORDER_ADJUSTMENT_PLAN_ARTIFACT),
            "position_truth": str(DEFAULT_POSITION_TRUTH_ARTIFACT),
            "runtime_environment_truth": str(DEFAULT_RUNTIME_ENVIRONMENT_TRUTH_ARTIFACT),
            "managed_position_registry": str(DEFAULT_MANAGED_POSITION_REGISTRY_ARTIFACT),
            "strategy_exit_coverage": str(DEFAULT_STRATEGY_EXIT_COVERAGE_ARTIFACT),
        },
        "live_money_eligible": proof_readiness.get("live_money_eligible") is True,
    }


def _shared_truth_has_managed_exit_pending(evidence: Mapping[str, Any]) -> bool:
    classifications = _mapping(evidence.get("classifications"))
    return (
        str(classifications.get("Open Order Truth") or "") == "BROKER_POSITION_WITHOUT_CLOSE_ORDER"
        and str(classifications.get("Managed Order Registry") or "") == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
        and str(classifications.get("Position Truth") or "") == "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING"
        and str(classifications.get("Managed Position Registry") or "") == "OPEN_MANAGED_MATCHED"
        and str(classifications.get("Order Adjustment Planner") or "") == "NO_ACTION_NEEDED"
    )


def _shared_truth_has_managed_exit_due(evidence: Mapping[str, Any]) -> bool:
    classifications = _mapping(evidence.get("classifications"))
    return (
        str(classifications.get("Open Order Truth") or "") == "BROKER_POSITION_WITHOUT_CLOSE_ORDER"
        and str(classifications.get("Managed Order Registry") or "") == "POSITION_WITHOUT_CLOSE_ORDER"
        and str(classifications.get("Position Truth") or "") == "ATTENTION_REQUIRED"
        and str(classifications.get("Managed Position Registry") or "") == "OPEN_MANAGED_EXIT_DUE"
        and str(classifications.get("Order Adjustment Planner") or "") in {"NO_ACTION_NEEDED", "ORDER_NOT_FOUND"}
    )


def _execution_core_shared_truth_decision(
    evidence: Mapping[str, Any],
    *,
    allow_managed_exit_pending: bool = False,
) -> dict[str, list[dict[str, Any]]]:
    if not evidence or evidence.get("available") is not True:
        return {"blockers": [], "warnings": []}

    proof = _mapping(evidence.get("proof_readiness"))
    proof_classification = str(proof.get("classification") or "")
    classifications = _mapping(evidence.get("classifications"))
    blockers: list[dict[str, Any]] = []
    warnings: list[dict[str, Any]] = []

    broker_lease_warning = _mapping(proof.get("broker_lease_warning"))
    for warning in list(proof.get("secondary_warnings") or []):
        if isinstance(warning, Mapping):
            warnings.append(
                {
                    "code": warning.get("code") or "proof_readiness_warning",
                    "detail": warning.get("detail") or "Proof-readiness warning.",
                    "source": "execution_core_proof_readiness",
                }
            )
    if broker_lease_warning:
        warnings.append(
            {
                "code": broker_lease_warning.get("code") or "broker_truth_lease_warning",
                "detail": broker_lease_warning.get("detail") or "Broker Truth Lease warning.",
                "source": "execution_core_broker_truth_lease",
            }
        )

    if proof_classification == MARKET_CLOSED_NO_FRESH_BARS:
        warnings.append(
            {
                "code": MARKET_CLOSED_NO_FRESH_BARS,
                "detail": "Phase-1 runtime candles are stale because the market/session is closed; scheduled-halt classification decides submit readiness.",
                "source": "execution_core_proof_readiness",
            }
        )
        return {"blockers": blockers, "warnings": warnings}

    lease_state = str(classifications.get("Broker Truth Lease") or broker_lease_warning.get("lease_state") or "")
    if lease_state == "ACTIVE_DEGRADED_REFRESH_FAILING":
        warnings.append(
            {
                "code": "broker_truth_lease_degraded_refresh_failing",
                "detail": "Broker Truth Lease is ACTIVE_DEGRADED_REFRESH_FAILING; broker truth remains usable while fresh and reconciled, but refresh degradation is diagnostic.",
                "source": "execution_core_broker_truth_lease",
            }
        )

    reconciliation_classification = str(classifications.get("Reconciliation") or "")
    if reconciliation_classification and not _reconciliation_classification_clean(reconciliation_classification):
        blockers.append(
            {
                "code": "execution_core_reconciliation_not_clean",
                "detail": f"Execution-core reconciliation is {reconciliation_classification}; submit-capable readiness is blocked.",
                "source": "execution_core_reconciliation",
                "state": "NOT_READY_RECONCILIATION",
            }
        )
        return {"blockers": blockers, "warnings": warnings}

    expected_clean: dict[str, str | set[str]] = {
        "Open Order Truth": "NO_OPEN_ORDERS",
        "Managed Order Registry": "NO_MANAGED_ORDERS",
        "Order Adjustment Planner": "NO_ACTION_NEEDED",
        "Position Truth": "CLEAN_FLAT_READY",
        "Managed Position Registry": "NO_MANAGED_POSITIONS",
    }
    if allow_managed_exit_pending:
        expected_clean.update(
            {
                "Open Order Truth": {"NO_OPEN_ORDERS", "BROKER_POSITION_WITHOUT_CLOSE_ORDER"},
                "Managed Order Registry": {
                    "NO_MANAGED_ORDERS",
                    "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING",
                    "POSITION_WITHOUT_CLOSE_ORDER",
                },
                "Position Truth": {"CLEAN_FLAT_READY", "ACTIVE_HOLD_MANAGED_TIMED_EXIT_PENDING", "ATTENTION_REQUIRED"},
                "Managed Position Registry": {"NO_MANAGED_POSITIONS", "OPEN_MANAGED_MATCHED", "OPEN_MANAGED_EXIT_DUE"},
                "Order Adjustment Planner": {"NO_ACTION_NEEDED", "ORDER_NOT_FOUND"},
            }
        )
    for service, expected in expected_clean.items():
        observed = str(classifications.get(service) or "")
        expected_values = expected if isinstance(expected, set) else {expected}
        if observed and observed not in expected_values:
            expected_detail = "/".join(sorted(expected_values))
            blockers.append(
                {
                    "code": f"{service.lower().replace(' ', '_')}_not_clean",
                    "detail": f"{service} is {observed}; expected {expected_detail} for submit-capable readiness.",
                    "source": "execution_core_shared_truth",
                    "state": "NOT_READY_DEPENDENCY",
                }
            )
            return {"blockers": blockers, "warnings": warnings}

    runtime_classification = str(classifications.get("Runtime Environment Truth") or "")
    runtime_allowed = {
        "",
        "RUNTIME_DOWN_CLEAN",
        "RUNTIME_ACTIVE_TRADE_CAPABLE",
        "RUNTIME_ACTIVE_OBSERVATION_ONLY",
    }
    if runtime_classification not in runtime_allowed:
        blockers.append(
            {
                "code": "runtime_environment_truth_not_clean",
                "detail": f"Runtime Environment Truth is {runtime_classification}; submit-capable readiness is blocked.",
                "source": "execution_core_runtime_environment_truth",
                "state": "NOT_READY_DEPENDENCY",
            }
        )
        return {"blockers": blockers, "warnings": warnings}

    if proof_classification == "PHASE1_DATA_UNHEALTHY":
        blockers.append(
            {
                "code": "phase1_data_unhealthy",
                "detail": "Proof-readiness Phase-1 checks report unhealthy runtime candle data.",
                "source": "execution_core_proof_readiness",
                "state": "NOT_READY_DEPENDENCY",
            }
        )
    elif proof_classification in {"BROKER_STATE_UNSAFE", "SHARED_TRUTH_BLOCKED"}:
        primary = _mapping(proof.get("primary_blocker"))
        blockers.append(
            {
                "code": primary.get("code") or "execution_core_shared_truth_blocked",
                "detail": primary.get("detail") or f"Proof readiness is {proof_classification}.",
                "source": "execution_core_proof_readiness",
                "state": "NOT_READY_DEPENDENCY",
            }
        )

    return {"blockers": blockers, "warnings": warnings}


def _reconciliation_classification_clean(classification: str) -> bool:
    return classification in RECONCILIATION_CLEAN_CLASSIFICATIONS


def _proof_readiness_allows_market_data_startup(
    *,
    execution_core_shared_truth: Mapping[str, Any],
    market_data: Mapping[str, Any],
) -> bool:
    proof = _mapping(execution_core_shared_truth.get("proof_readiness"))
    if proof.get("classification") != "READY_FOR_PROOF":
        return False
    if market_data.get("listener_global_issue") is True:
        return False
    blockers = [row for row in list(market_data.get("blockers") or []) if isinstance(row, Mapping)]
    if not blockers:
        return False
    return all(str(row.get("code") or "") == "market_data_not_fresh" for row in blockers)


def _scheduled_market_data_wait(market_data: Mapping[str, Any]) -> bool:
    schedule = _mapping(market_data.get("market_schedule"))
    return bool(
        market_data.get("stale_market_data_expected") is True
        or schedule.get("stale_market_data_expected") is True
        or schedule.get("readiness_block_is_scheduled_halt") is True
    )


def _proof_readiness_market_closed(execution_core_shared_truth: Mapping[str, Any]) -> bool:
    proof = _mapping(execution_core_shared_truth.get("proof_readiness"))
    return str(proof.get("classification") or "") == MARKET_CLOSED_NO_FRESH_BARS


def _market_schedule_fields(market_data: Mapping[str, Any]) -> dict[str, Any]:
    schedule = _mapping(market_data.get("market_schedule"))
    return {
        "market_schedule_state": schedule.get("market_schedule_state") or market_data.get("market_schedule_state"),
        "stale_market_data_expected": bool(
            schedule.get("stale_market_data_expected") is True
            or market_data.get("stale_market_data_expected") is True
        ),
        "next_expected_reopen_time": schedule.get("next_expected_reopen_time")
        or market_data.get("next_expected_reopen_time"),
        "market_data_grace_until": schedule.get("market_data_grace_until")
        or market_data.get("market_data_grace_until"),
        "readiness_block_is_scheduled_halt": bool(
            schedule.get("readiness_block_is_scheduled_halt") is True
            or market_data.get("readiness_block_is_scheduled_halt") is True
        ),
    }


def _classification(payload: Mapping[str, Any], *keys: str) -> str | None:
    for key in (*keys, "classification"):
        value = payload.get(key)
        if value is not None and str(value).strip():
            return str(value)
    return None


def _lifecycle_summary(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {}
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


def _artifact_generated_at(payload: Mapping[str, Any]) -> str | None:
    value = payload.get("generated_at") or payload.get("last_success_at") or payload.get("latest_refresh_time")
    return str(value) if value is not None else None


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
    *,
    now: datetime,
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
    last_processed_bar_end_ts = _latest_runtime_processed_bar_ts(operator_status)
    ingestion_age_seconds = _age_seconds(last_processed_bar_end_ts, now)
    ingestion_threshold = MARKET_DATA_FRESHNESS_DEFAULT_SECONDS
    runtime_ingestion_fresh = bool(
        ingestion_age_seconds is not None and ingestion_age_seconds <= ingestion_threshold
    )
    runtime_pid = _first_int(operator_status.get("source_runtime_pid"), config_in_force.get("source_runtime_pid"))
    runtime_commit = (
        operator_status.get("source_runtime_git_head")
        or operator_status.get("source_runtime_commit")
        or config_in_force.get("source_runtime_git_head")
        or config_in_force.get("source_runtime_commit")
    )
    profile = _runtime_profile(operator_status, config_in_force)
    lane_ingestion = _runtime_lane_ingestion_rows(lane_rows, now=now, threshold=ingestion_threshold)
    return {
        "running": process_running,
        "healthy": bool(process_running and not faulted and operator_status.get("operator_halt") is not True),
        "health_status": health.get("health_status"),
        "entries_enabled": operator_status.get("entries_enabled") is True,
        "operator_halt": operator_status.get("operator_halt") is True,
        "loaded_lane_count": len(active_lane_ids) or len(configured_lanes),
        "eligible_lane_count": eligible_count,
        "active_lane_ids": active_lane_ids,
        "runtime_pid": runtime_pid,
        "runtime_commit": runtime_commit,
        "profile": profile,
        "last_processed_bar_end_ts": last_processed_bar_end_ts,
        "latest_runtime_ingested_bar": last_processed_bar_end_ts,
        "ingestion_age_seconds": ingestion_age_seconds,
        "ingestion_freshness_threshold_seconds": ingestion_threshold,
        "runtime_lane_ingestion": lane_ingestion,
        "affected_lanes": [row["lane_id"] for row in lane_ingestion if row.get("fresh") is not True],
        "affected_symbols": sorted(
            {
                str(row.get("symbol") or "").strip().upper()
                for row in lane_ingestion
                if row.get("fresh") is not True and str(row.get("symbol") or "").strip()
            }
        ),
        "runtime_ingestion_fresh": runtime_ingestion_fresh,
        "live_money_eligible": operator_status.get("live_money_eligible") is True,
    }


def _runtime_lane_ingestion_rows(
    lane_rows: Sequence[Mapping[str, Any]],
    *,
    now: datetime,
    threshold: float,
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for lane in lane_rows:
        lane_id = str(lane.get("lane_id") or lane.get("id") or "").strip()
        if not lane_id:
            continue
        latest = (
            lane.get("last_processed_bar_end_ts")
            or lane.get("last_execution_bar_evaluated_at")
            or lane.get("latest_completed_bar_end_ts")
        )
        age_seconds = _age_seconds(latest, now)
        symbol = str(lane.get("symbol") or lane.get("instrument") or "").strip().upper()
        rows.append(
            {
                "lane_id": lane_id,
                "symbol": symbol or None,
                "latest_runtime_ingested_bar": latest,
                "age_seconds": age_seconds,
                "threshold_seconds": threshold,
                "fresh": bool(age_seconds is not None and age_seconds <= threshold),
            }
        )
    return rows


def _runtime_with_phase1_relative_ingestion(
    runtime: Mapping[str, Any],
    market_data: Mapping[str, Any],
) -> dict[str, Any]:
    updated = dict(runtime)
    threshold = _float_or_none(updated.get("ingestion_freshness_threshold_seconds"))
    if threshold is None:
        return updated
    phase1_latest = _latest_phase1_bar_by_symbol_timeframe(market_data)
    latest_phase1_dt = _latest_phase1_datetime(phase1_latest)
    latest_runtime_dt = _parse_iso(updated.get("latest_runtime_ingested_bar") or updated.get("last_processed_bar_end_ts"))
    if latest_phase1_dt is None or latest_runtime_dt is None:
        return updated
    lag_seconds = max(0.0, (latest_phase1_dt - latest_runtime_dt).total_seconds())
    updated["ingestion_lag_seconds"] = lag_seconds
    if lag_seconds <= threshold:
        updated["runtime_ingestion_fresh"] = True
        updated["affected_lanes"] = []
        updated["affected_symbols"] = []
        updated["runtime_lane_ingestion"] = [
            {
                **dict(row),
                "fresh": True,
                "phase1_relative_lag_seconds": max(
                    0.0,
                    (
                        latest_phase1_dt
                        - (_parse_iso(row.get("latest_runtime_ingested_bar")) or latest_runtime_dt)
                    ).total_seconds(),
                ),
            }
            for row in list(updated.get("runtime_lane_ingestion") or [])
            if isinstance(row, Mapping)
        ]
    return updated


def _runtime_profile(operator_status: Mapping[str, Any], config_in_force: Mapping[str, Any]) -> str | None:
    for payload in (operator_status, config_in_force):
        for key in ("profile", "runtime_profile", "track_b_paper_stack_profile", "paper_stack_profile"):
            value = str(payload.get(key) or "").strip()
            if value:
                return value
        command = str(payload.get("source_runtime_command") or "").strip()
        for token in command.split():
            name = Path(token).name
            if name.startswith("paper_stack_") and name.endswith(".yaml"):
                return name.removeprefix("paper_stack_").removesuffix(".yaml")
    return None


def _latest_runtime_processed_bar_ts(operator_status: Mapping[str, Any]) -> Any:
    candidates: list[tuple[datetime, Any]] = []

    def _add(value: Any) -> None:
        parsed = _parse_iso(value)
        if parsed is not None:
            candidates.append((parsed, value))

    _add(operator_status.get("last_processed_bar_end_ts"))
    for row in list(operator_status.get("lanes") or []):
        if isinstance(row, Mapping):
            _add(row.get("last_processed_bar_end_ts"))
    if not candidates:
        return operator_status.get("last_processed_bar_end_ts")
    candidates.sort(key=lambda item: item[0])
    return candidates[-1][1]


def _runtime_ingestion_not_fresh_context(
    *,
    runtime: Mapping[str, Any],
    market_data: Mapping[str, Any],
) -> dict[str, Any]:
    latest_runtime = runtime.get("latest_runtime_ingested_bar") or runtime.get("last_processed_bar_end_ts")
    phase1_latest = _latest_phase1_bar_by_symbol_timeframe(market_data)
    latest_phase1_dt = _latest_phase1_datetime(phase1_latest)
    latest_runtime_dt = _parse_iso(latest_runtime)
    if latest_phase1_dt is not None and latest_runtime_dt is not None:
        ingestion_lag_seconds = max(0.0, (latest_phase1_dt - latest_runtime_dt).total_seconds())
    else:
        ingestion_lag_seconds = runtime.get("ingestion_age_seconds")
    affected_lanes = list(runtime.get("affected_lanes") or [])
    affected_symbols = list(runtime.get("affected_symbols") or [])
    if not affected_symbols:
        affected_symbols = list(market_data.get("required_symbols") or market_data.get("active_required_symbols") or [])
    return {
        "classification": "RUNTIME_ALIVE_PHASE1_FRESH_RUNTIME_INGESTION_STALE",
        "dependency_status": "RUNTIME_INGESTION_STALE",
        "distinction_from_listener_feed_failure": (
            "Phase-1 listener/feed freshness is evaluated separately; this block means live bars are fresh "
            "but the PAPER runtime has not ingested/evaluated a fresh runtime bar."
        ),
        "latest_phase1_bar_by_symbol_timeframe": phase1_latest,
        "latest_runtime_ingested_bar": latest_runtime,
        "ingestion_lag_seconds": ingestion_lag_seconds,
        "threshold_seconds": runtime.get("ingestion_freshness_threshold_seconds"),
        "affected_symbols": affected_symbols,
        "affected_lanes": affected_lanes,
        "runtime_pid": runtime.get("runtime_pid"),
        "runtime_commit": runtime.get("runtime_commit"),
        "profile": runtime.get("profile"),
    }


def _latest_phase1_bar_by_symbol_timeframe(market_data: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    by_symbol: dict[str, dict[str, Any]] = {}
    scoped_symbols = {
        str(symbol).strip().upper()
        for symbol in list(market_data.get("active_required_symbols") or market_data.get("required_symbols") or [])
        if str(symbol).strip()
    }
    for row in list(market_data.get("rows") or []):
        if not isinstance(row, Mapping):
            continue
        symbol = str(row.get("symbol") or "").strip().upper()
        if not symbol:
            continue
        if scoped_symbols and symbol not in scoped_symbols:
            continue
        timeframe = _phase1_row_timeframe(row)
        by_symbol.setdefault(symbol, {})[timeframe] = row.get("latest_completed_bar_ts")
    return by_symbol


def _phase1_row_timeframe(row: Mapping[str, Any]) -> str:
    for key in ("timeframe", "bar_size", "interval"):
        value = str(row.get(key) or "").strip()
        if value:
            return value
    schema = str(row.get("schema") or "").strip().lower()
    if schema.startswith("ohlcv-"):
        return schema.removeprefix("ohlcv-")
    return "unknown"


def _latest_phase1_datetime(phase1_latest: Mapping[str, Mapping[str, Any]]) -> datetime | None:
    candidates: list[datetime] = []
    for timeframes in phase1_latest.values():
        if not isinstance(timeframes, Mapping):
            continue
        for value in timeframes.values():
            parsed = _parse_iso(value)
            if parsed is not None:
                candidates.append(parsed)
    return max(candidates) if candidates else None


def _runtime_truth_heartbeat_input(payload: Mapping[str, Any], *, now: datetime) -> dict[str, Any]:
    if not payload:
        return {
            "available": False,
            "fresh": False,
            "evidence_only": True,
            "readiness_authority": False,
            "restart_authority": False,
            "live_money_eligible": False,
        }
    timestamp = payload.get("last_success_at") or payload.get("generated_at")
    age_seconds = _age_seconds(timestamp, now)
    threshold = _float_value(payload.get("freshness_ttl_seconds"), MARKET_DATA_FRESHNESS_DEFAULT_SECONDS)
    fresh = bool(age_seconds is not None and age_seconds <= threshold)
    return {
        "available": True,
        "fresh": fresh,
        "generated_at": payload.get("generated_at"),
        "last_success_at": payload.get("last_success_at"),
        "age_seconds": age_seconds,
        "freshness_ttl_seconds": threshold,
        "heartbeat_state": payload.get("heartbeat_state"),
        "freshness_state": payload.get("freshness_state"),
        "writer_authority": payload.get("writer_authority"),
        "runtime_instance_id": payload.get("runtime_instance_id"),
        "restart_generation": payload.get("restart_generation"),
        "producer_pid": payload.get("producer_pid"),
        "producer_root": payload.get("producer_root"),
        "source_commit": payload.get("source_commit"),
        "lane_count": payload.get("lane_count"),
        "b_plus_threshold": payload.get("b_plus_threshold"),
        "test_mule_enabled": payload.get("test_mule_enabled"),
        "duplicate_writer_detection": _mapping(payload.get("duplicate_writer_detection")),
        "evidence_only": True,
        "readiness_authority": False,
        "restart_authority": False,
        "live_money_eligible": payload.get("live_money_eligible") is True,
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
    entry_seconds_remaining = _seconds_until(entry_valid_until, now)
    exit_seconds_remaining = _seconds_until(exit_valid_until, now)
    effective_lease_state = lease_state
    if lease_state in BROKER_TRUTH_LEASE_READY_STATES:
        if entry_seconds_remaining is not None and entry_seconds_remaining <= 0:
            effective_lease_state = "EXPIRED_EXITS_ONLY"
        if exit_seconds_remaining is not None and exit_seconds_remaining <= 0:
            effective_lease_state = "EXPIRED_BLOCK_NEW_ENTRIES"
    return {
        "available": True,
        "lease_state": effective_lease_state,
        "source_lease_state": lease_state,
        "authority_generation_id": payload.get("authority_generation_id"),
        "authority_writer": payload.get("authority_writer"),
        "authority_source_timestamp": payload.get("authority_source_timestamp"),
        "broker_session_owner": _mapping(payload.get("broker_session_owner")),
        "position_snapshot_timestamp": payload.get("position_snapshot_timestamp"),
        "open_order_snapshot_timestamp": payload.get("open_order_snapshot_timestamp"),
        "callback_timestamps": _mapping(payload.get("callback_timestamps")),
        "generated_at": generated_at,
        "broker_truth_generated_at": payload.get("broker_truth_generated_at"),
        "reconciliation_generated_at": payload.get("reconciliation_generated_at"),
        "age_seconds": _age_seconds(generated_at, now),
        "valid_until": valid_until,
        "entry_valid_until": entry_valid_until,
        "exit_valid_until": exit_valid_until,
        "entry_seconds_remaining": entry_seconds_remaining,
        "exit_seconds_remaining": exit_seconds_remaining,
        "submit_entry_allowed": payload.get("submit_entry_allowed") is True,
        "submit_exit_allowed": payload.get("submit_exit_allowed") is True,
        "warnings": list(payload.get("warnings") or []),
        "blockers": list(payload.get("blockers") or []),
        "contradiction_details": list(payload.get("contradiction_details") or []),
        "operator_action_required": payload.get("operator_action_required") is True,
        "account_id": payload.get("account_id"),
        "positions": list(payload.get("positions") or []),
        "open_orders": list(payload.get("open_orders") or []),
        "track_b_broker_position_count": payload.get("track_b_broker_position_count"),
        "track_b_broker_open_order_count": payload.get("track_b_broker_open_order_count"),
        "unknown_broker_open_order_count": payload.get("unknown_broker_open_order_count"),
        "review_required_count": payload.get("review_required_count"),
        "broker_reconciled": payload.get("broker_reconciled") is True,
        "latest_attempt_classification": payload.get("latest_attempt_classification"),
        "latest_attempt_generated_at": payload.get("latest_attempt_generated_at"),
        "latest_attempt_error": payload.get("latest_attempt_error"),
        "source_artifacts": dict(_mapping(payload.get("source_artifacts"))),
        "source_artifact_paths": dict(_mapping(payload.get("source_artifact_paths"))),
        "refreshed_by_canonical_readiness": payload.get("refreshed_by_canonical_readiness") is True,
        "previous_lease_state": payload.get("previous_lease_state"),
        "previous_lease_generated_at": payload.get("previous_lease_generated_at"),
        "live_money_eligible": payload.get("live_money_eligible") is True,
    }


def _broker_session_authority_input(payload: Mapping[str, Any]) -> dict[str, Any]:
    if not payload:
        return {
            "available": False,
            "classification": "BROKER_SESSION_AUTHORITY_MISSING",
            "connection_mode": "UNKNOWN",
            "allowed_uses": {},
            "authority_blockers": [
                {
                    "code": "broker_session_authority_missing",
                    "detail": "Broker Session Authority artifact is missing; alignment is unknown.",
                }
            ],
            "callback_ownership_attribution": None,
        }
    return {
        "available": True,
        "schema_version": payload.get("schema_version"),
        "generated_at": payload.get("generated_at"),
        "classification": payload.get("classification") or "BROKER_SESSION_AUTHORITY_UNKNOWN",
        "connection_mode": payload.get("connection_mode") or "UNKNOWN",
        "allowed_uses": _mapping(payload.get("allowed_uses")),
        "authority_blockers": [
            dict(row) for row in list(payload.get("authority_blockers") or []) if isinstance(row, Mapping)
        ],
        "callback_ownership_attribution": payload.get("callback_ownership_attribution"),
        "broker_session_owner": _mapping(payload.get("broker_session_owner")),
        "connection_health": _mapping(payload.get("connection_health")),
        "callback_health": _mapping(payload.get("callback_health")),
    }


def _broker_session_submit_alignment(*, readiness_submit_allowed: bool, broker_session_authority: Mapping[str, Any]) -> str:
    if not broker_session_authority or broker_session_authority.get("available") is False:
        return "UNKNOWN"
    allowed_uses = _mapping(broker_session_authority.get("allowed_uses"))
    if "new_entry" not in allowed_uses:
        return "UNKNOWN"
    broker_session_new_entry_allowed = allowed_uses.get("new_entry") is True
    if readiness_submit_allowed and not broker_session_new_entry_allowed:
        return "READINESS_SUBMIT_ALLOWED_BROKER_SESSION_BLOCKED"
    if not readiness_submit_allowed and broker_session_new_entry_allowed:
        return "READINESS_BLOCKED_BROKER_SESSION_ALLOWED"
    return "ALIGNED"


def _broker_session_new_entry_allowed(broker_session_authority: Mapping[str, Any]) -> bool:
    if not broker_session_authority or broker_session_authority.get("available") is False:
        return False
    allowed_uses = _mapping(broker_session_authority.get("allowed_uses"))
    return allowed_uses.get("new_entry") is True


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
    registry = _mapping(payload.get("registry_reconciliation"))
    lifecycle_positions = payload.get("track_b_lifecycle_positions")
    if isinstance(lifecycle_positions, list):
        lifecycle_open_position_count = len(lifecycle_positions)
    elif "lifecycle_position_count" in registry:
        lifecycle_open_position_count = int(registry.get("lifecycle_position_count") or 0)
    else:
        lifecycle_open_position_count = int(payload.get("lifecycle_open_position_count") or 0)
    review_required_count = (
        int(payload.get("current_scope_review_required_count") or 0)
        if "current_scope_review_required_count" in payload
        else int(payload.get("review_required_count") or 0)
    )
    return {
        "available": True,
        "fresh": bool(age_seconds is not None and age_seconds <= threshold),
        "classification": payload.get("classification"),
        "generated_at": payload.get("generated_at"),
        "age_seconds": age_seconds,
        "freshness_threshold_seconds": threshold,
        "broker_reconciled": payload.get("broker_reconciled") is True,
        "review_required_count": review_required_count,
        "lifecycle_open_position_count": lifecycle_open_position_count,
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
        fallback.update(_scheduled_market_data_fields(now))
        return fallback

    listener = _phase1_listener_market_data_input(
        listener_payload,
        repo_root=repo_root,
        now=now,
        active_required_symbols=_active_runtime_market_data_symbols(operator_status),
    )
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
    last_bar = _latest_runtime_processed_bar_ts(operator_status) or operator_status.get("updated_at")
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
        **_scheduled_market_data_fields(now),
    }


def _phase1_listener_market_data_input(
    payload: Mapping[str, Any],
    *,
    repo_root: Path,
    now: datetime,
    active_required_symbols: set[str] | None = None,
) -> dict[str, Any]:
    scheduled_fields = _scheduled_market_data_fields(now)
    source = str(payload.get("source") or payload.get("source_id") or "").strip()
    generated_at = payload.get("generated_at")
    status_age_seconds = _age_seconds(generated_at, now)
    live_rows = _phase1_live_symbol_rows(payload, repo_root=repo_root)
    configured_required_symbols = _symbols_from_payload_or_rows(payload.get("required_for_readiness_symbols"), live_rows, required=True)
    configured_optional_symbols = _symbols_from_payload_or_rows(payload.get("optional_symbols"), live_rows, required=False)
    active_symbols = {str(symbol).strip().upper() for symbol in (active_required_symbols or set()) if str(symbol).strip()}
    if active_symbols:
        available_symbols = {
            str(row.get("symbol") or "").strip().upper()
            for row in live_rows
            if str(row.get("symbol") or "").strip()
        }
        required_symbols = sorted(symbol for symbol in active_symbols if symbol in available_symbols)
        optional_symbols = sorted(
            {
                *configured_required_symbols,
                *configured_optional_symbols,
                *(symbol for symbol in active_symbols if symbol not in available_symbols),
            }
            - set(required_symbols)
        )
    else:
        required_symbols = configured_required_symbols
        optional_symbols = configured_optional_symbols
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
        issue = {
            "code": "phase1_listener_status_stale",
            "detail": "Phase-1 Databento listener status artifact is stale.",
            "source": "phase1_databento_live_listener",
        }
        if scheduled_fields["stale_market_data_expected"]:
            warnings.append(issue)
        else:
            blockers.append(issue)
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
        "active_required_symbols": sorted(active_symbols),
        "configured_required_symbols": configured_required_symbols,
        "required_blocked_symbols": [row["symbol"] for row in rows if row["required"] and row["ready"] is not True],
        "optional_degraded_symbols": [row["symbol"] for row in rows if not row["required"] and row["ready"] is not True],
        "rows": rows,
        "warnings": warnings,
        "blockers": blockers,
        "historical_seed_ready": payload.get("historical_seed_ready") is True,
        "research_artifact_used": payload.get("research_artifact_used") is True,
        "archive_artifact_used": payload.get("archive_artifact_used") is True,
        "databento_live_api_replay": payload.get("databento_live_api_replay") is True,
        **scheduled_fields,
    }


def _active_runtime_market_data_symbols(operator_status: Mapping[str, Any]) -> set[str]:
    active_lane_ids = {
        str(value).strip()
        for value in list(operator_status.get("active_lane_ids") or [])
        if str(value).strip()
    }
    symbols: set[str] = set()
    for lane in list(operator_status.get("lanes") or []):
        if not isinstance(lane, Mapping):
            continue
        lane_id = str(lane.get("lane_id") or "").strip()
        if active_lane_ids and lane_id and lane_id not in active_lane_ids:
            continue
        symbol = str(lane.get("symbol") or lane.get("instrument") or "").strip().upper()
        if symbol:
            symbols.add(symbol)
    if symbols:
        return symbols
    for symbol in list(operator_status.get("required_instruments") or operator_status.get("active_symbols") or []):
        symbol_text = str(symbol).strip().upper()
        if symbol_text:
            symbols.add(symbol_text)
    return symbols


def _scheduled_market_data_fields(now: datetime) -> dict[str, Any]:
    actual_now = _ensure_utc(now)
    session = classify_phase1_futures_market_session(actual_now)
    next_reopen = _next_expected_futures_reopen(actual_now)
    last_reopen = _last_expected_futures_reopen(actual_now)
    grace_until = last_reopen + timedelta(seconds=MARKET_DATA_POST_REOPEN_GRACE_SECONDS)
    in_scheduled_halt = bool(session.get("market_closed"))
    in_post_reopen_grace = bool(not in_scheduled_halt and actual_now <= grace_until)
    if in_scheduled_halt:
        market_schedule_state = MARKET_SCHEDULED_HALT
        stale_expected = True
        grace_value = next_reopen + timedelta(seconds=MARKET_DATA_POST_REOPEN_GRACE_SECONDS)
    elif in_post_reopen_grace:
        market_schedule_state = MARKET_POST_REOPEN_GRACE
        stale_expected = True
        grace_value = grace_until
    else:
        market_schedule_state = MARKET_SCHEDULE_OPEN
        stale_expected = False
        grace_value = None
    return {
        "market_schedule_state": market_schedule_state,
        "stale_market_data_expected": stale_expected,
        "next_expected_reopen_time": next_reopen.isoformat(),
        "market_data_grace_until": grace_value.isoformat() if grace_value is not None else None,
        "readiness_block_is_scheduled_halt": stale_expected,
        "market_schedule": {
            "classification": session.get("classification"),
            "reason": session.get("reason"),
            "market_closed": in_scheduled_halt,
            "market_schedule_state": market_schedule_state,
            "stale_market_data_expected": stale_expected,
            "next_expected_reopen_time": next_reopen.isoformat(),
            "market_data_grace_until": grace_value.isoformat() if grace_value is not None else None,
            "readiness_block_is_scheduled_halt": stale_expected,
            "post_reopen_grace_seconds": MARKET_DATA_POST_REOPEN_GRACE_SECONDS,
        },
    }


def _next_expected_futures_reopen(now: datetime) -> datetime:
    local = _ensure_utc(now).astimezone(NEW_YORK)
    local_time = local.timetz().replace(tzinfo=None)
    weekday = local.weekday()
    if weekday == 5:
        days_until_sunday = 1
        reopen_date = local.date() + timedelta(days=days_until_sunday)
    elif weekday == 6 and local_time < time(18, 0):
        reopen_date = local.date()
    elif weekday == 4 and local_time >= time(17, 0):
        reopen_date = local.date() + timedelta(days=2)
    elif time(17, 0) <= local_time < time(18, 0):
        reopen_date = local.date()
    elif local_time < time(17, 0):
        reopen_date = local.date()
    else:
        reopen_date = local.date() + timedelta(days=1)
    reopen_local = datetime.combine(reopen_date, time(18, 0), tzinfo=NEW_YORK)
    return reopen_local.astimezone(timezone.utc)


def _last_expected_futures_reopen(now: datetime) -> datetime:
    local = _ensure_utc(now).astimezone(NEW_YORK)
    local_time = local.timetz().replace(tzinfo=None)
    weekday = local.weekday()
    if time(18, 0) <= local_time:
        reopen_date = local.date()
    else:
        reopen_date = local.date() - timedelta(days=1)
    if weekday == 6 and local_time < time(18, 0):
        reopen_date = local.date() - timedelta(days=2)
    elif weekday == 5:
        reopen_date = local.date() - timedelta(days=1)
    reopen_local = datetime.combine(reopen_date, time(18, 0), tzinfo=NEW_YORK)
    return reopen_local.astimezone(timezone.utc)


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
    primary_1m_live_ready = bool(
        not confirmed
        and str(row.get("schema") or "").strip().lower() == "ohlcv-1m"
        and latest_completed
        and fresh
        and bar_count >= min_bars
    )
    ready = bool((confirmed or primary_1m_live_ready) and fresh and bar_count >= min_bars)
    if ready:
        block_reason = "READY" if confirmed else "READY_PRIMARY_1M_FEED_DERIVED_TIMEFRAMES_PENDING"
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
        primary_1m_live_ready=primary_1m_live_ready,
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
    primary_1m_live_ready: bool = False,
) -> dict[str, Any]:
    return {
        "symbol": symbol,
        "required": required,
        "ready": ready,
        "realtime_feed_confirmed": row.get("realtime_feed_confirmed") is True,
        "primary_1m_live_ready": bool(primary_1m_live_ready),
        "derived_timeframes_pending": bool(primary_1m_live_ready),
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
    market_schedule_override: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    if state not in CANONICAL_READINESS_STATES:
        state = "NOT_READY_CONFIG"
    root_guard = _mapping(inputs.get("root_guard_summary"))
    broker_truth_lease = _mapping(inputs.get("broker_truth_lease"))
    broker_session_authority = _mapping(inputs.get("broker_session_authority"))
    execution_core_shared_truth = _mapping(inputs.get("execution_core_shared_truth"))
    shared_truth_classifications = _mapping(execution_core_shared_truth.get("classifications"))
    submit_allowed = state == "READY_SUBMIT_CAPABLE"
    broker_lease_degraded_diagnostic = (
        str(broker_truth_lease.get("lease_state") or "").upper() == "ACTIVE_DEGRADED_REFRESH_FAILING"
        or str(shared_truth_classifications.get("Broker Truth Lease") or "").upper()
        == "ACTIVE_DEGRADED_REFRESH_FAILING"
    )
    market_schedule = _market_schedule_fields(_mapping(inputs.get("market_data")))
    if market_schedule_override:
        market_schedule.update(dict(market_schedule_override))
    return {
        "schema_version": "track_b_canonical_readiness_v1",
        "generated_at": generated_at,
        "paper_only": True,
        "canonical_readiness": state,
        "state": state,
        "ready_submit_capable": submit_allowed,
        "submit_allowed": submit_allowed,
        "runtime_start_allowed": state in {
            "READY_SUBMIT_CAPABLE",
            "READY_OBSERVATION_ONLY",
            "READY_TO_START_DIAGNOSTIC_ONLY",
        },
        "readiness_reasons": list(reasons),
        "readiness_blockers": [dict(row) for row in blockers],
        "readiness_warnings": [dict(row) for row in warnings],
        "operator_action_required": bool(blockers or root_guard.get("operator_action_required")),
        "root_guard_summary": dict(root_guard),
        "backend": _mapping(inputs.get("backend")),
        "runtime": _mapping(inputs.get("runtime")),
        "runtime_truth_heartbeat": _mapping(inputs.get("runtime_truth_heartbeat")),
        "broker_truth": _mapping(inputs.get("broker_truth")),
        "broker_truth_lease": broker_truth_lease,
        "broker_lease_degraded_diagnostic": broker_lease_degraded_diagnostic,
        "broker_session_authority": broker_session_authority,
        "broker_session_authority_classification": broker_session_authority.get("classification"),
        "broker_session_connection_mode": broker_session_authority.get("connection_mode"),
        "broker_session_allowed_uses": _mapping(broker_session_authority.get("allowed_uses")),
        "broker_session_authority_blockers": list(broker_session_authority.get("authority_blockers") or []),
        "callback_ownership_attribution": broker_session_authority.get("callback_ownership_attribution"),
        "broker_session_submit_alignment": _broker_session_submit_alignment(
            readiness_submit_allowed=submit_allowed,
            broker_session_authority=broker_session_authority,
        ),
        "phase1_reconciliation": _mapping(inputs.get("phase1_reconciliation")),
        "execution_core_shared_truth": execution_core_shared_truth,
        "control_plane_authorization": _mapping(inputs.get("control_plane_authorization")),
        "strategy_exit_coverage": _mapping(inputs.get("strategy_exit_coverage")),
        "market_data": _mapping(inputs.get("market_data")),
        **market_schedule,
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
    if is_archived_project_root(path):
        return False
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


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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
