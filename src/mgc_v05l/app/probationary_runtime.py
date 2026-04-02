"""Probationary shadow runtime, inspection, and paper-soak readiness helpers."""

from __future__ import annotations

import hashlib
import json
import os
import shutil
import signal
import socket
import sys
import time as time_module
from collections import Counter
from dataclasses import asdict, dataclass, field, replace
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.parse import urlencode, urlparse
from zoneinfo import ZoneInfo

from sqlalchemy import and_, select

from ..config_models import (
    AddDirectionPolicy,
    ParticipationPolicy,
    RuntimeMode,
    StrategySettings,
    load_settings_from_files,
)
from ..domain.enums import HealthStatus
from ..domain.enums import (
    LongEntryFamily,
    OrderIntentType,
    OrderStatus,
    PositionSide,
    ReplayFillPolicy,
    ShortEntryFamily,
    StrategyStatus,
)
from ..domain.exceptions import DeterminismError
from ..domain.models import Bar, HealthSnapshot
from ..execution.execution_engine import ExecutionEngine, PendingExecution
from ..execution.live_strategy_broker import LiveStrategyPilotBroker
from ..execution.order_models import FillEvent, OrderIntent
from ..execution.paper_broker import PaperBroker, PaperPosition
from ..execution.reconciliation import (
    RECONCILIATION_CLASS_BROKER_UNAVAILABLE,
    RECONCILIATION_CLASS_SAFE_REPAIR,
    RECONCILIATION_SAFE_CLASSES,
)
from ..market_data import (
    HistoricalPollingLiveClient,
    LivePollingService,
    SchwabHistoricalHttpClient,
    SchwabHistoricalRequest,
    SchwabLivePollRequest,
    SchwabMarketDataAdapter,
    load_schwab_market_data_config,
)
from ..monitoring.alerts import AlertDispatcher
from ..monitoring.health import derive_health_status
from ..monitoring.logger import StructuredLogger
from ..persistence import build_engine
from ..persistence.repositories import RepositorySet, decode_order_intent
from ..persistence.tables import bars_table, fills_table, order_intents_table, signals_table
from ..production_link import SchwabProductionLinkService
from ..research.trend_participation.canary import _CANARY_LANES
from ..research.trend_participation.canary import atpe_runtime_lane_id, atpe_runtime_lane_name
from ..research.trend_participation.engine import DEFAULT_POINT_VALUES as ATPE_POINT_VALUES
from ..research.trend_participation.features import build_feature_states
from ..research.trend_participation.models import HigherPrioritySignal, PatternVariant, ResearchBar
from ..research.trend_participation.patterns import default_pattern_variants, generate_signal_decisions
from ..research.trend_participation.phase2_continuation import (
    ATP_V1_LONG_CONTINUATION_VARIANT_ID,
    ENTRY_ELIGIBLE,
    PHASE2_ALLOWED_SESSIONS,
    PHASE2_WARMUP_BARS,
    atp_phase2_variant,
    classify_entry_states,
    latest_atp_entry_state_summary,
)
from ..research.trend_participation.phase3_timing import (
    ATP_TIMING_CONFIRMED,
    classify_timing_states,
    latest_atp_timing_state_summary,
)
from ..research.trend_participation.state_layers import latest_atp_state_summary
from ..app.replay_reporting import build_session_lookup, build_summary_metrics, build_trade_ledger, write_trade_ledger_csv
from .execution_truth import (
    AUTHORITATIVE_INTRABAR_ENTRY_ONLY,
    BASELINE_NEXT_BAR_OPEN,
    CURRENT_CANDLE_VWAP,
    FULL_AUTHORITATIVE_LIFECYCLE,
    PAPER_RUNTIME_LEDGER,
    normalize_trade_lifecycle_records,
)
from ..strategy.strategy_engine import StrategyEngine
from ..market_data.schwab_auth import SchwabOAuthClient, SchwabTokenStore
from ..market_data.timeframes import timeframe_minutes
from ..market_data.schwab_http import UrllibJsonTransport
from .approved_quant_lanes.engine import ApprovedQuantStrategyEngine
from .approved_quant_lanes.specs import ApprovedQuantLaneSpec, approved_quant_lane_specs
from .gc_mgc_london_open_acceptance_continuation_runtime import (
    GC_MGC_LONDON_OPEN_ACCEPTANCE_FIRST_THREE_BARS,
    GC_MGC_LONDON_OPEN_ACCEPTANCE_SOURCE,
    GcMgcLondonOpenAcceptanceContinuationStrategyEngine,
    gc_mgc_london_open_acceptance_window_matches,
)
from .shared_strategy_identities import get_shared_strategy_identity
from .session_phase_labels import label_session_phase
from .strategy_runtime_registry import (
    StandaloneStrategyRuntimeInstance,
    StrategyRuntimeRegistry,
    build_standalone_strategy_definitions,
)


@dataclass(frozen=True)
class ProbationaryShadowSummary:
    processed_bars: int
    new_bars: int
    last_processed_bar_end_ts: str | None
    operator_status_path: str
    artifacts_dir: str


@dataclass(frozen=True)
class ProbationarySessionInspection:
    session_date: str
    artifacts_dir: str
    operator_status_path: str
    health_status: str
    market_data_ok: bool
    broker_ok: bool
    persistence_ok: bool
    reconciliation_clean: bool
    invariants_ok: bool
    strategy_status: str
    processed_bars_total: int
    processed_bars_session: int
    last_processed_bar_end_ts: str | None
    new_bars_last_cycle: int
    current_position_side: str
    open_intent_count: int
    fill_count_session: int
    branch_source_counts: dict[str, int]
    blocked_reason_counts: dict[str, int]
    alert_count: int
    fault_alert_count: int
    alert_counts_by_code: dict[str, int]


@dataclass(frozen=True)
class ProbationaryDailySummary:
    session_date: str
    artifact_dir: str
    json_path: str
    markdown_path: str
    blotter_path: str
    summary: dict[str, Any]


@dataclass(frozen=True)
class ProbationaryPaperSummary:
    processed_bars: int
    new_bars: int
    last_processed_bar_end_ts: str | None
    operator_status_path: str
    artifacts_dir: str
    reconciliation_clean: bool
    stop_reason: str | None


@dataclass(frozen=True)
class ProbationaryPaperReadiness:
    artifact_path: str
    ready_for_paper_soak: bool
    summary: dict[str, Any]


@dataclass(frozen=True)
class ProbationaryPaperSoakValidation:
    artifact_path: str
    markdown_path: str
    summary: dict[str, Any]


@dataclass(frozen=True)
class ProbationaryPaperSoakExtendedRun:
    artifact_path: str
    markdown_path: str
    summary: dict[str, Any]


@dataclass(frozen=True)
class ProbationaryPaperSoakUnattendedRun:
    artifact_path: str
    markdown_path: str
    summary: dict[str, Any]


@dataclass(frozen=True)
class ProbationaryLiveTimingValidationRun:
    artifact_path: str
    markdown_path: str
    summary: dict[str, Any]


@dataclass(frozen=True)
class ProbationaryLiveStrategyPilotSummary:
    processed_bars: int
    new_bars: int
    last_processed_bar_end_ts: str | None
    operator_status_path: str
    artifacts_dir: str
    summary_path: str
    stop_reason: str | None


class ProbationaryRuntimeTransportFailure(RuntimeError):
    """Raised when the paper runtime cannot reach Schwab market-data at startup."""

    def __init__(self, payload: dict[str, Any]) -> None:
        message = str(payload.get("exception_text") or payload.get("message") or "Probationary runtime transport failure.")
        super().__init__(message)
        self.payload = payload


@dataclass(frozen=True)
class ProbationaryOperatorControlResult:
    action: str
    control_path: str
    status: str
    requested_at: str


APPROVED_LONG_SOURCE_FIELDS = {
    "usLatePauseResumeLongTurn": "enable_us_late_pause_resume_longs",
    "asiaEarlyNormalBreakoutRetestHoldTurn": "enable_asia_early_normal_breakout_retest_hold_longs",
}

LIVE_TIMING_STAGE_IDLE = "IDLE"
LIVE_TIMING_STAGE_AWAITING_ACK = "AWAITING_ACK"
LIVE_TIMING_STAGE_AWAITING_FILL = "AWAITING_FILL"
LIVE_TIMING_STAGE_RECONCILING = "RECONCILING"
LIVE_TIMING_STAGE_FAULTED = "FAULTED"
LIVE_TIMING_STAGE_FILLED = "FILLED_CONFIRMED"
LIVE_TIMING_STAGE_TERMINAL_NON_FILL = "TERMINAL_NON_FILL_CONFIRMED"
LIVE_STRATEGY_PILOT_OPERATOR_PATH = "mgc-v05l probationary-live-strategy-pilot"
LIVE_STRATEGY_PILOT_REARM_ACTION = "rearm_live_strategy_pilot"
LIVE_STRATEGY_PILOT_CYCLE_TERMINAL_RESULTS = {"completed", "reconciled", "faulted", "aborted"}
LIVE_STRATEGY_TERMINAL_NON_FILL_STATUSES = {"REJECTED", "CANCELLED", "CANCELED", "EXPIRED"}
LIVE_STRATEGY_ACKNOWLEDGED_STATUSES = {
    OrderStatus.ACKNOWLEDGED.value,
    OrderStatus.FILLED.value,
    "WORKING",
    "OPEN",
    "NEW",
    "QUEUED",
    "ACCEPTED",
    "PENDING_ACTIVATION",
    "PARTIALLY_FILLED",
}

LIVE_TIMING_BROKER_TRUTH_DECISION_ORDER = (
    "direct_order_status",
    "open_orders",
    "position_truth",
    "fill_truth",
)

LIVE_SIGNAL_OBSERVABILITY_COUNT_FIELDS = (
    ("bull_snap_turn_candidate", "bull_snap_turn_candidate"),
    ("firstBullSnapTurn", "first_bull_snap_turn"),
    ("asia_reclaim_bar_raw", "asia_reclaim_bar_raw"),
    ("asia_hold_bar_ok", "asia_hold_bar_ok"),
    ("asia_acceptance_bar_ok", "asia_acceptance_bar_ok"),
    ("asiaVWAPLongSignal", "asia_vwap_long_signal"),
    ("bear_snap_turn_candidate", "bear_snap_turn_candidate"),
    ("firstBearSnapTurn", "first_bear_snap_turn"),
    ("longEntryRaw", "long_entry_raw"),
    ("shortEntryRaw", "short_entry_raw"),
    ("longEntry", "long_entry"),
    ("shortEntry", "short_entry"),
)

LIVE_SIGNAL_OBSERVABILITY_FAMILY_FIELDS = {
    "bullSnapLong": (
        ("bull_snap_turn_candidate", "bull_snap_turn_candidate"),
        ("firstBullSnapTurn", "first_bull_snap_turn"),
    ),
    "asiaVWAPLong": (
        ("asia_reclaim_bar_raw", "asia_reclaim_bar_raw"),
        ("asia_hold_bar_ok", "asia_hold_bar_ok"),
        ("asia_acceptance_bar_ok", "asia_acceptance_bar_ok"),
        ("asiaVWAPLongSignal", "asia_vwap_long_signal"),
    ),
    "bearSnapShort": (
        ("bear_snap_turn_candidate", "bear_snap_turn_candidate"),
        ("firstBearSnapTurn", "first_bear_snap_turn"),
    ),
}

APPROVED_SHORT_SOURCE_FIELDS = {
    "asiaEarlyPauseResumeShortTurn": "enable_asia_early_pause_resume_shorts",
}

PAPER_EXECUTION_CANARY_MODE = "PAPER_EXECUTION_CANARY"
PAPER_EXECUTION_CANARY_SIGNAL_SOURCE = "paperExecutionCanary"
PAPER_EXECUTION_CANARY_ENTRY_REASON = "paperExecutionCanaryEntryLateWindow"
PAPER_EXECUTION_CANARY_EXIT_REASON = "paperExecutionCanaryExitNextBarLateWindow"
PAPER_EXECUTION_CANARY_FORCE_SIGNAL_SOURCE = "paperExecutionCanaryForceFireOnce"
PAPER_EXECUTION_CANARY_FORCE_ENTRY_REASON = "paperExecutionCanaryForceFireOnceEntry"
PAPER_EXECUTION_CANARY_FORCE_EXIT_REASON = "paperExecutionCanaryForceFireOnceExitNextBar"
ATPE_CANARY_RUNTIME_KIND = "atpe_canary_observer"
ATP_COMPANION_BENCHMARK_RUNTIME_KIND = "atp_companion_benchmark_paper"
ATPE_CANARY_LANE_MODE = "ATPE_CANARY_OBSERVER"
ATP_COMPANION_BENCHMARK_LANE_MODE = "ATP_COMPANION_BENCHMARK"
ATPE_CANARY_SOURCE_FAMILY = "active_trend_participation_engine"
ATPE_EXIT_POLICY_HARD_TARGET = "hard_target"
ATPE_EXIT_POLICY_TARGET_CHECKPOINT = "target_checkpoint_trail_v1"
ATPE_TARGET_CHECKPOINT_LOCK_R = 0.25
ATPE_TARGET_CHECKPOINT_TRAIL_R = 0.15
GC_MGC_ACCEPTANCE_RUNTIME_KIND = "gc_mgc_london_open_acceptance_temp_paper"
GC_MGC_ACCEPTANCE_LANE_MODE = "TEMP_PAPER_EXPERIMENTAL"
GC_MGC_ACCEPTANCE_SOURCE_FAMILY = GC_MGC_LONDON_OPEN_ACCEPTANCE_SOURCE
APPROVED_QUANT_RUNTIME_LOOKBACK_MINUTES = 43200
APPROVED_QUANT_POINT_VALUES: dict[str, Decimal] = {
    "MGC": Decimal("10"),
    "GC": Decimal("100"),
    "PL": Decimal("50"),
    "HG": Decimal("25000"),
    "QC": Decimal("25000"),
    "CL": Decimal("1000"),
    "ES": Decimal("50"),
    "6E": Decimal("125000"),
    "6J": Decimal("12500000"),
}


def _atp_runtime_identity_payload(spec: "ProbationaryPaperLaneSpec") -> dict[str, Any]:
    experimental_status = str(spec.experimental_status or "").strip().lower()
    is_benchmark = experimental_status == "tracked_paper_benchmark"
    participation_policy = spec.participation_policy.value
    is_staged_candidate = (not is_benchmark) and spec.participation_policy is not ParticipationPolicy.SINGLE_ENTRY_ONLY

    if is_benchmark:
        strategy_status = "RUNNING_ATP_COMPANION_BENCHMARK_PAPER"
        scope_label = "ATP Companion Benchmark / Paper Only / London Diagnostic-Only"
        runtime_mode = "atp_companion_benchmark_paper_runtime"
        tracked_strategy_id = "atp_companion_v1_asia_us"
        benchmark_designation = "CURRENT_ATP_COMPANION_BENCHMARK"
        notes = [
            "ATP Companion Baseline v1 benchmark runtime",
            "Paper Only",
            "Asia + US executable",
            "London diagnostic-only",
            f"Participation Policy: {participation_policy}",
        ]
    else:
        strategy_status = (
            "RUNNING_ATP_COMPANION_CANDIDATE_STAGED_PAPER"
            if is_staged_candidate
            else "RUNNING_ATP_COMPANION_CANDIDATE_PAPER"
        )
        scope_label = (
            "ATP Companion Candidate / Paper Only / London Diagnostic-Only / Staged"
            if is_staged_candidate
            else "ATP Companion Candidate / Paper Only / London Diagnostic-Only"
        )
        runtime_mode = (
            "atp_companion_candidate_staged_paper_runtime"
            if is_staged_candidate
            else "atp_companion_candidate_paper_runtime"
        )
        tracked_strategy_id = str(spec.standalone_strategy_id or spec.lane_id)
        benchmark_designation = None
        notes = [
            "ATP Companion candidate lane runtime",
            "Paper Only",
            "Asia + US executable",
            "London diagnostic-only",
            f"Participation Policy: {participation_policy}",
        ]

    return {
        "strategy_status": strategy_status,
        "scope_label": scope_label,
        "live_runtime_mode": runtime_mode,
        "tracked_strategy_id": tracked_strategy_id,
        "benchmark_designation": benchmark_designation,
        "notes": notes,
        "participation_policy": participation_policy,
    }


@dataclass(frozen=True)
class ProbationaryPaperLaneSpec:
    lane_id: str
    display_name: str
    symbol: str
    long_sources: tuple[str, ...]
    short_sources: tuple[str, ...]
    session_restriction: str | None
    point_value: Decimal
    standalone_strategy_id: str | None = None
    trade_size: int = 1
    participation_policy: ParticipationPolicy = ParticipationPolicy.SINGLE_ENTRY_ONLY
    max_concurrent_entries: int = 1
    max_position_quantity: int | None = None
    max_adds_after_entry: int = 0
    add_direction_policy: AddDirectionPolicy = AddDirectionPolicy.SAME_DIRECTION_ONLY
    catastrophic_open_loss: Decimal | None = None
    lane_mode: str = "STANDARD"
    strategy_family: str = "UNKNOWN"
    strategy_identity_root: str | None = None
    runtime_kind: str = "strategy_engine"
    allowed_sessions: tuple[str, ...] = ()
    live_poll_lookback_minutes: int | None = None
    database_url: str | None = None
    artifacts_dir: str | None = None
    canary_entry_not_before_et: str | None = None
    canary_entry_window_end_et: str | None = None
    canary_exit_not_before_et: str | None = None
    canary_max_entries_per_session: int = 1
    canary_one_shot_per_session: bool = False
    observed_instruments: tuple[str, ...] = ()
    quality_bucket_policy: str | None = None
    experimental_status: str | None = None
    paper_only: bool = False
    non_approved: bool = False
    observer_variant_id: str | None = None
    observer_side: str | None = None
    identity_components: tuple[str, ...] = ()
    shared_strategy_identity: str | None = None


@dataclass(frozen=True)
class ProbationaryPaperLaneMetrics:
    session_date: str
    realized_pnl: Decimal
    unrealized_pnl: Decimal
    total_pnl: Decimal
    closed_trades: int
    losing_closed_trades: int
    intent_count: int
    fill_count: int
    open_order_count: int
    position_side: str
    internal_position_qty: int
    broker_position_qty: int
    open_entry_leg_count: int
    open_add_count: int
    additional_entry_allowed: bool
    entry_price: Decimal | None
    last_mark: Decimal | None
    last_processed_bar_end_ts: str | None


@dataclass
class ProbationaryPaperRiskRuntimeState:
    session_date: str
    desk_halt_new_entries_triggered: bool = False
    desk_flatten_and_halt_triggered: bool = False
    desk_last_trigger_reason: str | None = None
    desk_last_triggered_at: str | None = None
    desk_last_cleared_at: str | None = None
    desk_last_cleared_action: str | None = None
    lane_states: dict[str, dict[str, Any]] = field(default_factory=dict)


REALIZED_LOSER_SESSION_OVERRIDE_ACTION = "force_lane_resume_session_override"
REALIZED_LOSER_SESSION_OVERRIDE_REASON = "lane_realized_loser_limit_per_session"
SESSION_RESET_AUTO_CLEAR_ACTION = "Next session reset auto-clear"
RECONCILIATION_HEARTBEAT_TRIGGER = "heartbeat"
MISSING_ACK_TIMEOUT_TRIGGER = "missing_ack_timeout"
FILL_TIMEOUT_TRIGGER = "fill_timeout"
PENDING_ORDER_UNCERTAINTY_TRIGGER = "pending_order_uncertainty"
_LANE_TARGETABLE_PROBATIONARY_CONTROL_ACTIONS = frozenset(
    {
        "halt_entries",
        "resume_entries",
        "clear_fault",
        "clear_risk_halts",
        "flatten_and_halt",
        "force_reconcile",
        REALIZED_LOSER_SESSION_OVERRIDE_ACTION,
    }
)


class ProbationaryLaneStructuredLogger:
    """Mirror lane events into both lane-local and root paper artifacts."""

    def __init__(
        self,
        *,
        lane_id: str,
        symbol: str,
        root_logger: StructuredLogger,
        lane_logger: StructuredLogger,
    ) -> None:
        self._lane_id = lane_id
        self._symbol = symbol
        self._root_logger = root_logger
        self._lane_logger = lane_logger

    @property
    def artifact_dir(self) -> Path:
        return self._lane_logger.artifact_dir

    def log_branch_source(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.log_branch_source(enriched)
        return self._lane_logger.log_branch_source(enriched)

    def log_rule_block(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.log_rule_block(enriched)
        return self._lane_logger.log_rule_block(enriched)

    def log_alert(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.log_alert(enriched)
        return self._lane_logger.log_alert(enriched)

    def write_alert_state(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.write_alert_state(enriched)
        return self._lane_logger.write_alert_state(enriched)

    def log_reconciliation_event(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.log_reconciliation_event(enriched)
        return self._lane_logger.log_reconciliation_event(enriched)

    def log_execution_watchdog_event(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.log_execution_watchdog_event(enriched)
        return self._lane_logger.log_execution_watchdog_event(enriched)

    def log_restore_validation_event(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.log_restore_validation_event(enriched)
        return self._lane_logger.log_restore_validation_event(enriched)

    def log_exit_parity_event(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.log_exit_parity_event(enriched)
        return self._lane_logger.log_exit_parity_event(enriched)

    def log_live_timing_event(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.log_live_timing_event(enriched)
        return self._lane_logger.log_live_timing_event(enriched)

    def log_operator_control(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.log_operator_control(enriched)
        return self._lane_logger.log_operator_control(enriched)

    def write_operator_status(self, payload: dict[str, Any]) -> Path:
        return self._lane_logger.write_operator_status(self._enrich(payload))

    def write_restore_validation_state(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.write_restore_validation_state(enriched)
        return self._lane_logger.write_restore_validation_state(enriched)

    def write_exit_parity_state(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.write_exit_parity_state(enriched)
        return self._lane_logger.write_exit_parity_state(enriched)

    def write_live_timing_state(self, payload: dict[str, Any]) -> Path:
        enriched = self._enrich(payload)
        self._root_logger.write_live_timing_state(enriched)
        return self._lane_logger.write_live_timing_state(enriched)

    def _enrich(self, payload: dict[str, Any]) -> dict[str, Any]:
        enriched = dict(payload)
        enriched.setdefault("lane_id", self._lane_id)
        enriched.setdefault("symbol", self._symbol)
        return enriched


def _sync_runtime_health_alerts(
    *,
    alert_dispatcher: AlertDispatcher,
    snapshot: HealthSnapshot,
    runtime_name: str,
    occurred_at: datetime,
    operator_status_path: str | None = None,
) -> None:
    payload = {
        "runtime_name": runtime_name,
        "health_status": snapshot.health_status.value,
        "market_data_ok": snapshot.market_data_ok,
        "broker_ok": snapshot.broker_ok,
        "persistence_ok": snapshot.persistence_ok,
        "reconciliation_clean": snapshot.reconciliation_clean,
        "invariants_ok": snapshot.invariants_ok,
        "operator_status_path": operator_status_path,
        "occurred_at": occurred_at.isoformat(),
    }
    alert_dispatcher.sync_condition(
        code=f"{runtime_name}_health_status",
        active=snapshot.health_status is not HealthStatus.HEALTHY,
        severity="ACTION" if snapshot.health_status is HealthStatus.DEGRADED else "BLOCKING",
        category="market_data",
        title=f"{runtime_name.replace('_', ' ').title()} Health",
        message=f"{runtime_name.replace('_', ' ').title()} health is {snapshot.health_status.value}.",
        payload=payload,
        dedup_key=f"{runtime_name}:health_status",
        recommended_action="Inspect market-data, broker, and persistence health if the condition does not self-resolve.",
        occurred_at=occurred_at,
    )
    alert_dispatcher.sync_condition(
        code=f"{runtime_name}_market_data_degradation",
        active=not snapshot.market_data_ok,
        severity="ACTION",
        category="market_data",
        title="Market Data Degradation",
        message=f"{runtime_name.replace('_', ' ').title()} is not receiving fresh market data.",
        payload=payload,
        dedup_key=f"{runtime_name}:market_data",
        recommended_action="Verify completed-bar polling and live market-data transport health.",
        occurred_at=occurred_at,
    )
    alert_dispatcher.sync_condition(
        code=f"{runtime_name}_broker_disconnect",
        active=not snapshot.broker_ok,
        severity="BLOCKING",
        category="broker_connectivity",
        title="Broker Disconnected",
        message=f"{runtime_name.replace('_', ' ').title()} broker connectivity is unavailable.",
        payload=payload,
        dedup_key=f"{runtime_name}:broker_connectivity",
        recommended_action="Verify broker connectivity before allowing new entries to continue.",
        occurred_at=occurred_at,
    )


def _effective_reconciliation_clean(payload: dict[str, Any] | None) -> bool:
    if not payload:
        return True
    classification = str(payload.get("classification") or "").strip()
    return bool(payload.get("clean")) or classification in RECONCILIATION_SAFE_CLASSES


def _initial_reconciliation_heartbeat_status(interval_seconds: int) -> dict[str, Any]:
    cadence = max(int(interval_seconds or 0), 1)
    return {
        "enabled": True,
        "trigger_source": RECONCILIATION_HEARTBEAT_TRIGGER,
        "cadence_seconds": cadence,
        "status": "AWAITING_FIRST_HEARTBEAT",
        "classification": None,
        "last_attempted_at": None,
        "last_completed_at": None,
        "next_due_at": None,
        "active_issue": False,
        "manual_action_required": False,
        "entries_frozen": False,
        "broker_truth_available": None,
        "recommended_action": "No action needed; waiting for the first heartbeat reconcile pass.",
        "reason": "Heartbeat reconciliation has not run yet in this runtime process.",
        "mismatches": [],
        "notes": [],
        "reconciliation_applied": False,
    }


def _parse_iso_datetime_or_none(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed


def _heartbeat_status_label(payload: dict[str, Any]) -> str:
    classification = str(payload.get("classification") or "").strip()
    if classification == "clean":
        return "CLEAN"
    if classification == RECONCILIATION_CLASS_SAFE_REPAIR:
        return "SAFE_REPAIR"
    if classification == RECONCILIATION_CLASS_BROKER_UNAVAILABLE:
        return "BROKER_UNAVAILABLE"
    if bool(payload.get("requires_fault")) or str(payload.get("resulting_strategy_status") or "").upper() == "FAULT":
        return "FAULT"
    return "RECONCILING"


def _heartbeat_reconcile_due(
    heartbeat_status: dict[str, Any] | None,
    *,
    interval_seconds: int,
    occurred_at: datetime,
) -> bool:
    prior_attempt = _parse_iso_datetime_or_none((heartbeat_status or {}).get("last_attempted_at"))
    if prior_attempt is None:
        return True
    return occurred_at >= prior_attempt + timedelta(seconds=max(int(interval_seconds or 0), 1))


def _heartbeat_reconcile_apply_required(
    *,
    strategy_engine: StrategyEngine,
    inspection: dict[str, Any],
) -> bool:
    classification = str(inspection.get("classification") or "").strip()
    fault_code = inspection.get("fault_code")
    state = strategy_engine.state
    if classification == "clean":
        return bool(
            state.reconcile_required
            or state.strategy_status in {StrategyStatus.RECONCILING, StrategyStatus.FAULT, StrategyStatus.DISABLED}
        )
    if classification == RECONCILIATION_CLASS_SAFE_REPAIR:
        return True
    if bool(inspection.get("requires_fault")):
        return not (
            state.strategy_status is StrategyStatus.FAULT
            and state.entries_enabled is False
            and state.fault_code == fault_code
        )
    return not (
        state.strategy_status is StrategyStatus.RECONCILING
        and state.entries_enabled is False
        and state.reconcile_required is True
        and (fault_code is None or state.fault_code == fault_code)
    )


def _build_reconciliation_heartbeat_status(
    *,
    payload: dict[str, Any],
    cadence_seconds: int,
    occurred_at: datetime,
    reconciliation_applied: bool,
) -> dict[str, Any]:
    broker_snapshot = payload.get("broker_snapshot") or {}
    notes = [str(value) for value in (payload.get("notes") or []) if str(value).strip()]
    mismatches = [str(value) for value in (payload.get("mismatches") or payload.get("issues") or []) if str(value).strip()]
    classification = str(payload.get("classification") or "").strip()
    status = _heartbeat_status_label(payload)
    broker_truth_available = bool(broker_snapshot.get("connected")) and bool(broker_snapshot.get("truth_complete"))
    active_issue = classification not in RECONCILIATION_SAFE_CLASSES
    manual_action_required = bool(payload.get("requires_review")) and classification != RECONCILIATION_CLASS_BROKER_UNAVAILABLE
    reason = (
        "; ".join(mismatches)
        if mismatches
        else notes[0]
        if notes
        else ("Broker truth is unavailable or incomplete." if classification == RECONCILIATION_CLASS_BROKER_UNAVAILABLE else "No mismatch is active.")
    )
    return {
        "enabled": True,
        "trigger_source": RECONCILIATION_HEARTBEAT_TRIGGER,
        "cadence_seconds": max(int(cadence_seconds or 0), 1),
        "status": status,
        "classification": classification or None,
        "last_attempted_at": occurred_at.isoformat(),
        "last_completed_at": occurred_at.isoformat(),
        "next_due_at": (occurred_at + timedelta(seconds=max(int(cadence_seconds or 0), 1))).isoformat(),
        "active_issue": active_issue,
        "manual_action_required": manual_action_required,
        "entries_frozen": bool(payload.get("resulting_entries_enabled") is False or payload.get("freeze_new_entries") is True),
        "broker_truth_available": broker_truth_available,
        "recommended_action": payload.get("recommended_action") or "No action needed.",
        "reason": reason,
        "mismatches": mismatches,
        "notes": notes,
        "reconciliation_applied": reconciliation_applied,
        "resulting_strategy_status": payload.get("resulting_strategy_status"),
        "resulting_fault_code": payload.get("resulting_fault_code") or payload.get("fault_code"),
    }


def _run_reconciliation_heartbeat(
    *,
    settings: StrategySettings,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    heartbeat_status: dict[str, Any] | None,
    occurred_at: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None, bool]:
    observed_at = occurred_at or datetime.now(timezone.utc)
    cadence_seconds = max(int(settings.reconciliation_heartbeat_interval_seconds or 0), 1)
    previous = dict(heartbeat_status or _initial_reconciliation_heartbeat_status(cadence_seconds))
    if not _heartbeat_reconcile_due(previous, interval_seconds=cadence_seconds, occurred_at=observed_at):
        next_due = _parse_iso_datetime_or_none(previous.get("last_attempted_at"))
        return (
            {
                **previous,
                "enabled": True,
                "cadence_seconds": cadence_seconds,
                "next_due_at": (
                    (next_due + timedelta(seconds=cadence_seconds)).isoformat()
                    if next_due is not None
                    else previous.get("next_due_at")
                ),
            },
            None,
            False,
        )
    inspection = strategy_engine.inspect_reconciliation(
        occurred_at=observed_at,
        trigger=RECONCILIATION_HEARTBEAT_TRIGGER,
        execution_engine=execution_engine,
    )
    reconciliation_applied = _heartbeat_reconcile_apply_required(
        strategy_engine=strategy_engine,
        inspection=inspection,
    )
    payload = (
        strategy_engine.apply_reconciliation(
            occurred_at=observed_at,
            trigger=RECONCILIATION_HEARTBEAT_TRIGGER,
            execution_engine=execution_engine,
        )
        if reconciliation_applied
        else inspection
    )
    payload.setdefault("occurred_at", observed_at.isoformat())
    payload.setdefault("logged_at", observed_at.isoformat())
    if not reconciliation_applied:
        payload.setdefault("resulting_strategy_status", strategy_engine.state.strategy_status.value)
        payload.setdefault("resulting_fault_code", strategy_engine.state.fault_code)
        payload.setdefault("resulting_entries_enabled", strategy_engine.state.entries_enabled)
        payload.setdefault("reconcile_required", strategy_engine.state.reconcile_required)
    return (
        _build_reconciliation_heartbeat_status(
            payload=payload,
            cadence_seconds=cadence_seconds,
            occurred_at=observed_at,
            reconciliation_applied=reconciliation_applied,
        ),
        payload,
        True,
    )


def _initial_order_timeout_watchdog_status(settings: StrategySettings) -> dict[str, Any]:
    cadence = max(int(settings.order_lifecycle_watchdog_interval_seconds or 0), 1)
    return {
        "enabled": True,
        "trigger_source": "order_lifecycle_watchdog",
        "cadence_seconds": cadence,
        "ack_timeout_seconds": int(settings.order_ack_timeout_seconds),
        "fill_timeout_seconds": int(settings.order_fill_timeout_seconds),
        "reconcile_grace_seconds": int(settings.order_timeout_reconcile_grace_seconds),
        "retry_limit": int(settings.order_timeout_retry_limit),
        "status": "HEALTHY",
        "last_checked_at": None,
        "next_due_at": None,
        "overdue_ack_count": 0,
        "overdue_fill_count": 0,
        "active_issue_count": 0,
        "active_issue_rows": [],
        "safe_repair_count": 0,
        "broker_truth_available": None,
        "last_escalation": None,
        "recommended_action": "No action needed.",
        "reason": "No active pending-order timeout issues.",
    }


def _order_timeout_watchdog_due(
    status: dict[str, Any] | None,
    *,
    interval_seconds: int,
    occurred_at: datetime,
) -> bool:
    previous = _parse_iso_datetime_or_none((status or {}).get("last_checked_at"))
    if previous is None:
        return True
    return occurred_at >= previous + timedelta(seconds=max(int(interval_seconds or 0), 1))


def _watchdog_pending_order_status(pending: PendingExecution) -> OrderStatus:
    if pending.acknowledged_at is not None:
        return OrderStatus.ACKNOWLEDGED
    return OrderStatus.PENDING


def _persist_pending_execution_row(
    *,
    repositories: RepositorySet,
    pending: PendingExecution,
    occurred_at: datetime,
    timeout_classification: str | None,
    order_status: OrderStatus | None = None,
) -> None:
    repositories.order_intents.save(
        pending.intent,
        order_status=order_status or _watchdog_pending_order_status(pending),
        broker_order_id=pending.broker_order_id,
        submitted_at=pending.submitted_at,
        acknowledged_at=pending.acknowledged_at,
        broker_order_status=pending.broker_order_status,
        last_status_checked_at=pending.last_status_checked_at or occurred_at,
        timeout_classification=timeout_classification,
        timeout_status_updated_at=occurred_at,
        retry_count=pending.retry_count,
    )


def _latest_fill_for_pending(
    repositories: RepositorySet,
    *,
    order_intent_id: str,
    broker_order_id: str | None,
) -> dict[str, Any] | None:
    latest: dict[str, Any] | None = None
    latest_ts: datetime | None = None
    for row in repositories.fills.list_all():
        if row.get("order_intent_id") != order_intent_id and row.get("broker_order_id") != broker_order_id:
            continue
        fill_ts = _parse_iso_datetime_or_none(row.get("fill_timestamp"))
        if fill_ts is None:
            continue
        if latest_ts is None or fill_ts > latest_ts:
            latest = dict(row)
            latest_ts = fill_ts
    return latest


def _timeout_dedup_key_for_order(repositories: RepositorySet, *, order_intent_id: str, symbol: str, suffix: str) -> str:
    identity = repositories.runtime_identity
    lane_id = str(identity.get("lane_id") or "")
    standalone_strategy_id = str(identity.get("standalone_strategy_id") or "")
    instrument = str(identity.get("instrument") or symbol)
    return f"{standalone_strategy_id}:{lane_id}:{instrument}:{order_intent_id}:{suffix}"


def _timeout_dedup_key(repositories: RepositorySet, pending: PendingExecution, suffix: str) -> str:
    return _timeout_dedup_key_for_order(
        repositories,
        order_intent_id=pending.intent.order_intent_id,
        symbol=pending.intent.symbol,
        suffix=suffix,
    )


def _log_execution_watchdog_event(
    *,
    repositories: RepositorySet,
    structured_logger: StructuredLogger | ProbationaryLaneStructuredLogger,
    occurred_at: datetime,
    payload: dict[str, Any],
) -> None:
    repositories.execution_watchdog_events.save(payload, created_at=occurred_at)
    structured_logger.log_execution_watchdog_event(payload)


def _sync_timeout_condition_alert(
    *,
    alert_dispatcher: AlertDispatcher,
    repositories: RepositorySet,
    pending: PendingExecution,
    occurred_at: datetime,
    active: bool,
    category: str,
    severity: str,
    title: str,
    message: str,
    recommended_action: str,
    dedup_suffix: str,
    payload: dict[str, Any],
) -> None:
    alert_dispatcher.sync_condition(
        code=category,
        active=active,
        severity=severity,
        category=category,
        title=title,
        message=message,
        payload={**repositories.runtime_identity, **payload},
        dedup_key=_timeout_dedup_key(repositories, pending, dedup_suffix),
        recommended_action=recommended_action,
        occurred_at=occurred_at,
    )


def _run_order_timeout_watchdog(
    *,
    settings: StrategySettings,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    structured_logger: StructuredLogger | ProbationaryLaneStructuredLogger,
    alert_dispatcher: AlertDispatcher,
    watchdog_status: dict[str, Any] | None,
    occurred_at: datetime | None = None,
) -> tuple[dict[str, Any], dict[str, Any] | None, bool]:
    observed_at = occurred_at or datetime.now(timezone.utc)
    cadence_seconds = max(int(settings.order_lifecycle_watchdog_interval_seconds or 0), 1)
    previous = dict(watchdog_status or _initial_order_timeout_watchdog_status(settings))
    if not _order_timeout_watchdog_due(previous, interval_seconds=cadence_seconds, occurred_at=observed_at):
        prior_check = _parse_iso_datetime_or_none(previous.get("last_checked_at"))
        return (
            {
                **previous,
                "enabled": True,
                "next_due_at": (
                    (prior_check + timedelta(seconds=cadence_seconds)).isoformat()
                    if prior_check is not None
                    else previous.get("next_due_at")
                ),
            },
            None,
            False,
        )

    broker_snapshot = execution_engine.broker.snapshot_state()
    broker_truth_available = bool(broker_snapshot.get("connected")) and "open_order_ids" in broker_snapshot
    broker_open_order_ids = {str(value) for value in broker_snapshot.get("open_order_ids", [])}
    broker_position_quantity = int(broker_snapshot.get("position_quantity", 0) or 0)
    latest_broker_fill = _parse_iso_datetime_or_none(broker_snapshot.get("last_fill_timestamp"))
    state = strategy_engine.state
    current_pending_ids = {pending.intent.order_intent_id for pending in execution_engine.pending_executions()}

    active_issue_rows: list[dict[str, Any]] = []
    overdue_ack_count = 0
    overdue_fill_count = 0
    safe_repair_count = 0
    last_escalation: dict[str, Any] | None = None
    last_meaningful_event: dict[str, Any] | None = None

    for pending in list(execution_engine.pending_executions()):
        status_payload = execution_engine.broker.get_order_status(pending.broker_order_id) or {}
        broker_order_status = str(status_payload.get("status") or pending.broker_order_status or "").strip().upper() or None
        pending = execution_engine.observe_pending_status(
            pending.intent.order_intent_id,
            broker_order_status=broker_order_status,
            observed_at=observed_at,
            acknowledged=(pending.acknowledged_at is not None),
        ) or pending
        latest_fill_row = _latest_fill_for_pending(
            repositories,
            order_intent_id=pending.intent.order_intent_id,
            broker_order_id=pending.broker_order_id,
        )
        latest_fill_timestamp = _parse_iso_datetime_or_none(
            latest_fill_row.get("fill_timestamp") if latest_fill_row is not None else latest_broker_fill
        )
        age_since_submit = max((observed_at - pending.submitted_at).total_seconds(), 0.0)
        ack_reference = pending.acknowledged_at or pending.submitted_at
        age_since_ack = max((observed_at - ack_reference).total_seconds(), 0.0)
        broker_has_open_order = pending.broker_order_id in broker_open_order_ids

        base_payload = {
            "event_type": "order_timeout_watchdog",
            "trigger": "order_lifecycle_watchdog",
            "order_intent_id": pending.intent.order_intent_id,
            "broker_order_id": pending.broker_order_id,
            "intent_type": pending.intent.intent_type.value,
            "symbol": pending.intent.symbol,
            "created_at": pending.intent.created_at.isoformat(),
            "submitted_at": pending.submitted_at.isoformat(),
            "acknowledged_at": pending.acknowledged_at.isoformat() if pending.acknowledged_at is not None else None,
            "fill_timestamp": latest_fill_timestamp.isoformat() if latest_fill_timestamp is not None else None,
            "pending_age_seconds": int(age_since_ack if pending.acknowledged_at is not None else age_since_submit),
            "broker_order_status": broker_order_status,
            "broker_truth_available": broker_truth_available,
            "broker_has_open_order": broker_has_open_order,
            "broker_position_quantity": broker_position_quantity,
            "strategy_status": state.strategy_status.value,
            "strategy_position_side": state.position_side.value,
            "strategy_internal_position_qty": state.internal_position_qty,
            "strategy_broker_position_qty": state.broker_position_qty,
        }

        if not broker_truth_available:
            _persist_pending_execution_row(
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                timeout_classification="broker_truth_unavailable",
            )
            continue

        if latest_fill_row is not None:
            _sync_timeout_condition_alert(
                alert_dispatcher=alert_dispatcher,
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                active=False,
                category="fill_timeout",
                severity="RECOVERY",
                title="Fill Timeout Resolved",
                message="Pending order timeout resolved because fill evidence is now present.",
                recommended_action="No action needed.",
                dedup_suffix="fill_timeout",
                payload=base_payload,
            )
            _sync_timeout_condition_alert(
                alert_dispatcher=alert_dispatcher,
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                active=False,
                category="missing_fill_ack",
                severity="RECOVERY",
                title="Missing Ack Resolved",
                message="Pending order acknowledgement timeout resolved because execution evidence is now present.",
                recommended_action="No action needed.",
                dedup_suffix="ack_timeout",
                payload=base_payload,
            )
            execution_engine.clear_intent(pending.intent.order_intent_id)
            continue

        if (
            broker_order_status in {OrderStatus.REJECTED.value, OrderStatus.CANCELLED.value, "CANCELED", "EXPIRED"}
            and not broker_has_open_order
            and broker_position_quantity == 0
            and state.position_side == PositionSide.FLAT
        ):
            _persist_pending_execution_row(
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                timeout_classification="terminal_non_fill_confirmed",
                order_status=OrderStatus.CANCELLED if broker_order_status in {OrderStatus.CANCELLED.value, "CANCELED", "EXPIRED"} else OrderStatus.REJECTED,
            )
            execution_engine.clear_intent(pending.intent.order_intent_id)
            last_meaningful_event = {
                **base_payload,
                "timeout_classification": "terminal_non_fill_confirmed",
                "terminal_broker_status": broker_order_status,
                "resulting_state": strategy_engine.state.strategy_status.value,
            }
            _log_execution_watchdog_event(
                repositories=repositories,
                structured_logger=structured_logger,
                occurred_at=observed_at,
                payload=last_meaningful_event,
            )
            alert_dispatcher.emit(
                severity="RECOVERY",
                code="terminal_non_fill_confirmed",
                message="Pending order resolved as a terminal non-fill from broker truth.",
                payload={**repositories.runtime_identity, **last_meaningful_event},
                category="safe_repair_performed",
                title="Terminal Non-Fill Confirmed",
                dedup_key=_timeout_dedup_key(repositories, pending, f"terminal_non_fill:{broker_order_status}"),
                active=False,
                coalesce=False,
                occurred_at=observed_at,
            )
            continue

        if pending.acknowledged_at is None and broker_has_open_order:
            pending = execution_engine.observe_pending_status(
                pending.intent.order_intent_id,
                broker_order_status=broker_order_status or OrderStatus.ACKNOWLEDGED.value,
                observed_at=observed_at,
                acknowledged=True,
            ) or pending
            _persist_pending_execution_row(
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                timeout_classification="ack_missing_broker_open_order_exists",
                order_status=OrderStatus.ACKNOWLEDGED,
            )
            safe_repair_count += 1
            last_meaningful_event = {
                **base_payload,
                "timeout_classification": "ack_missing_broker_open_order_exists",
                "repair_action": "record_broker_acknowledgement",
                "resulting_state": "ACKNOWLEDGED",
            }
            _log_execution_watchdog_event(
                repositories=repositories,
                structured_logger=structured_logger,
                occurred_at=observed_at,
                payload=last_meaningful_event,
            )
            alert_dispatcher.emit(
                severity="RECOVERY",
                code="safe_timeout_cleanup",
                message="Order acknowledgement was inferred safely from broker open-order truth.",
                payload={**repositories.runtime_identity, **last_meaningful_event},
                category="safe_repair_performed",
                title="Safe Timeout Repair",
                dedup_key=_timeout_dedup_key(repositories, pending, "ack_safe_repair"),
                active=False,
                coalesce=False,
                occurred_at=observed_at,
            )
            _sync_timeout_condition_alert(
                alert_dispatcher=alert_dispatcher,
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                active=False,
                category="missing_fill_ack",
                severity="RECOVERY",
                title="Missing Ack Resolved",
                message="Pending order acknowledgement is now confirmed by broker open-order truth.",
                recommended_action="No action needed.",
                dedup_suffix="ack_timeout",
                payload=base_payload,
            )
            continue

        if pending.acknowledged_at is None and age_since_submit >= int(settings.order_ack_timeout_seconds):
            overdue_ack_count += 1
            if not broker_has_open_order and broker_position_quantity == 0 and state.position_side == PositionSide.FLAT:
                _persist_pending_execution_row(
                    repositories=repositories,
                    pending=pending,
                    occurred_at=observed_at,
                    timeout_classification="broker_flat_no_open_order_safe_cleanup",
                    order_status=OrderStatus.CANCELLED,
                )
                execution_engine.clear_intent(pending.intent.order_intent_id)
                reconciliation = _reconcile_paper_runtime(
                    repositories=repositories,
                    strategy_engine=strategy_engine,
                    execution_engine=execution_engine,
                    trigger=MISSING_ACK_TIMEOUT_TRIGGER,
                    apply_repairs=True,
                    occurred_at=observed_at,
                )
                safe_repair_count += 1
                last_meaningful_event = {
                    **base_payload,
                    "timeout_classification": "broker_flat_no_open_order_safe_cleanup",
                    "repair_action": "clear_stale_pending_intent",
                    "reconciliation": reconciliation,
                    "resulting_state": reconciliation.get("resulting_strategy_status") or strategy_engine.state.strategy_status.value,
                }
                _log_execution_watchdog_event(
                    repositories=repositories,
                    structured_logger=structured_logger,
                    occurred_at=observed_at,
                    payload=last_meaningful_event,
                )
                alert_dispatcher.emit(
                    severity="RECOVERY",
                    code="safe_timeout_cleanup",
                    message="Pending order timed out and was safely cleared because broker truth is flat with no open order.",
                    payload={**repositories.runtime_identity, **last_meaningful_event},
                    category="safe_repair_performed",
                    title="Safe Timeout Cleanup",
                    dedup_key=_timeout_dedup_key(repositories, pending, "safe_cleanup"),
                    active=False,
                    coalesce=False,
                    occurred_at=observed_at,
                )
                _sync_timeout_condition_alert(
                    alert_dispatcher=alert_dispatcher,
                    repositories=repositories,
                    pending=pending,
                    occurred_at=observed_at,
                    active=False,
                    category="missing_fill_ack",
                    severity="RECOVERY",
                    title="Missing Ack Resolved",
                    message="Pending acknowledgement timeout resolved by safe broker-flat cleanup.",
                    recommended_action="No action needed.",
                    dedup_suffix="ack_timeout",
                    payload=base_payload,
                )
                continue

            if age_since_submit >= int(settings.order_ack_timeout_seconds) + int(settings.order_timeout_reconcile_grace_seconds):
                reconciliation = _reconcile_paper_runtime(
                    repositories=repositories,
                    strategy_engine=strategy_engine,
                    execution_engine=execution_engine,
                    trigger=MISSING_ACK_TIMEOUT_TRIGGER,
                    apply_repairs=True,
                    occurred_at=observed_at,
                )
                _persist_pending_execution_row(
                    repositories=repositories,
                    pending=pending,
                    occurred_at=observed_at,
                    timeout_classification=str(reconciliation.get("classification") or "ack_timeout"),
                )
                escalation_state = str(reconciliation.get("resulting_strategy_status") or strategy_engine.state.strategy_status.value).upper()
                last_escalation = {
                    **base_payload,
                    "timeout_classification": "ack_timeout_escalated",
                    "reconciliation_trigger": MISSING_ACK_TIMEOUT_TRIGGER,
                    "reconciliation": reconciliation,
                    "resulting_state": escalation_state,
                }
                last_meaningful_event = last_escalation
                _log_execution_watchdog_event(
                    repositories=repositories,
                    structured_logger=structured_logger,
                    occurred_at=observed_at,
                    payload=last_escalation,
                )
                _sync_timeout_condition_alert(
                    alert_dispatcher=alert_dispatcher,
                    repositories=repositories,
                    pending=pending,
                    occurred_at=observed_at,
                    active=False,
                    category="missing_fill_ack",
                    severity="RECOVERY",
                    title="Missing Ack Escalated",
                    message="Missing acknowledgement timeout was escalated into reconciliation handling.",
                    recommended_action="See the active reconciliation or fault surface for the next step.",
                    dedup_suffix="ack_timeout",
                    payload=last_escalation,
                )
                alert_dispatcher.emit(
                    severity="BLOCKING" if escalation_state == "FAULT" else "ACTION",
                    code="timeout_escalated",
                    message=(
                        "Missing acknowledgement timeout escalated to FAULT because execution ambiguity is unsafe."
                        if escalation_state == "FAULT"
                        else "Missing acknowledgement timeout escalated to reconciliation review."
                    ),
                    payload={**repositories.runtime_identity, **last_escalation},
                    category="reconciliation_mismatch",
                    title="Missing Ack Timeout Escalated",
                    dedup_key=_timeout_dedup_key(repositories, pending, f"ack-escalated:{escalation_state}"),
                    active=False,
                    coalesce=False,
                    occurred_at=observed_at,
                )
                if escalation_state in {"RECONCILING", "FAULT"}:
                    active_issue_rows.append(
                        {
                            "order_intent_id": pending.intent.order_intent_id,
                            "symbol": pending.intent.symbol,
                            "classification": reconciliation.get("classification"),
                            "reason": reconciliation.get("recommended_action"),
                            "recommended_action": reconciliation.get("recommended_action"),
                            "lane_id": repositories.runtime_identity.get("lane_id"),
                        }
                    )
                continue

            _persist_pending_execution_row(
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                timeout_classification="ack_overdue",
            )
            message = (
                "Order acknowledgement is overdue. Broker still shows the order as open."
                if broker_has_open_order
                else "Order acknowledgement is overdue and broker truth does not cleanly explain the pending state."
            )
            recommended_action = (
                "No manual action needed yet; the order is still open at broker."
                if broker_has_open_order
                else "Wait briefly for broker evidence; if the condition persists it will escalate to reconciliation."
            )
            _sync_timeout_condition_alert(
                alert_dispatcher=alert_dispatcher,
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                active=True,
                category="missing_fill_ack",
                severity="ACTION",
                title="Missing Acknowledgement",
                message=message,
                recommended_action=recommended_action,
                dedup_suffix="ack_timeout",
                payload={**base_payload, "timeout_classification": "ack_overdue"},
            )
            active_issue_rows.append(
                {
                    "order_intent_id": pending.intent.order_intent_id,
                    "symbol": pending.intent.symbol,
                    "classification": "ack_overdue",
                    "reason": message,
                    "recommended_action": recommended_action,
                    "lane_id": repositories.runtime_identity.get("lane_id"),
                }
            )
            continue

        if pending.acknowledged_at is not None and age_since_ack >= int(settings.order_fill_timeout_seconds):
            overdue_fill_count += 1
            if broker_has_open_order and age_since_ack < int(settings.order_fill_timeout_seconds) + int(settings.order_timeout_reconcile_grace_seconds):
                _persist_pending_execution_row(
                    repositories=repositories,
                    pending=pending,
                    occurred_at=observed_at,
                    timeout_classification="fill_missing_broker_order_still_open",
                )
                _sync_timeout_condition_alert(
                    alert_dispatcher=alert_dispatcher,
                    repositories=repositories,
                    pending=pending,
                    occurred_at=observed_at,
                    active=True,
                    category="fill_timeout",
                    severity="ACTION",
                    title="Fill Still Pending",
                    message="Order fill is overdue, but broker still shows the order as open.",
                    recommended_action="No manual action needed yet; waiting for broker order progression within the reconcile grace window.",
                    dedup_suffix="fill_timeout",
                    payload={**base_payload, "timeout_classification": "fill_missing_broker_order_still_open"},
                )
                active_issue_rows.append(
                    {
                        "order_intent_id": pending.intent.order_intent_id,
                        "symbol": pending.intent.symbol,
                        "classification": "fill_missing_broker_order_still_open",
                        "reason": "Fill overdue, broker still shows the order as open.",
                        "recommended_action": "Wait for broker progression; escalation happens only if the stall persists.",
                        "lane_id": repositories.runtime_identity.get("lane_id"),
                    }
                )
                continue

            trigger = FILL_TIMEOUT_TRIGGER if not broker_has_open_order else PENDING_ORDER_UNCERTAINTY_TRIGGER
            reconciliation = _reconcile_paper_runtime(
                repositories=repositories,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
                trigger=trigger,
                apply_repairs=True,
                occurred_at=observed_at,
            )
            _persist_pending_execution_row(
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                timeout_classification=str(reconciliation.get("classification") or "fill_timeout"),
            )
            escalation_state = str(reconciliation.get("resulting_strategy_status") or strategy_engine.state.strategy_status.value).upper()
            last_escalation = {
                **base_payload,
                "timeout_classification": "fill_timeout_escalated",
                "reconciliation_trigger": trigger,
                "reconciliation": reconciliation,
                "resulting_state": escalation_state,
            }
            last_meaningful_event = last_escalation
            _log_execution_watchdog_event(
                repositories=repositories,
                structured_logger=structured_logger,
                occurred_at=observed_at,
                payload=last_escalation,
            )
            _sync_timeout_condition_alert(
                alert_dispatcher=alert_dispatcher,
                repositories=repositories,
                pending=pending,
                occurred_at=observed_at,
                active=False,
                category="fill_timeout",
                severity="RECOVERY",
                title="Fill Timeout Escalated",
                message="Fill timeout condition was escalated into reconciliation handling.",
                recommended_action="See the active reconciliation or fault surface for the next step.",
                dedup_suffix="fill_timeout",
                payload=last_escalation,
            )
            alert_dispatcher.emit(
                severity="BLOCKING" if escalation_state == "FAULT" else "ACTION",
                code="timeout_escalated",
                message=(
                    "Pending order timeout escalated to FAULT because execution ambiguity is unsafe."
                    if escalation_state == "FAULT"
                    else "Pending order timeout escalated to reconciliation review."
                ),
                payload={**repositories.runtime_identity, **last_escalation},
                category="reconciliation_mismatch",
                title="Pending Order Timeout Escalated",
                dedup_key=_timeout_dedup_key(repositories, pending, f"escalated:{escalation_state}"),
                active=False,
                coalesce=False,
                occurred_at=observed_at,
            )
            if escalation_state in {"RECONCILING", "FAULT"}:
                active_issue_rows.append(
                    {
                        "order_intent_id": pending.intent.order_intent_id,
                        "symbol": pending.intent.symbol,
                        "classification": reconciliation.get("classification"),
                        "reason": reconciliation.get("recommended_action"),
                        "recommended_action": reconciliation.get("recommended_action"),
                        "lane_id": repositories.runtime_identity.get("lane_id"),
                    }
                )

    for row in previous.get("active_issue_rows") or []:
        order_intent_id = str(row.get("order_intent_id") or "").strip()
        if not order_intent_id or order_intent_id in current_pending_ids:
            continue
        symbol = str(row.get("symbol") or repositories.runtime_identity.get("instrument") or "").strip() or "UNKNOWN"
        classification = str(row.get("classification") or "").strip().lower()
        category = "missing_fill_ack" if "ack" in classification else "fill_timeout"
        suffix = "ack_timeout" if category == "missing_fill_ack" else "fill_timeout"
        alert_dispatcher.sync_condition(
            code=category,
            active=False,
            severity="RECOVERY",
            category=category,
            title="Pending Timeout Resolved",
            message="Pending-order timeout condition resolved because the order is no longer outstanding.",
            payload={**repositories.runtime_identity, **row},
            dedup_key=_timeout_dedup_key_for_order(
                repositories,
                order_intent_id=order_intent_id,
                symbol=symbol,
                suffix=suffix,
            ),
            recommended_action="No action needed.",
            occurred_at=observed_at,
        )

    if not broker_truth_available:
        return (
            {
                **previous,
                "enabled": True,
                "status": "BROKER_UNAVAILABLE",
                "last_checked_at": observed_at.isoformat(),
                "next_due_at": (observed_at + timedelta(seconds=cadence_seconds)).isoformat(),
                "broker_truth_available": False,
                "recommended_action": "Wait for broker truth to recover before treating pending-order state as resolved.",
                "reason": "Pending-order timeout watchdog skipped because broker truth is unavailable or incomplete.",
            },
            None,
            True,
        )

    status = "HEALTHY"
    if last_escalation is not None:
        escalation_state = str(last_escalation.get("resulting_state") or "").upper()
        status = "FAULT" if escalation_state == "FAULT" else "RECONCILING"
    elif safe_repair_count:
        status = "SAFE_REPAIR"
    elif overdue_ack_count or overdue_fill_count:
        status = "ACTIVE_TIMEOUTS"

    summary = {
        "enabled": True,
        "trigger_source": "order_lifecycle_watchdog",
        "cadence_seconds": cadence_seconds,
        "ack_timeout_seconds": int(settings.order_ack_timeout_seconds),
        "fill_timeout_seconds": int(settings.order_fill_timeout_seconds),
        "reconcile_grace_seconds": int(settings.order_timeout_reconcile_grace_seconds),
        "retry_limit": int(settings.order_timeout_retry_limit),
        "status": status,
        "last_checked_at": observed_at.isoformat(),
        "next_due_at": (observed_at + timedelta(seconds=cadence_seconds)).isoformat(),
        "overdue_ack_count": overdue_ack_count,
        "overdue_fill_count": overdue_fill_count,
        "active_issue_count": len(active_issue_rows),
        "active_issue_rows": active_issue_rows,
        "safe_repair_count": safe_repair_count,
        "broker_truth_available": True,
        "last_escalation": last_escalation,
        "recommended_action": (
            "Inspect reconciliation or fault details before resuming entries."
            if last_escalation is not None
            else "No manual action needed."
            if not active_issue_rows
            else active_issue_rows[0].get("recommended_action") or "Wait for broker progression or reconciliation."
        ),
        "reason": (
            "Pending-order timeout automation escalated an unresolved issue."
            if last_escalation is not None
            else "Pending-order timeout automation performed a safe repair."
            if safe_repair_count
            else "Pending-order timeouts are active."
            if active_issue_rows
            else "No active pending-order timeout issues."
        ),
    }
    return summary, last_meaningful_event, True


def _normalize_broker_status_value(payload: Any) -> str | None:
    if isinstance(payload, dict):
        value = payload.get("status") or payload.get("broker_order_status") or payload.get("order_status")
        return str(value).strip().upper() or None if value is not None else None
    if payload is None:
        return None
    return str(payload).strip().upper() or None


def _normalize_broker_open_order_ids(payload: Any) -> list[str]:
    rows = payload if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes)) else []
    values: list[str] = []
    for row in rows:
        if isinstance(row, str):
            broker_order_id = row
        elif isinstance(row, dict):
            broker_order_id = row.get("broker_order_id") or row.get("orderId") or row.get("order_id")
        else:
            broker_order_id = getattr(row, "broker_order_id", None) or getattr(row, "order_id", None)
        normalized = str(broker_order_id or "").strip()
        if normalized:
            values.append(normalized)
    return values


def _normalize_broker_position_truth(payload: Any) -> dict[str, Any]:
    if isinstance(payload, dict):
        quantity = payload.get("quantity") or payload.get("position_quantity") or 0
        average_price = payload.get("average_price") or payload.get("avg_price")
    else:
        quantity = getattr(payload, "quantity", 0)
        average_price = getattr(payload, "average_price", None)
    try:
        normalized_quantity = int(quantity or 0)
    except (TypeError, ValueError):
        normalized_quantity = 0
    return {
        "quantity": normalized_quantity,
        "average_price": str(average_price) if average_price is not None else None,
    }


def _latest_pending_execution(execution_engine: ExecutionEngine) -> PendingExecution | None:
    pending_rows = execution_engine.pending_executions()
    if not pending_rows:
        return None
    return max(pending_rows, key=lambda pending: pending.submitted_at)


def _live_timing_contract(settings: StrategySettings) -> dict[str, Any]:
    return {
        "symbol": settings.symbol,
        "timeframe": settings.timeframe,
        "completed_bar_only": True,
        "deterministic_sequential_processing": True,
        "position_transitions_on_fill_only": True,
        "flat_reset_on_exit_fill_only": True,
        "earliest_permissible_broker_submit": "same_completed_bar_cycle_after_intent_persist",
        "acknowledgement_window_seconds": int(settings.order_ack_timeout_seconds),
        "fill_confirmation_window_seconds": int(settings.order_fill_timeout_seconds),
        "reconcile_grace_seconds": int(settings.order_timeout_reconcile_grace_seconds),
        "broker_truth_decision_order": list(LIVE_TIMING_BROKER_TRUTH_DECISION_ORDER),
        "awaiting_ack_definition": "Intent exists, broker submit attempted, no broker acknowledgement timestamp yet.",
        "awaiting_fill_definition": "Broker acknowledged the order, but no confirmed fill truth exists yet.",
        "reconciling_definition": "Broker truth remains insufficient or mismatched after the allowed timing/grace path.",
        "fault_definition": "Execution ambiguity is unsafe or a persistence/invariant failure requires fail-closed halt.",
    }


def _live_timing_reconcile_trigger_source(
    *,
    latest_reconciliation: dict[str, Any] | None,
    latest_watchdog: dict[str, Any] | None,
) -> str | None:
    last_escalation = dict((latest_watchdog or {}).get("last_escalation") or {})
    return (
        str(last_escalation.get("reconciliation_trigger") or "").strip()
        or str((latest_reconciliation or {}).get("trigger") or "").strip()
        or None
    )


def _shadow_session_classification(latest_processed_end_ts: datetime | None) -> str | None:
    if latest_processed_end_ts is None:
        return None
    return label_session_phase(latest_processed_end_ts)


def _shadow_feature_summary(strategy_engine: StrategyEngine) -> dict[str, Any]:
    packet = getattr(strategy_engine, "_last_feature_packet", None)  # noqa: SLF001
    if packet is None:
        return {}
    return {
        "bar_id": packet.bar_id,
        "atr": str(packet.atr),
        "vwap": str(packet.vwap),
        "vol_ratio": str(packet.vol_ratio),
        "swing_low_confirmed": packet.swing_low_confirmed,
        "swing_high_confirmed": packet.swing_high_confirmed,
        "last_swing_low": str(packet.last_swing_low) if packet.last_swing_low is not None else None,
        "last_swing_high": str(packet.last_swing_high) if packet.last_swing_high is not None else None,
    }


def _shadow_signal_summary(strategy_engine: StrategyEngine) -> dict[str, Any]:
    packet = getattr(strategy_engine, "_last_signal_packet", None)  # noqa: SLF001
    if packet is None:
        return {}
    return {
        "bar_id": packet.bar_id,
        "long_entry_raw": packet.long_entry_raw,
        "short_entry_raw": packet.short_entry_raw,
        "long_entry": packet.long_entry,
        "short_entry": packet.short_entry,
        "long_entry_source": packet.long_entry_source,
        "short_entry_source": packet.short_entry_source,
    }


def _signal_payload_from_json(payload_json: Any) -> dict[str, Any]:
    if not payload_json:
        return {}
    try:
        payload = json.loads(str(payload_json))
    except Exception:
        return {}
    return payload if isinstance(payload, dict) else {}


def _primary_signal_family_failure(payload: dict[str, Any], family: str) -> str | None:
    for label, field_name in LIVE_SIGNAL_OBSERVABILITY_FAMILY_FIELDS.get(family, ()):
        if not bool(payload.get(field_name)):
            return label
    return None


def _derive_no_trade_reason(
    payload: dict[str, Any],
    *,
    bull_failure: str | None,
    asia_failure: str | None,
    bear_failure: str | None,
    anti_churn_long_suppressed: bool,
    anti_churn_short_suppressed: bool,
) -> str:
    if bool(payload.get("long_entry")) or bool(payload.get("short_entry")):
        return "actionable entry qualified on this completed bar"

    reasons: list[str] = []
    if anti_churn_long_suppressed:
        reasons.append("recentLongSetup suppressed a raw long candidate")
    if anti_churn_short_suppressed:
        reasons.append("recentShortSetup suppressed a raw short candidate")
    if bull_failure:
        reasons.append(f"bullSnapLong stalled at {bull_failure}")
    if asia_failure:
        reasons.append(f"asiaVWAPLong stalled at {asia_failure}")
    if bear_failure:
        reasons.append(f"bearSnapShort stalled at {bear_failure}")
    if reasons:
        return "; ".join(reasons[:3])
    return "no actionable entry candidate on this completed bar"


def _top_failed_predicates(counter: Counter[str]) -> list[dict[str, Any]]:
    return [
        {"predicate": predicate, "count": count}
        for predicate, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def _build_live_strategy_signal_observability_summary(
    *,
    settings: StrategySettings,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    observed = observed_at or datetime.now(timezone.utc)
    latest_processed_end_ts = repositories.processed_bars.latest_end_ts()
    if latest_processed_end_ts is None:
        return {
            "available": False,
            "generated_at": observed.isoformat(),
            "operator_path": LIVE_STRATEGY_PILOT_OPERATOR_PATH,
            "summary_line": "No persisted completed-bar signal packets are available yet for live pilot observability.",
        }

    session_date = latest_processed_end_ts.astimezone(settings.timezone_info).date()
    signal_rows = _load_table_rows_for_session_date(
        repositories.engine,
        signals_table,
        timestamp_column="created_at",
        session_date=session_date,
        timezone_info=settings.timezone_info,
    )
    bars = _load_bars_for_session_date(repositories.engine, session_date, settings)
    bars_by_id = {bar.bar_id: bar for bar in bars}

    sorted_signal_rows = sorted(signal_rows, key=lambda row: str(row.get("created_at") or ""))
    session_counts = {label: 0 for label, _ in LIVE_SIGNAL_OBSERVABILITY_COUNT_FIELDS}
    family_failures = {family: Counter() for family in LIVE_SIGNAL_OBSERVABILITY_FAMILY_FIELDS}
    recent_long_setup_count = 0
    recent_short_setup_count = 0
    anti_churn_long_suppressed_count = 0
    anti_churn_short_suppressed_count = 0
    bars_since_long_setup: int | None = None
    bars_since_short_setup: int | None = None
    per_bar_rows: list[dict[str, Any]] = []

    for row in sorted_signal_rows:
        payload = _signal_payload_from_json(row.get("payload_json"))
        if not payload:
            continue
        bar_id = str(row.get("bar_id") or payload.get("bar_id") or "").strip()
        bar = bars_by_id.get(bar_id)
        bar_end_ts = bar.end_ts if bar is not None else datetime.fromisoformat(str(row.get("created_at")))
        session_classification = label_session_phase(bar_end_ts)

        for label, field_name in LIVE_SIGNAL_OBSERVABILITY_COUNT_FIELDS:
            if bool(payload.get(field_name)):
                session_counts[label] += 1

        if bool(payload.get("recent_long_setup")):
            recent_long_setup_count += 1
        if bool(payload.get("recent_short_setup")):
            recent_short_setup_count += 1

        if bool(payload.get("long_entry_raw")):
            bars_since_long_setup = 0
        elif bars_since_long_setup is not None:
            bars_since_long_setup += 1

        if bool(payload.get("short_entry_raw")):
            bars_since_short_setup = 0
        elif bars_since_short_setup is not None:
            bars_since_short_setup += 1

        anti_churn_long_suppressed = (
            bool(payload.get("long_entry_raw"))
            and not bool(payload.get("long_entry"))
            and bool(payload.get("recent_long_setup"))
            and not bool(payload.get("first_bull_snap_turn"))
            and not bool(payload.get("asia_vwap_long_signal"))
        )
        anti_churn_short_suppressed = (
            bool(payload.get("short_entry_raw"))
            and not bool(payload.get("short_entry"))
            and bool(payload.get("recent_short_setup"))
            and not bool(payload.get("first_bear_snap_turn"))
        )
        if anti_churn_long_suppressed:
            anti_churn_long_suppressed_count += 1
        if anti_churn_short_suppressed:
            anti_churn_short_suppressed_count += 1

        bull_failure = None if bool(payload.get("long_entry")) else _primary_signal_family_failure(payload, "bullSnapLong")
        asia_failure = None if bool(payload.get("long_entry")) else _primary_signal_family_failure(payload, "asiaVWAPLong")
        bear_failure = None if bool(payload.get("short_entry")) else _primary_signal_family_failure(payload, "bearSnapShort")

        if bull_failure:
            family_failures["bullSnapLong"][bull_failure] += 1
        if asia_failure:
            family_failures["asiaVWAPLong"][asia_failure] += 1
        if bear_failure:
            family_failures["bearSnapShort"][bear_failure] += 1

        per_bar_rows.append(
            {
                "bar_id": bar_id or None,
                "bar_end_ts": bar_end_ts.isoformat(),
                "session_classification": session_classification,
                "why_no_trade": _derive_no_trade_reason(
                    payload,
                    bull_failure=bull_failure,
                    asia_failure=asia_failure,
                    bear_failure=bear_failure,
                    anti_churn_long_suppressed=anti_churn_long_suppressed,
                    anti_churn_short_suppressed=anti_churn_short_suppressed,
                ),
                "bull_snap_turn_candidate": bool(payload.get("bull_snap_turn_candidate")),
                "firstBullSnapTurn": bool(payload.get("first_bull_snap_turn")),
                "asia_reclaim_bar_raw": bool(payload.get("asia_reclaim_bar_raw")),
                "asia_hold_bar_ok": bool(payload.get("asia_hold_bar_ok")),
                "asia_acceptance_bar_ok": bool(payload.get("asia_acceptance_bar_ok")),
                "asiaVWAPLongSignal": bool(payload.get("asia_vwap_long_signal")),
                "bear_snap_location_ok": bool(payload.get("bear_snap_location_ok")),
                "bear_snap_turn_candidate": bool(payload.get("bear_snap_turn_candidate")),
                "firstBearSnapTurn": bool(payload.get("first_bear_snap_turn")),
                "longEntryRaw": bool(payload.get("long_entry_raw")),
                "shortEntryRaw": bool(payload.get("short_entry_raw")),
                "longEntry": bool(payload.get("long_entry")),
                "shortEntry": bool(payload.get("short_entry")),
                "recentLongSetup": bool(payload.get("recent_long_setup")),
                "recentShortSetup": bool(payload.get("recent_short_setup")),
                "barsSinceLongSetup": bars_since_long_setup,
                "barsSinceShortSetup": bars_since_short_setup,
                "antiChurnLongSuppressed": anti_churn_long_suppressed,
                "antiChurnShortSuppressed": anti_churn_short_suppressed,
                "bullSnapLongPrimaryFailure": bull_failure,
                "asiaVWAPLongPrimaryFailure": asia_failure,
                "bearSnapShortPrimaryFailure": bear_failure,
            }
        )

    top_failed_predicates = {
        family: _top_failed_predicates(counter)
        for family, counter in family_failures.items()
    }
    raw_vs_final = {
        "long": {
            "raw_candidates_seen": session_counts["longEntryRaw"],
            "final_entries_produced": session_counts["longEntry"],
        },
        "short": {
            "raw_candidates_seen": session_counts["shortEntryRaw"],
            "final_entries_produced": session_counts["shortEntry"],
        },
    }
    why_no_trade_so_far = (
        "No final entries yet. "
        f"Raw long candidates: {raw_vs_final['long']['raw_candidates_seen']} -> final long entries: {raw_vs_final['long']['final_entries_produced']}. "
        f"Raw short candidates: {raw_vs_final['short']['raw_candidates_seen']} -> final short entries: {raw_vs_final['short']['final_entries_produced']}."
        if session_counts["longEntry"] == 0 and session_counts["shortEntry"] == 0
        else "At least one final entry signal has been produced in this live session."
    )

    return {
        "available": True,
        "generated_at": observed.isoformat(),
        "operator_path": LIVE_STRATEGY_PILOT_OPERATOR_PATH,
        "session_date": session_date.isoformat(),
        "processed_bars_session": _count_bars_for_session_date(repositories.engine, session_date, settings),
        "signal_packets_session": len(per_bar_rows),
        "latest_processed_bar_end_ts": latest_processed_end_ts.isoformat(),
        "session_counts": session_counts,
        "raw_candidates_seen_vs_final_entries": raw_vs_final,
        "family_failure_breakdown": {
            family: dict(sorted(counter.items(), key=lambda item: (-item[1], item[0])))
            for family, counter in family_failures.items()
        },
        "top_failed_predicates": top_failed_predicates,
        "anti_churn": {
            "recentLongSetup_true_bars": recent_long_setup_count,
            "recentShortSetup_true_bars": recent_short_setup_count,
            "recentLongSetup_suppressed_bars": anti_churn_long_suppressed_count,
            "recentShortSetup_suppressed_bars": anti_churn_short_suppressed_count,
            "lastRecentLongSetup": per_bar_rows[-1]["recentLongSetup"] if per_bar_rows else None,
            "lastRecentShortSetup": per_bar_rows[-1]["recentShortSetup"] if per_bar_rows else None,
            "lastBarsSinceLongSetup": per_bar_rows[-1]["barsSinceLongSetup"] if per_bar_rows else None,
            "lastBarsSinceShortSetup": per_bar_rows[-1]["barsSinceShortSetup"] if per_bar_rows else None,
        },
        "why_no_trade_so_far": why_no_trade_so_far,
        "per_bar_rows": per_bar_rows,
        "summary_line": (
            f"bars={len(per_bar_rows)} | "
            f"longRaw={raw_vs_final['long']['raw_candidates_seen']} -> longEntry={raw_vs_final['long']['final_entries_produced']} | "
            f"shortRaw={raw_vs_final['short']['raw_candidates_seen']} -> shortEntry={raw_vs_final['short']['final_entries_produced']}"
        ),
    }


def _shadow_broker_truth_summary(
    *,
    broker_truth_snapshot: dict[str, Any] | None,
    strategy_engine: StrategyEngine,
    symbol: str,
) -> dict[str, Any]:
    snapshot = dict(broker_truth_snapshot or {})
    health = dict(snapshot.get("health") or {})
    reconciliation = dict(snapshot.get("reconciliation") or {})
    orders = dict(snapshot.get("orders") or {})
    portfolio = dict(snapshot.get("portfolio") or {})
    connection = dict(snapshot.get("connection") or {})
    accounts = dict(snapshot.get("accounts") or {})

    def _health_ok(name: str) -> bool:
        value = health.get(name)
        return isinstance(value, dict) and value.get("ok") is True

    target_symbol = str(symbol or "").strip().upper()
    open_rows = [
        dict(row)
        for row in list(orders.get("open_rows") or [])
        if isinstance(row, dict) and str(row.get("symbol") or "").strip().upper() == target_symbol
    ]
    position_rows = [
        dict(row)
        for row in list(portfolio.get("positions") or [])
        if isinstance(row, dict) and str(row.get("symbol") or "").strip().upper() == target_symbol
    ]
    latest_position_row = position_rows[0] if position_rows else {}
    raw_quantity = latest_position_row.get("quantity") or latest_position_row.get("position_quantity") or 0
    try:
        broker_position_qty = int(Decimal(str(raw_quantity)))
    except Exception:
        broker_position_qty = 0
    broker_reachable = _health_ok("broker_reachable")
    auth_ready = _health_ok("auth") or _health_ok("auth_healthy")
    account_selected = _health_ok("account_selected")
    orders_fresh = _health_ok("orders_fresh")
    positions_fresh = _health_ok("positions_fresh")
    reconciliation_status = str(reconciliation.get("status") or "").strip().lower() or "unknown"
    reconciliation_clear = reconciliation_status == "clear"

    state = strategy_engine.state
    mismatch_reasons: list[str] = []
    if state.position_side is PositionSide.FLAT and broker_position_qty != 0:
        mismatch_reasons.append("broker_position_mismatch")
    if state.open_broker_order_id and not any(
        str(row.get("broker_order_id") or "").strip() == str(state.open_broker_order_id or "").strip()
        for row in open_rows
    ):
        mismatch_reasons.append("pending_order_missing_from_broker_truth")

    missing_or_ambiguous: list[str] = []
    if not snapshot:
        missing_or_ambiguous.append("snapshot_unavailable")
    if not broker_reachable:
        missing_or_ambiguous.append("broker_unreachable")
    if not auth_ready:
        missing_or_ambiguous.append("auth_not_ready")
    if not account_selected:
        missing_or_ambiguous.append("account_not_selected")
    if not orders_fresh:
        missing_or_ambiguous.append("orders_not_fresh")
    if not positions_fresh:
        missing_or_ambiguous.append("positions_not_fresh")
    if not reconciliation_clear:
        missing_or_ambiguous.append("broker_reconciliation_not_clear")
    missing_or_ambiguous.extend(mismatch_reasons)

    if broker_reachable and auth_ready and account_selected and orders_fresh and positions_fresh and reconciliation_clear and not mismatch_reasons:
        classification = "SUFFICIENT_BROKER_TRUTH"
    elif mismatch_reasons:
        classification = "CONFLICTING_TRUTH_RECONCILE"
    elif not broker_reachable or not auth_ready or not account_selected or not reconciliation_clear:
        classification = "INSUFFICIENT_TRUTH_RECONCILE"
    else:
        classification = "PARTIAL_BUT_USABLE_TRUTH"

    requires_reconciliation = (
        classification in {"INSUFFICIENT_TRUTH_RECONCILE", "CONFLICTING_TRUTH_RECONCILE"}
        or state.reconcile_required
        or state.strategy_status is StrategyStatus.RECONCILING
    )
    blocker = mismatch_reasons[0] if mismatch_reasons else missing_or_ambiguous[0] if missing_or_ambiguous else None
    reconcile_trigger_source = None
    if mismatch_reasons:
        reconcile_trigger_source = "broker_truth_mismatch"
    elif not reconciliation_clear:
        reconcile_trigger_source = "broker_reconciliation"
    elif not orders_fresh or not positions_fresh:
        reconcile_trigger_source = "broker_truth_freshness"
    elif not broker_reachable or not auth_ready or not account_selected:
        reconcile_trigger_source = "broker_account_health"

    return {
        "classification": classification,
        "selected_account_hash": str(connection.get("selected_account_hash") or accounts.get("selected_account_hash") or "").strip() or None,
        "broker_reachable": broker_reachable,
        "auth_ready": auth_ready,
        "account_selected": account_selected,
        "orders_fresh": orders_fresh,
        "positions_fresh": positions_fresh,
        "reconciliation_status": reconciliation.get("status"),
        "reconciliation_detail": reconciliation.get("detail"),
        "reconciliation_mismatch_count": reconciliation.get("mismatch_count"),
        "open_orders_for_symbol": open_rows,
        "position_for_symbol": latest_position_row or None,
        "broker_position_qty": broker_position_qty,
        "requires_reconciliation": requires_reconciliation,
        "requires_fault": state.strategy_status is StrategyStatus.FAULT,
        "blocker": blocker,
        "reconcile_trigger_source": reconcile_trigger_source,
        "missing_or_ambiguous_fields": missing_or_ambiguous,
        "snapshot_status": snapshot.get("status"),
        "snapshot_detail": snapshot.get("detail"),
    }


def _build_live_shadow_summary(
    *,
    settings: StrategySettings,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    broker_truth_snapshot: dict[str, Any] | None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    observed = observed_at or datetime.now(timezone.utc)
    latest_processed_end_ts = repositories.processed_bars.latest_end_ts()
    broker_truth_summary = _shadow_broker_truth_summary(
        broker_truth_snapshot=broker_truth_snapshot,
        strategy_engine=strategy_engine,
        symbol=settings.symbol,
    )
    runtime_phase = _paper_soak_runtime_phase(strategy_engine)
    if broker_truth_summary.get("requires_fault"):
        runtime_phase = "FAULT"
    elif broker_truth_summary.get("requires_reconciliation"):
        runtime_phase = "RECONCILING"
    state_snapshot = _restore_validation_state_snapshot(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=strategy_engine._execution_engine,  # noqa: SLF001
    )
    entry_blocker = broker_truth_summary.get("blocker") or _paper_soak_entry_blocker(strategy_engine)
    latest_shadow_intent = strategy_engine.latest_shadow_intent_summary()
    latest_exit_decision = strategy_engine.latest_exit_decision_summary()
    last_processed_bar_id = None
    if latest_processed_end_ts is not None:
        last_processed_bar_id = f"{settings.symbol}|{settings.timeframe}|{latest_processed_end_ts.astimezone(timezone.utc).isoformat()}"
    submit_would_be_allowed = (
        entry_blocker is None
        and runtime_phase in {"READY", "IN_POSITION"}
        and state_snapshot.get("fault_code") is None
    )
    return {
        "generated_at": observed.isoformat(),
        "operator_path": "mgc-v05l probationary-live-shadow",
        "allowed_scope": {
            "symbol": settings.symbol,
            "timeframe": settings.timeframe,
            "mode": "LIVE_SHADOW_NO_SUBMIT",
            "completed_bar_only": True,
            "deterministic_sequential_processing": True,
            "strategy_submit_enabled": False,
        },
        "current_runtime_phase": runtime_phase,
        "strategy_state": strategy_engine.state.strategy_status.value,
        "position_state": {
            "side": state_snapshot.get("position_side"),
            "internal_qty": state_snapshot.get("internal_position_qty"),
            "broker_qty": state_snapshot.get("broker_position_qty"),
            "entry_bar_id": state_snapshot.get("entry_bar_id"),
            "long_entry_family": state_snapshot.get("long_entry_family"),
            "short_entry_family": state_snapshot.get("short_entry_family"),
        },
        "last_finalized_live_bar_id": last_processed_bar_id,
        "last_finalized_live_bar_end_ts": latest_processed_end_ts.isoformat() if latest_processed_end_ts is not None else None,
        "session_classification": _shadow_session_classification(latest_processed_end_ts),
        "latest_feature_summary": _shadow_feature_summary(strategy_engine),
        "latest_signal_summary": _shadow_signal_summary(strategy_engine),
        "latest_exit_decision": latest_exit_decision,
        "latest_shadow_intent": latest_shadow_intent,
        "shadow_submit_suppressed": True,
        "submit_would_be_allowed_if_shadow_disabled": submit_would_be_allowed,
        "entries_disabled_blocker": entry_blocker,
        "pending_reason": "shadow_submit_suppressed" if latest_shadow_intent else "no_actionable_intent",
        "pending_stage": "SHADOW_INTENT_SUPPRESSED" if latest_shadow_intent else "IDLE",
        "reconcile_trigger_source": broker_truth_summary.get("reconcile_trigger_source"),
        "fault_code": strategy_engine.state.fault_code,
        "broker_truth_summary": broker_truth_summary,
        "summary_line": (
            f"phase={runtime_phase} | "
            f"last_bar={last_processed_bar_id or 'NONE'} | "
            f"submit={'WOULD_SUBMIT' if submit_would_be_allowed else 'BLOCKED'} | "
            f"blocker={entry_blocker or 'none'}"
        ),
    }


def _latest_runtime_bar(strategy_engine: StrategyEngine) -> Bar | None:
    history = getattr(strategy_engine, "_bar_history", [])  # noqa: SLF001
    return history[-1] if history else None


def _load_live_strategy_broker_truth_snapshot(
    *,
    execution_engine: ExecutionEngine,
    broker_truth_service: SchwabProductionLinkService | Any | None = None,
    force_refresh: bool = True,
) -> dict[str, Any]:
    broker = execution_engine.broker
    if broker_truth_service is not None:
        try:
            payload = broker_truth_service.snapshot(force_refresh=force_refresh)
            snapshot = dict(payload) if isinstance(payload, dict) else {}
            refresh = getattr(broker, "refresh_from_snapshot", None)
            if callable(refresh):
                refresh(snapshot)
            return snapshot
        except Exception as exc:
            return {
                "status": "degraded",
                "detail": str(exc),
                "health": {
                    "broker_reachable": {"ok": False, "label": "BROKER DEGRADED", "detail": str(exc)},
                    "account_selected": {"ok": False, "label": "ACCOUNT UNKNOWN", "detail": str(exc)},
                    "orders_fresh": {"ok": False, "label": "ORDERS STALE", "detail": str(exc)},
                    "positions_fresh": {"ok": False, "label": "POSITIONS STALE", "detail": str(exc)},
                    "auth": {"ok": False, "label": "AUTH UNKNOWN", "detail": str(exc)},
                },
                "reconciliation": {
                    "status": "blocked",
                    "label": "BROKER TRUTH DEGRADED",
                    "detail": str(exc),
                    "mismatch_count": None,
                },
            }
    load_snapshot = getattr(broker, "load_snapshot", None)
    if callable(load_snapshot):
        try:
            payload = load_snapshot(force_refresh=force_refresh)
            return dict(payload) if isinstance(payload, dict) else {}
        except Exception:
            return {}
    return {}


def _live_strategy_pilot_gate_status(
    *,
    settings: StrategySettings,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    broker_truth_snapshot: dict[str, Any],
    latest_reconciliation: dict[str, Any] | None = None,
    latest_watchdog: dict[str, Any] | None = None,
    bar: Bar | None = None,
    intent: OrderIntent | None = None,
    pilot_cycle_state: dict[str, Any] | None = None,
) -> dict[str, Any]:
    state = strategy_engine.state
    latest_bar = bar or _latest_runtime_bar(strategy_engine)
    cycle_state = dict(pilot_cycle_state or _load_live_strategy_pilot_cycle_state(settings))
    broker_truth_summary = _shadow_broker_truth_summary(
        broker_truth_snapshot=broker_truth_snapshot,
        strategy_engine=strategy_engine,
        symbol=settings.symbol,
    )
    warmup_bars_loaded = len(getattr(strategy_engine, "_bar_history", []))  # noqa: SLF001
    warmup_bars_required = settings.warmup_bars_required()
    warmup_complete = warmup_bars_loaded >= warmup_bars_required
    state_blocker = _paper_soak_entry_blocker(
        strategy_engine,
        latest_reconciliation=latest_reconciliation,
        latest_watchdog=latest_watchdog,
    )
    blockers: list[str] = []
    if not settings.live_strategy_pilot_enabled:
        blockers.append("live_strategy_pilot_disabled")
    if not settings.live_strategy_pilot_submit_enabled:
        blockers.append("live_strategy_pilot_submit_disabled")
    if settings.mode is not RuntimeMode.LIVE:
        blockers.append("runtime_mode_not_live")
    if settings.symbol != "MGC":
        blockers.append("symbol_out_of_scope")
    if settings.timeframe != "5m":
        blockers.append("timeframe_out_of_scope")
    if settings.trade_size != 1:
        blockers.append("trade_size_out_of_scope")
    if int(settings.live_strategy_pilot_max_quantity) != 1:
        blockers.append("pilot_quantity_cap_not_one")
    if intent is not None and int(intent.quantity) != 1:
        blockers.append("intent_quantity_out_of_scope")
    if latest_bar is None:
        blockers.append("no_finalized_bar_processed")
    elif not latest_bar.is_final:
        blockers.append("completed_bar_gate_unsatisfied")
    if not warmup_complete:
        blockers.append("warmup_incomplete")
    if latest_bar is not None and not latest_bar.session_allowed:
        blockers.append("session_disallowed")
    if settings.live_strategy_pilot_regular_hours_only and latest_bar is not None and not latest_bar.session_us:
        blockers.append("regular_hours_only_gate_unsatisfied")
    if state.position_side is not PositionSide.FLAT and intent is not None and intent.is_entry:
        blockers.append("existing_position_not_flat")
    cycle_blocker = _live_strategy_cycle_submit_blocker(cycle_state, intent)
    if cycle_blocker:
        blockers.append(cycle_blocker)
    if broker_truth_summary.get("blocker"):
        blockers.append(str(broker_truth_summary["blocker"]))
    if state_blocker and not (
        intent is not None
        and intent.is_exit
        and state.position_side is not PositionSide.FLAT
        and state_blocker == state.strategy_status.value
    ):
        blockers.append(state_blocker)
    deduped_blockers = list(dict.fromkeys(item for item in blockers if item))
    return {
        "pilot_mode_enabled": settings.live_strategy_pilot_enabled,
        "submit_enabled_flag": settings.live_strategy_pilot_submit_enabled,
        "warmup_complete": warmup_complete,
        "warmup_bars_loaded": warmup_bars_loaded,
        "warmup_bars_required": warmup_bars_required,
        "completed_bar_gate_satisfied": bool(latest_bar is not None and latest_bar.is_final),
        "regular_hours_gate_satisfied": (
            None
            if latest_bar is None
            else (latest_bar.session_us if settings.live_strategy_pilot_regular_hours_only else latest_bar.session_allowed)
        ),
        "single_cycle_mode": bool(settings.live_strategy_pilot_single_cycle_mode),
        "pilot_cycle": cycle_state,
        "remaining_allowed_live_submits": cycle_state.get("remaining_allowed_live_submits"),
        "broker_truth_summary": broker_truth_summary,
        "blockers": deduped_blockers,
        "blocker": deduped_blockers[0] if deduped_blockers else None,
        "submit_eligible": not deduped_blockers,
    }


def _live_strategy_submit_gate_blocker(
    *,
    settings: StrategySettings,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    broker_truth_service: SchwabProductionLinkService | Any | None,
    bar: Bar,
    intent: OrderIntent,
) -> str | None:
    snapshot = _load_live_strategy_broker_truth_snapshot(
        execution_engine=execution_engine,
        broker_truth_service=broker_truth_service,
        force_refresh=True,
    )
    gate_status = _live_strategy_pilot_gate_status(
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        broker_truth_snapshot=snapshot,
        bar=bar,
        intent=intent,
    )
    return gate_status.get("blocker")


def _build_live_timing_summary(
    *,
    settings: StrategySettings,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    latest_reconciliation: dict[str, Any] | None = None,
    latest_watchdog: dict[str, Any] | None = None,
    latest_restore: dict[str, Any] | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    observed = observed_at or datetime.now(timezone.utc)
    latest_processed_end_ts = repositories.processed_bars.latest_end_ts()
    state_snapshot = _restore_validation_state_snapshot(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    latest_intent = dict(state_snapshot.get("latest_order_intent") or {})
    latest_fill = dict(state_snapshot.get("latest_fill") or {})
    latest_submit_attempt = dict(execution_engine.last_submit_attempt() or {})
    submit_failure = execution_engine.last_submit_failure()
    pending = _latest_pending_execution(execution_engine)

    broker = execution_engine.broker
    broker_connected = bool(broker.is_connected())
    direct_status_error: str | None = None
    open_orders_error: str | None = None
    position_error: str | None = None
    account_health_error: str | None = None
    direct_status_payload: Any = None
    open_orders_payload: Any = []
    position_payload: Any = None
    account_health_payload: Any = None
    if broker_connected:
        if pending is not None:
            try:
                direct_status_payload = broker.get_order_status(pending.broker_order_id)
            except Exception as exc:
                direct_status_error = str(exc)
        try:
            open_orders_payload = broker.get_open_orders()
        except Exception as exc:
            open_orders_error = str(exc)
        try:
            position_payload = broker.get_position()
        except Exception as exc:
            position_error = str(exc)
        try:
            account_health_payload = broker.get_account_health()
        except Exception as exc:
            account_health_error = str(exc)

    direct_status = _normalize_broker_status_value(direct_status_payload)
    open_order_ids = _normalize_broker_open_order_ids(open_orders_payload)
    position_truth = _normalize_broker_position_truth(position_payload)
    current_runtime_phase = _paper_soak_runtime_phase(
        strategy_engine,
        latest_reconciliation=latest_reconciliation,
        latest_watchdog=latest_watchdog,
    )
    broker_order_id = (
        (pending.broker_order_id if pending is not None else None)
        or str(latest_intent.get("broker_order_id") or "").strip()
        or str(state_snapshot.get("open_broker_order_id") or "").strip()
        or None
    )
    latest_fill_broker_order_id = str(latest_fill.get("broker_order_id") or "").strip() or None
    fill_truth_confirms_current = bool(
        latest_fill
        and broker_order_id
        and latest_fill_broker_order_id == broker_order_id
    )
    terminal_non_fill_statuses = {"REJECTED", "CANCELLED", "CANCELED", "EXPIRED"}
    if current_runtime_phase == "FAULT":
        pending_stage = LIVE_TIMING_STAGE_FAULTED
    elif current_runtime_phase == "RECONCILING":
        pending_stage = LIVE_TIMING_STAGE_RECONCILING
    elif fill_truth_confirms_current:
        pending_stage = LIVE_TIMING_STAGE_FILLED
    elif direct_status in terminal_non_fill_statuses and broker_order_id not in open_order_ids and position_truth["quantity"] == 0:
        pending_stage = LIVE_TIMING_STAGE_TERMINAL_NON_FILL
    elif pending is not None and pending.acknowledged_at is None:
        pending_stage = LIVE_TIMING_STAGE_AWAITING_ACK
    elif pending is not None:
        pending_stage = LIVE_TIMING_STAGE_AWAITING_FILL
    else:
        pending_stage = LIVE_TIMING_STAGE_IDLE

    pending_since = (
        pending.acknowledged_at.isoformat()
        if pending is not None and pending.acknowledged_at is not None
        else pending.submitted_at.isoformat()
        if pending is not None
        else latest_intent.get("submitted_at")
        or latest_intent.get("created_at")
        if pending_stage in {LIVE_TIMING_STAGE_AWAITING_ACK, LIVE_TIMING_STAGE_AWAITING_FILL}
        else None
    )
    last_escalation = dict((latest_watchdog or {}).get("last_escalation") or {})
    pending_reason = (
        str(submit_failure.error).strip()
        if submit_failure is not None
        else str(last_escalation.get("timeout_classification") or "").strip()
        or str((latest_reconciliation or {}).get("classification") or "").strip()
        or "awaiting_broker_ack"
        if pending_stage == LIVE_TIMING_STAGE_AWAITING_ACK
        else "awaiting_broker_fill"
        if pending_stage == LIVE_TIMING_STAGE_AWAITING_FILL
        else "no_pending_execution"
        if pending_stage == LIVE_TIMING_STAGE_IDLE
        else "runtime_fault"
        if pending_stage == LIVE_TIMING_STAGE_FAULTED
        else "runtime_reconciling"
        if pending_stage == LIVE_TIMING_STAGE_RECONCILING
        else "fill_confirmed"
        if pending_stage == LIVE_TIMING_STAGE_FILLED
        else "terminal_non_fill_confirmed"
    )
    return {
        "generated_at": observed.isoformat(),
        "contract": _live_timing_contract(settings),
        "runtime_phase": current_runtime_phase,
        "strategy_state": strategy_engine.state.strategy_status.value,
        "position_side": state_snapshot.get("position_side"),
        "position_state": {
            "side": state_snapshot.get("position_side"),
            "internal_qty": state_snapshot.get("internal_position_qty"),
            "broker_qty": state_snapshot.get("broker_position_qty"),
            "entry_price": state_snapshot.get("entry_price"),
        },
        "evaluated_bar_id": strategy_engine.state.last_signal_bar_id or latest_intent.get("bar_id"),
        "evaluated_bar_end_ts": latest_processed_end_ts.isoformat() if latest_processed_end_ts is not None else None,
        "intent_created_at": latest_intent.get("created_at"),
        "submit_attempted_at": latest_intent.get("submitted_at") or latest_submit_attempt.get("submit_attempted_at"),
        "broker_ack_at": latest_intent.get("acknowledged_at") or (pending.acknowledged_at.isoformat() if pending and pending.acknowledged_at else None),
        "broker_fill_at": latest_fill.get("fill_timestamp"),
        "pending_since": pending_since,
        "pending_reason": pending_reason,
        "pending_stage": pending_stage,
        "awaiting_ack": pending_stage == LIVE_TIMING_STAGE_AWAITING_ACK,
        "awaiting_fill": pending_stage == LIVE_TIMING_STAGE_AWAITING_FILL,
        "reconciling": pending_stage == LIVE_TIMING_STAGE_RECONCILING,
        "faulted": pending_stage == LIVE_TIMING_STAGE_FAULTED,
        "reconcile_trigger_source": _live_timing_reconcile_trigger_source(
            latest_reconciliation=latest_reconciliation,
            latest_watchdog=latest_watchdog,
        ),
        "submit_failure": (
            {
                "order_intent_id": submit_failure.order_intent_id,
                "failure_stage": submit_failure.failure_stage,
                "error": submit_failure.error,
                "submit_attempted_at": submit_failure.submit_attempted_at.isoformat(),
            }
            if submit_failure is not None
            else None
        ),
        "latest_order_intent": latest_intent,
        "latest_fill": latest_fill,
        "latest_restore_result": (latest_restore or {}).get("restore_result"),
        "entries_disabled_blocker": _paper_soak_entry_blocker(
            strategy_engine,
            latest_reconciliation=latest_reconciliation,
            latest_watchdog=latest_watchdog,
        )
        or (submit_failure.error if submit_failure is not None else None),
        "broker_truth": {
            "decision_order": list(LIVE_TIMING_BROKER_TRUTH_DECISION_ORDER),
            "connected": broker_connected,
            "direct_order_status": direct_status,
            "direct_order_status_payload": direct_status_payload,
            "direct_order_status_error": direct_status_error,
            "open_order_ids": open_order_ids,
            "open_orders_error": open_orders_error,
            "position_quantity": position_truth["quantity"],
            "position_average_price": position_truth["average_price"],
            "position_error": position_error,
            "account_health": account_health_payload,
            "account_health_error": account_health_error,
            "fill_truth_available": bool(latest_fill),
        },
        "summary_line": (
            f"stage={pending_stage} | "
            f"bar={strategy_engine.state.last_signal_bar_id or 'NONE'} | "
            f"intent={latest_intent.get('intent_type') or 'NONE'} | "
            f"ack={latest_intent.get('acknowledged_at') or 'NONE'} | "
            f"fill={latest_fill.get('fill_timestamp') or 'NONE'}"
        ),
    }


def _extract_live_fill_details(
    *,
    pending: PendingExecution,
    direct_status_payload: dict[str, Any] | None,
    position_payload: Any,
) -> tuple[datetime | None, Decimal | None]:
    payload = dict(direct_status_payload or {})
    fill_timestamp = _parse_iso_datetime_or_none(
        payload.get("fill_timestamp")
        or payload.get("closed_at")
        or payload.get("updated_at")
    )
    fill_price_raw = payload.get("fill_price")
    fill_price: Decimal | None = None
    if fill_price_raw not in (None, ""):
        fill_price = Decimal(str(fill_price_raw))
    if fill_price is None and pending.intent.is_entry:
        if isinstance(position_payload, dict):
            average_price = (
                position_payload.get("average_price")
                or position_payload.get("avg_price")
                or position_payload.get("average_cost")
            )
        else:
            average_price = (
                getattr(position_payload, "average_price", None)
                or getattr(position_payload, "avg_price", None)
                or getattr(position_payload, "average_cost", None)
            )
        if average_price not in (None, ""):
            fill_price = Decimal(str(average_price))
    return fill_timestamp, fill_price


def _run_live_strategy_fill_sync(
    *,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    observed_at: datetime,
) -> dict[str, Any]:
    broker = execution_engine.broker
    latest_event: dict[str, Any] | None = None
    processed_count = 0
    applied_fill_count = 0
    terminal_non_fill_count = 0
    for pending in list(execution_engine.pending_executions()):
        processed_count += 1
        direct_status_payload = broker.get_order_status(pending.broker_order_id) or {}
        direct_status = _normalize_broker_status_value(direct_status_payload)
        open_orders_payload = broker.get_open_orders()
        open_order_ids = _normalize_broker_open_order_ids(open_orders_payload)
        position_payload = broker.get_position()
        position_truth = _normalize_broker_position_truth(position_payload)

        acknowledged = direct_status in LIVE_STRATEGY_ACKNOWLEDGED_STATUSES or pending.broker_order_id in open_order_ids
        pending = execution_engine.observe_pending_status(
            pending.intent.order_intent_id,
            broker_order_status=direct_status,
            observed_at=observed_at,
            acknowledged=acknowledged,
        ) or pending
        _persist_pending_execution_row(
            repositories=repositories,
            pending=pending,
            occurred_at=observed_at,
            timeout_classification=None,
        )

        signed_quantity = int(position_truth.get("quantity") or 0)
        if pending.intent.intent_type is OrderIntentType.BUY_TO_OPEN:
            expected_signed_quantity = int(pending.intent.quantity)
        elif pending.intent.intent_type is OrderIntentType.SELL_TO_OPEN:
            expected_signed_quantity = -int(pending.intent.quantity)
        elif pending.intent.intent_type is OrderIntentType.SELL_TO_CLOSE:
            expected_signed_quantity = 0
        else:
            expected_signed_quantity = 0

        fill_confirmed = False
        if direct_status == OrderStatus.FILLED.value:
            fill_confirmed = True
        elif pending.intent.is_entry and signed_quantity == expected_signed_quantity and pending.broker_order_id not in open_order_ids:
            fill_confirmed = True
        elif pending.intent.is_exit and signed_quantity == 0 and pending.broker_order_id not in open_order_ids:
            fill_confirmed = True

        if fill_confirmed:
            fill_timestamp, fill_price = _extract_live_fill_details(
                pending=pending,
                direct_status_payload=dict(direct_status_payload or {}),
                position_payload=position_payload,
            )
            if pending.intent.is_entry and fill_price is None:
                latest_event = {
                    "order_intent_id": pending.intent.order_intent_id,
                    "broker_order_id": pending.broker_order_id,
                    "event": "fill_truth_missing_price",
                    "direct_status": direct_status,
                    "position_quantity": signed_quantity,
                }
                continue
            fill_event = FillEvent(
                order_intent_id=pending.intent.order_intent_id,
                intent_type=pending.intent.intent_type,
                order_status=OrderStatus.FILLED,
                fill_timestamp=fill_timestamp or observed_at,
                fill_price=fill_price,
                broker_order_id=pending.broker_order_id,
                quantity=pending.intent.quantity,
            )
            strategy_engine.apply_fill(
                fill_event=fill_event,
                signal_bar_id=pending.signal_bar_id,
                long_entry_family=pending.long_entry_family,
                short_entry_family=pending.short_entry_family,
                short_entry_source=pending.short_entry_source,
            )
            repositories.order_intents.save(
                pending.intent,
                order_status=OrderStatus.FILLED,
                broker_order_id=pending.broker_order_id,
                submitted_at=pending.submitted_at,
                acknowledged_at=pending.acknowledged_at or observed_at,
                broker_order_status=direct_status or OrderStatus.FILLED.value,
                last_status_checked_at=observed_at,
                retry_count=pending.retry_count,
            )
            execution_engine.clear_intent(pending.intent.order_intent_id)
            applied_fill_count += 1
            latest_event = {
                "order_intent_id": pending.intent.order_intent_id,
                "broker_order_id": pending.broker_order_id,
                "event": "fill_confirmed",
                "direct_status": direct_status,
                "position_quantity": signed_quantity,
                "fill_timestamp": fill_event.fill_timestamp.isoformat(),
                "fill_price": str(fill_event.fill_price) if fill_event.fill_price is not None else None,
            }
            continue

        if (
            direct_status in LIVE_STRATEGY_TERMINAL_NON_FILL_STATUSES
            and pending.broker_order_id not in open_order_ids
            and signed_quantity == _strategy_state_to_signed_quantity(strategy_engine.state)
        ):
            terminal_status = OrderStatus.CANCELLED if direct_status in {"CANCELLED", "CANCELED", "EXPIRED"} else OrderStatus.REJECTED
            repositories.order_intents.save(
                pending.intent,
                order_status=terminal_status,
                broker_order_id=pending.broker_order_id,
                submitted_at=pending.submitted_at,
                acknowledged_at=pending.acknowledged_at,
                broker_order_status=direct_status,
                last_status_checked_at=observed_at,
                retry_count=pending.retry_count,
            )
            execution_engine.clear_intent(pending.intent.order_intent_id)
            terminal_non_fill_count += 1
            latest_event = {
                "order_intent_id": pending.intent.order_intent_id,
                "broker_order_id": pending.broker_order_id,
                "event": "terminal_non_fill_confirmed",
                "direct_status": direct_status,
                "position_quantity": signed_quantity,
            }

    return {
        "checked_at": observed_at.isoformat(),
        "processed_pending_count": processed_count,
        "applied_fill_count": applied_fill_count,
        "terminal_non_fill_count": terminal_non_fill_count,
        "latest_event": latest_event,
    }


def _live_strategy_pilot_cycle_path(settings: StrategySettings) -> Path:
    return settings.probationary_artifacts_path / "live_strategy_pilot_cycle_latest.json"


def _default_live_strategy_pilot_cycle_state(
    *,
    settings: StrategySettings,
    armed_at: datetime | None = None,
) -> dict[str, Any]:
    observed = armed_at or datetime.now(timezone.utc)
    return {
        "generated_at": observed.isoformat(),
        "pilot_armed_at": observed.isoformat(),
        "pilot_disarmed_at": None,
        "pilot_armed": bool(settings.live_strategy_pilot_enabled and settings.live_strategy_pilot_submit_enabled),
        "rearm_required": False,
        "submit_enabled": bool(settings.live_strategy_pilot_enabled and settings.live_strategy_pilot_submit_enabled),
        "cycle_status": "waiting_for_entry",
        "remaining_allowed_live_submits": 2,
        "entry": {},
        "exit": {},
        "flat_restore_confirmation_time": None,
        "final_reconcile_status": None,
        "passive_refresh_restart_remained_passive": None,
        "final_result": None,
        "blocker": None,
        "reconcile_fault_reason": None,
        "auto_stop_reason": None,
        "rearm_action": LIVE_STRATEGY_PILOT_REARM_ACTION,
    }


def _load_live_strategy_pilot_cycle_state(settings: StrategySettings) -> dict[str, Any]:
    path = _live_strategy_pilot_cycle_path(settings)
    payload = _read_json(path)
    if payload:
        return payload
    return _default_live_strategy_pilot_cycle_state(settings=settings)


def _live_strategy_cycle_submit_blocker(cycle_state: dict[str, Any] | None, intent: OrderIntent | None) -> str | None:
    payload = dict(cycle_state or {})
    if not payload:
        return None
    armed = payload.get("pilot_armed")
    if armed is False:
        return str(payload.get("blocker") or "live_strategy_pilot_rearm_required")
    if intent is None:
        return None
    entry = dict(payload.get("entry") or {})
    exit_leg = dict(payload.get("exit") or {})
    if intent.is_entry and entry.get("submit_attempted_at"):
        return "live_strategy_pilot_entry_already_used"
    if intent.is_exit:
        if not entry.get("submit_attempted_at"):
            return "live_strategy_pilot_exit_before_entry_not_allowed"
        if exit_leg.get("submit_attempted_at"):
            return "live_strategy_pilot_exit_already_used"
    return None


def _intent_row_sort_key(row: dict[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("submitted_at") or row.get("created_at") or ""),
        str(row.get("order_intent_id") or ""),
    )


def _first_live_intent_row(rows: list[dict[str, Any]], *, entry: bool) -> dict[str, Any] | None:
    intent_types = (
        {OrderIntentType.BUY_TO_OPEN.value, OrderIntentType.SELL_TO_OPEN.value}
        if entry
        else {OrderIntentType.SELL_TO_CLOSE.value, OrderIntentType.BUY_TO_CLOSE.value}
    )
    matches = [dict(row) for row in rows if str(row.get("intent_type") or "") in intent_types]
    if not matches:
        return None
    matches.sort(key=_intent_row_sort_key)
    return matches[0]


def _latest_fill_row_for_intent(fill_rows: list[dict[str, Any]], order_intent_id: str | None) -> dict[str, Any] | None:
    if not order_intent_id:
        return None
    matches = [dict(row) for row in fill_rows if str(row.get("order_intent_id") or "") == str(order_intent_id)]
    if not matches:
        return None
    matches.sort(key=lambda row: str(row.get("fill_timestamp") or ""))
    return matches[-1]


def _build_live_strategy_cycle_leg(
    *,
    existing: dict[str, Any] | None,
    row: dict[str, Any] | None,
    fill_row: dict[str, Any] | None,
    latest_live_intent: dict[str, Any] | None,
    latest_exit_decision: dict[str, Any] | None,
    entry: bool,
) -> dict[str, Any]:
    leg = dict(existing or {})
    row = dict(row or {})
    fill_row = dict(fill_row or {})
    latest_live_intent = dict(latest_live_intent or {})
    latest_exit_decision = dict(latest_exit_decision or {})
    if row:
        leg.setdefault("order_intent_id", row.get("order_intent_id"))
        leg.setdefault("intent_type", row.get("intent_type"))
        leg.setdefault("evaluated_bar_id", row.get("bar_id"))
        leg.setdefault("intent_created_at", row.get("created_at"))
        leg.setdefault("submit_attempted_at", row.get("submitted_at"))
        leg.setdefault("broker_ack_at", row.get("acknowledged_at"))
        leg.setdefault("broker_order_id", row.get("broker_order_id"))
        leg.setdefault("qty", row.get("quantity"))
        if entry:
            if not leg.get("signal_family"):
                leg["signal_family"] = (
                    latest_live_intent.get("long_entry_family")
                    or latest_live_intent.get("short_entry_family")
                    or row.get("reason_code")
                )
            leg.setdefault("reason", row.get("reason_code"))
        else:
            leg.setdefault("primary_reason", latest_exit_decision.get("primary_reason") or row.get("reason_code"))
            all_true_reasons = latest_exit_decision.get("all_true_reasons")
            if all_true_reasons is not None and not leg.get("all_true_reasons"):
                leg["all_true_reasons"] = list(all_true_reasons)
    if fill_row:
        leg["broker_fill_at"] = fill_row.get("fill_timestamp")
        leg["fill_price"] = fill_row.get("fill_price")
        leg["fill_qty"] = fill_row.get("fill_quantity") or leg.get("qty")
        leg.setdefault("broker_order_id", fill_row.get("broker_order_id"))
    elif row and not entry and latest_exit_decision.get("exit_fill_confirmed"):
        leg.setdefault("broker_fill_at", latest_exit_decision.get("fill_timestamp"))
    return leg


def _build_live_strategy_pilot_cycle_summary(
    *,
    settings: StrategySettings,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    latest_reconciliation: dict[str, Any] | None,
    latest_watchdog: dict[str, Any] | None,
    latest_restore: dict[str, Any] | None,
    observed_at: datetime,
) -> dict[str, Any]:
    previous = _load_live_strategy_pilot_cycle_state(settings)
    order_rows = [dict(row) for row in repositories.order_intents.list_all()]
    fill_rows = [dict(row) for row in repositories.fills.list_all()]
    latest_live_intent = strategy_engine.latest_live_intent_summary()
    latest_exit_decision = strategy_engine.latest_exit_decision_summary()
    entry_row = _first_live_intent_row(order_rows, entry=True)
    exit_row = _first_live_intent_row(order_rows, entry=False)
    entry_fill = _latest_fill_row_for_intent(fill_rows, entry_row.get("order_intent_id") if entry_row else None)
    exit_fill = _latest_fill_row_for_intent(fill_rows, exit_row.get("order_intent_id") if exit_row else None)
    latest_processed_end_ts = repositories.processed_bars.latest_end_ts()
    flat_confirmed = (
        strategy_engine.state.position_side is PositionSide.FLAT
        and strategy_engine.state.internal_position_qty == 0
        and strategy_engine.state.broker_position_qty == 0
        and strategy_engine.state.open_broker_order_id is None
        and not execution_engine.pending_executions()
    )
    reconciliation_status = str((latest_reconciliation or {}).get("status") or "").strip().lower() or None
    if reconciliation_status is None and strategy_engine.state.strategy_status is not StrategyStatus.RECONCILING and strategy_engine.state.fault_code is None:
        reconciliation_status = "clear"
    passive_refresh = None
    restore_result = str((latest_restore or {}).get("restore_result") or "").strip().lower() or None
    if restore_result in {"clean", "safe_repair"}:
        passive_refresh = True
    elif restore_result in {"fault", "reconciling"}:
        passive_refresh = False

    entry_leg = _build_live_strategy_cycle_leg(
        existing=dict(previous.get("entry") or {}),
        row=entry_row,
        fill_row=entry_fill,
        latest_live_intent=latest_live_intent,
        latest_exit_decision=latest_exit_decision,
        entry=True,
    )
    exit_leg = _build_live_strategy_cycle_leg(
        existing=dict(previous.get("exit") or {}),
        row=exit_row,
        fill_row=exit_fill,
        latest_live_intent=latest_live_intent,
        latest_exit_decision=latest_exit_decision,
        entry=False,
    )

    entry_submit_used = bool(entry_leg.get("submit_attempted_at"))
    exit_submit_used = bool(exit_leg.get("submit_attempted_at"))
    remaining_allowed_live_submits = max(0, 2 - int(entry_submit_used) - int(exit_submit_used))

    final_result = previous.get("final_result")
    if strategy_engine.state.strategy_status is StrategyStatus.FAULT or strategy_engine.state.fault_code is not None:
        final_result = "faulted"
    elif strategy_engine.state.strategy_status is StrategyStatus.RECONCILING:
        final_result = "reconciled"
    elif exit_leg.get("broker_fill_at") and flat_confirmed and reconciliation_status == "clear":
        final_result = "completed"
    elif (
        entry_row
        and str(entry_row.get("order_status") or "").upper() in LIVE_STRATEGY_TERMINAL_NON_FILL_STATUSES
        and flat_confirmed
    ) or (
        exit_row
        and str(exit_row.get("order_status") or "").upper() in LIVE_STRATEGY_TERMINAL_NON_FILL_STATUSES
        and flat_confirmed
    ):
        final_result = "aborted"

    if final_result == "completed":
        cycle_status = "completed"
    elif final_result in {"reconciled", "aborted"}:
        cycle_status = "reconciled"
    elif final_result == "faulted":
        cycle_status = "faulted"
    elif exit_submit_used:
        cycle_status = "exit_pending"
    elif entry_leg.get("broker_fill_at"):
        cycle_status = "in_position"
    elif entry_submit_used:
        cycle_status = "entry_pending"
    else:
        cycle_status = "waiting_for_entry"

    blocker = None
    reconcile_fault_reason = None
    if final_result == "completed":
        blocker = "live_strategy_pilot_cycle_complete_rearm_required"
    elif final_result == "reconciled":
        blocker = "live_strategy_pilot_reconcile_review_required"
        last_escalation = dict((latest_watchdog or {}).get("last_escalation") or {})
        reconcile_fault_reason = (
            str(last_escalation.get("trigger") or "")
            or str((latest_reconciliation or {}).get("classification") or "")
            or "reconciling"
        )
    elif final_result == "faulted":
        blocker = "live_strategy_pilot_fault_review_required"
        reconcile_fault_reason = strategy_engine.state.fault_code or "faulted"
    elif final_result == "aborted":
        blocker = "live_strategy_pilot_cycle_aborted_rearm_required"

    armed = bool(previous.get("pilot_armed", settings.live_strategy_pilot_enabled and settings.live_strategy_pilot_submit_enabled))
    if final_result in LIVE_STRATEGY_PILOT_CYCLE_TERMINAL_RESULTS:
        armed = False
    pilot_disarmed_at = previous.get("pilot_disarmed_at")
    if armed is False and pilot_disarmed_at is None:
        pilot_disarmed_at = observed_at.isoformat()

    flat_restore_confirmation_time = previous.get("flat_restore_confirmation_time")
    if flat_confirmed and exit_leg.get("broker_fill_at") and reconciliation_status == "clear" and flat_restore_confirmation_time is None:
        flat_restore_confirmation_time = observed_at.isoformat()

    submit_enabled = bool(
        settings.live_strategy_pilot_enabled
        and settings.live_strategy_pilot_submit_enabled
        and armed
        and final_result not in LIVE_STRATEGY_PILOT_CYCLE_TERMINAL_RESULTS
    )

    return {
        "generated_at": observed_at.isoformat(),
        "pilot_armed_at": previous.get("pilot_armed_at") or observed_at.isoformat(),
        "pilot_disarmed_at": pilot_disarmed_at,
        "pilot_armed": armed,
        "rearm_required": not armed,
        "submit_enabled": submit_enabled,
        "cycle_status": cycle_status,
        "remaining_allowed_live_submits": 0 if not armed else remaining_allowed_live_submits,
        "evaluated_entry_bar_id": entry_leg.get("evaluated_bar_id"),
        "evaluated_exit_bar_id": exit_leg.get("evaluated_bar_id"),
        "entry": entry_leg,
        "exit": exit_leg,
        "flat_restore_confirmation_time": flat_restore_confirmation_time,
        "final_reconcile_status": reconciliation_status,
        "passive_refresh_restart_remained_passive": passive_refresh,
        "final_result": final_result,
        "blocker": blocker,
        "reconcile_fault_reason": reconcile_fault_reason,
        "auto_stop_reason": (
            f"pilot_{final_result}"
            if final_result in LIVE_STRATEGY_PILOT_CYCLE_TERMINAL_RESULTS
            else None
        ),
        "rearm_action": LIVE_STRATEGY_PILOT_REARM_ACTION,
        "latest_processed_bar_end_ts": latest_processed_end_ts.isoformat() if latest_processed_end_ts is not None else None,
    }


def _build_live_strategy_pilot_summary(
    *,
    settings: StrategySettings,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    broker_truth_snapshot: dict[str, Any],
    latest_reconciliation: dict[str, Any] | None = None,
    latest_watchdog: dict[str, Any] | None = None,
    latest_restore: dict[str, Any] | None = None,
    latest_fill_sync: dict[str, Any] | None = None,
    signal_observability: dict[str, Any] | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    observed = observed_at or datetime.now(timezone.utc)
    pilot_cycle = _build_live_strategy_pilot_cycle_summary(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=latest_reconciliation,
        latest_watchdog=latest_watchdog,
        latest_restore=latest_restore,
        observed_at=observed,
    )
    timing_summary = _build_live_timing_summary(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=latest_reconciliation,
        latest_watchdog=latest_watchdog,
        latest_restore=latest_restore,
        observed_at=observed,
    )
    gate_status = _live_strategy_pilot_gate_status(
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        broker_truth_snapshot=broker_truth_snapshot,
        latest_reconciliation=latest_reconciliation,
        latest_watchdog=latest_watchdog,
        pilot_cycle_state=pilot_cycle,
    )
    latest_processed_end_ts = repositories.processed_bars.latest_end_ts()
    latest_live_intent = strategy_engine.latest_live_intent_summary() or dict(timing_summary.get("latest_order_intent") or {})
    latest_exit_decision = strategy_engine.latest_exit_decision_summary()
    latest_bar = _latest_runtime_bar(strategy_engine)
    runtime_phase = str(timing_summary.get("runtime_phase") or _paper_soak_runtime_phase(strategy_engine)).upper()
    return {
        "generated_at": observed.isoformat(),
        "operator_path": LIVE_STRATEGY_PILOT_OPERATOR_PATH,
        "allowed_scope": {
            "symbol": settings.symbol,
            "timeframe": settings.timeframe,
            "mode": "LIVE_STRATEGY_PILOT",
            "one_position_at_a_time": True,
            "completed_bar_only": True,
            "deterministic_sequential_processing": True,
            "order_types": ["LIMIT"],
            "max_quantity": int(settings.live_strategy_pilot_max_quantity),
            "regular_hours_only": bool(settings.live_strategy_pilot_regular_hours_only),
        },
        "live_strategy_pilot_enabled": settings.live_strategy_pilot_enabled,
        "live_strategy_submit_enabled": settings.live_strategy_pilot_submit_enabled,
        "live_strategy_single_cycle_mode": bool(settings.live_strategy_pilot_single_cycle_mode),
        "pilot_armed": pilot_cycle.get("pilot_armed"),
        "pilot_rearm_required": pilot_cycle.get("rearm_required"),
        "submit_currently_enabled": bool(gate_status.get("submit_eligible") and pilot_cycle.get("submit_enabled", True)),
        "cycle_status": pilot_cycle.get("cycle_status"),
        "remaining_allowed_live_submits": pilot_cycle.get("remaining_allowed_live_submits"),
        "current_runtime_phase": runtime_phase,
        "strategy_state": strategy_engine.state.strategy_status.value,
        "current_strategy_readiness": bool(gate_status.get("submit_eligible") and pilot_cycle.get("submit_enabled", True)),
        "latest_evaluated_bar": {
            "bar_id": timing_summary.get("evaluated_bar_id"),
            "bar_end_ts": timing_summary.get("evaluated_bar_end_ts"),
            "session_classification": label_session_phase(latest_bar.end_ts) if latest_bar is not None else None,
        },
        "latest_signal_decision": _shadow_signal_summary(strategy_engine),
        "latest_exit_decision": latest_exit_decision,
        "latest_live_strategy_intent": latest_live_intent,
        "submit_attempted_at": latest_live_intent.get("submit_attempted_at") or timing_summary.get("submit_attempted_at"),
        "broker_ack_at": latest_live_intent.get("broker_ack_at") or timing_summary.get("broker_ack_at"),
        "broker_fill_at": latest_live_intent.get("broker_fill_at") or timing_summary.get("broker_fill_at"),
        "broker_order_id": latest_live_intent.get("broker_order_id") or _nested_get(timing_summary, "latest_order_intent", "broker_order_id"),
        "pending_stage": timing_summary.get("pending_stage"),
        "pending_reason": timing_summary.get("pending_reason"),
        "reconcile_trigger_source": timing_summary.get("reconcile_trigger_source"),
        "entries_disabled_blocker": gate_status.get("blocker") or timing_summary.get("entries_disabled_blocker"),
        "submit_gate": gate_status,
        "pilot_cycle": pilot_cycle,
        "broker_truth_summary": gate_status.get("broker_truth_summary"),
        "position_state": dict(timing_summary.get("position_state") or {}),
        "latest_restore_result": timing_summary.get("latest_restore_result"),
        "latest_fill_sync": dict(latest_fill_sync or {}),
        "signal_observability": dict(signal_observability or {}),
        "fault_code": strategy_engine.state.fault_code,
        "summary_line": (
            f"pilot={'ENABLED' if settings.live_strategy_pilot_enabled and settings.live_strategy_pilot_submit_enabled else 'DISABLED'} | "
            f"armed={'YES' if pilot_cycle.get('pilot_armed') else 'NO'} | "
            f"phase={runtime_phase} | "
            f"submit={'ELIGIBLE' if gate_status.get('submit_eligible') and pilot_cycle.get('submit_enabled', True) else 'BLOCKED'} | "
            f"bar={timing_summary.get('evaluated_bar_id') or 'NONE'} | "
            f"blocker={(pilot_cycle.get('blocker') or gate_status.get('blocker') or timing_summary.get('entries_disabled_blocker') or 'none')}"
        ),
    }


class ProbationaryPaperLaneRuntime:
    """A single symbol/source paper lane supervised by the root paper runner."""

    def __init__(
        self,
        *,
        spec: ProbationaryPaperLaneSpec,
        settings: StrategySettings,
        repositories: RepositorySet,
        strategy_engine: StrategyEngine,
        execution_engine: ExecutionEngine,
        live_polling_service: LivePollingService,
        structured_logger: ProbationaryLaneStructuredLogger,
        alert_dispatcher: AlertDispatcher,
    ) -> None:
        self.spec = spec
        self.settings = settings
        self.repositories = repositories
        self.strategy_engine = strategy_engine
        self.execution_engine = execution_engine
        self.live_polling_service = live_polling_service
        self.structured_logger = structured_logger
        self.alert_dispatcher = alert_dispatcher
        self.started = False
        self._last_reconciliation_payload: dict[str, Any] | None = None
        self._heartbeat_reconciliation = _initial_reconciliation_heartbeat_status(
            self.settings.reconciliation_heartbeat_interval_seconds
        )
        self._order_timeout_watchdog = _initial_order_timeout_watchdog_status(self.settings)
        self._startup_restore_validation: dict[str, Any] = {}

    def _write_exit_parity_summary(self, observed_at: datetime) -> Path:
        payload = _build_exit_parity_summary(
            repositories=self.repositories,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            latest_restore=self._startup_restore_validation,
            observed_at=observed_at,
        )
        self.structured_logger.log_exit_parity_event(payload)
        return self.structured_logger.write_exit_parity_state(payload)

    def _write_live_timing_summary(self, observed_at: datetime) -> Path:
        payload = _build_live_timing_summary(
            settings=self.settings,
            repositories=self.repositories,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            latest_reconciliation=self._last_reconciliation_payload,
            latest_watchdog=self._order_timeout_watchdog,
            latest_restore=self._startup_restore_validation,
            observed_at=observed_at,
        )
        self.structured_logger.log_live_timing_event(payload)
        return self.structured_logger.write_live_timing_state(payload)

    def restore_startup(self) -> str | None:
        restore_started_at = datetime.now(timezone.utc)
        pre_restore_state = _restore_validation_state_snapshot(
            repositories=self.repositories,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
        )
        pre_restore_counts = _restore_validation_record_counts(self.repositories)
        restore_adjustments: list[str] = []
        _restore_paper_runtime_state(
            repositories=self.repositories,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
        )
        if (
            self.spec.lane_mode == PAPER_EXECUTION_CANARY_MODE
            and self.strategy_engine.state.operator_halt
            and self.strategy_engine.state.position_side == PositionSide.FLAT
            and self.strategy_engine.state.internal_position_qty == 0
            and self.strategy_engine.state.broker_position_qty == 0
            and self.strategy_engine.state.open_broker_order_id is None
            and self.strategy_engine.state.fault_code is None
        ):
            self.strategy_engine.set_operator_halt(datetime.now(timezone.utc), False)
            restore_adjustments.append("clear_stale_operator_halt_flat_canary")
        reconciliation = _reconcile_paper_runtime(
            repositories=self.repositories,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            trigger="startup",
            apply_repairs=True,
        )
        self._last_reconciliation_payload = dict(reconciliation)
        self._startup_restore_validation = _record_restore_validation(
            repositories=self.repositories,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            structured_logger=self.structured_logger,
            alert_dispatcher=self.alert_dispatcher,
            restore_started_at=restore_started_at,
            reconciliation=reconciliation,
            scope_label=f"Paper lane {self.spec.display_name}",
            runtime_name="probationary_paper_lane",
            lane_id=self.spec.lane_id,
            instrument=self.spec.symbol,
            restore_adjustments=restore_adjustments,
            before_state_summary=pre_restore_state,
            before_counts=pre_restore_counts,
        )
        self._write_exit_parity_summary(datetime.now(timezone.utc))
        self._write_live_timing_summary(datetime.now(timezone.utc))
        self.started = True
        if reconciliation["clean"] or reconciliation.get("classification") == "safe_repair":
            return None
        self.alert_dispatcher.emit(
            severity="BLOCKING",
            code="paper_startup_reconciliation_failed",
            message=f"Paper lane {self.spec.lane_id} startup reconciliation failed; refusing to run.",
            payload={**reconciliation, "lane_id": self.spec.lane_id, "instrument": self.spec.symbol},
            category="state_restore_failure",
            title="State Restore Failure",
            dedup_key=f"{self.spec.lane_id}:startup_reconciliation_failed",
            recommended_action="Inspect broker/internal state before restarting this lane.",
            active=True,
        )
        return "paper_startup_reconciliation_failed"

    def supervisor_status_extras(self) -> dict[str, Any]:
        return {"startup_restore_validation": dict(self._startup_restore_validation or {})}

    def poll_and_process(self) -> tuple[int, dict[str, Any], Path]:
        latest_processed_end_ts = self.repositories.processed_bars.latest_end_ts()
        bars = self.live_polling_service.poll_bars(
            SchwabLivePollRequest(
                internal_symbol=self.settings.symbol,
                since=latest_processed_end_ts,
            ),
            internal_timeframe=self.settings.timeframe,
            default_is_final=True,
        )
        for bar in bars:
            self.strategy_engine.process_bar(bar)
            self._apply_canary_lifecycle(bar)
        heartbeat_reconciliation, reconciliation, _ = _run_reconciliation_heartbeat(
            settings=self.settings,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            heartbeat_status=self._heartbeat_reconciliation,
        )
        order_timeout_watchdog, _, _ = _run_order_timeout_watchdog(
            settings=self.settings,
            repositories=self.repositories,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            structured_logger=self.structured_logger,
            alert_dispatcher=self.alert_dispatcher,
            watchdog_status=self._order_timeout_watchdog,
        )
        self._heartbeat_reconciliation = heartbeat_reconciliation
        self._order_timeout_watchdog = order_timeout_watchdog
        if reconciliation is not None:
            reconciliation["lane_id"] = self.spec.lane_id
            self._last_reconciliation_payload = dict(reconciliation)
        effective_reconciliation = dict(reconciliation or self._last_reconciliation_payload or {})
        effective_reconciliation.setdefault("lane_id", self.spec.lane_id)
        status_path = self.structured_logger.write_operator_status(
            {
                "updated_at": datetime.now(timezone.utc).isoformat(),
                "display_name": self.spec.display_name,
                "session_restriction": self.spec.session_restriction,
                "lane_mode": self.spec.lane_mode,
                "approved_long_entry_sources": sorted(self.settings.approved_long_entry_sources),
                "approved_short_entry_sources": sorted(self.settings.approved_short_entry_sources),
                "last_processed_bar_end_ts": (
                    self.repositories.processed_bars.latest_end_ts().isoformat()
                    if self.repositories.processed_bars.latest_end_ts() is not None
                    else None
                ),
                "processed_bars": self.repositories.processed_bars.count(),
                "new_bars_last_cycle": len(bars),
                "position_side": self.strategy_engine.state.position_side.value,
                "strategy_status": self.strategy_engine.state.strategy_status.value,
                "fault_code": self.strategy_engine.state.fault_code,
                "entries_enabled": self.strategy_engine.state.entries_enabled,
                "operator_halt": self.strategy_engine.state.operator_halt,
                "same_underlying_entry_hold": self.strategy_engine.state.same_underlying_entry_hold,
                "same_underlying_hold_reason": self.strategy_engine.state.same_underlying_hold_reason,
                "canary_force_fire_once_active": (
                    self._force_fire_canary_enabled() if self.spec.lane_mode == PAPER_EXECUTION_CANARY_MODE else False
                ),
                "canary_force_fire_once_consumed": (
                    (
                        self._force_fire_canary_entry_count() > 0
                        and self._force_fire_canary_entry_count() <= self._force_fire_canary_exit_count()
                    )
                    if self.spec.lane_mode == PAPER_EXECUTION_CANARY_MODE
                    else False
                ),
                "open_paper_order_ids": (
                    effective_reconciliation.get("broker_open_order_ids", [])
                ),
                "reconciliation": effective_reconciliation,
                "heartbeat_reconciliation": self._heartbeat_reconciliation,
                "order_timeout_watchdog": self._order_timeout_watchdog,
                "startup_restore_validation": self._startup_restore_validation,
                "exit_parity_summary": _build_exit_parity_summary(
                    repositories=self.repositories,
                    strategy_engine=self.strategy_engine,
                    execution_engine=self.execution_engine,
                    latest_restore=self._startup_restore_validation,
                    observed_at=datetime.now(timezone.utc),
                ),
                "live_timing_summary": _build_live_timing_summary(
                    settings=self.settings,
                    repositories=self.repositories,
                    strategy_engine=self.strategy_engine,
                    execution_engine=self.execution_engine,
                    latest_reconciliation=effective_reconciliation,
                    latest_watchdog=self._order_timeout_watchdog,
                    latest_restore=self._startup_restore_validation,
                    observed_at=datetime.now(timezone.utc),
                ),
            }
        )
        self._write_exit_parity_summary(datetime.now(timezone.utc))
        self._write_live_timing_summary(datetime.now(timezone.utc))
        return len(bars), effective_reconciliation, status_path

    def _apply_canary_lifecycle(self, bar: Bar) -> None:
        if self.spec.lane_mode != PAPER_EXECUTION_CANARY_MODE:
            return
        session_date = bar.end_ts.astimezone(self.settings.timezone_info).date()
        if self._should_submit_force_fire_canary_entry():
            self.strategy_engine.submit_paper_canary_entry_intent(
                bar,
                signal_source=PAPER_EXECUTION_CANARY_FORCE_SIGNAL_SOURCE,
                reason_code=self._force_fire_canary_entry_reason_code(),
            )
            return
        if self._should_submit_force_fire_canary_exit(bar):
            self.strategy_engine.submit_operator_flatten_intent(
                bar.end_ts,
                reason_code=self._force_fire_canary_exit_reason_code(),
            )
            return
        if self._force_fire_canary_enabled():
            return
        if self._should_submit_canary_entry(bar, session_date):
            self.strategy_engine.submit_paper_canary_entry_intent(
                bar,
                signal_source=PAPER_EXECUTION_CANARY_SIGNAL_SOURCE,
                reason_code=PAPER_EXECUTION_CANARY_ENTRY_REASON,
            )
            return
        if self._should_submit_canary_exit(bar, session_date):
            self.strategy_engine.submit_operator_flatten_intent(
                bar.end_ts,
                reason_code=PAPER_EXECUTION_CANARY_EXIT_REASON,
            )

    def _should_submit_canary_entry(self, bar: Bar, session_date: date) -> bool:
        if self._canary_entry_count(session_date) >= max(1, self.spec.canary_max_entries_per_session):
            return False
        state = self.strategy_engine.state
        if state.position_side != PositionSide.FLAT or state.internal_position_qty > 0:
            return False
        if state.open_broker_order_id is not None:
            return False
        if not state.entries_enabled or state.operator_halt or state.fault_code is not None:
            return False
        local_time = bar.end_ts.astimezone(self.settings.timezone_info).time()
        return _time_in_closed_open_window(
            local_time,
            _parse_time_or_none(self.spec.canary_entry_not_before_et),
            _parse_time_or_none(self.spec.canary_entry_window_end_et),
        )

    def _should_submit_canary_exit(self, bar: Bar, session_date: date) -> bool:
        if self._canary_entry_count(session_date) <= 0:
            return False
        if self._canary_exit_count(session_date) >= self._canary_entry_count(session_date):
            return False
        state = self.strategy_engine.state
        if state.position_side != PositionSide.LONG or state.internal_position_qty <= 0:
            return False
        if state.open_broker_order_id is not None:
            return False
        if state.entry_timestamp is None:
            return False
        local_time = bar.end_ts.astimezone(self.settings.timezone_info).time()
        if self.spec.canary_exit_not_before_et:
            exit_not_before = _parse_time_or_none(self.spec.canary_exit_not_before_et)
            if exit_not_before is not None and local_time < exit_not_before:
                return False
            return True
        required_end = state.entry_timestamp + timedelta(minutes=timeframe_minutes(self.settings.timeframe))
        return bar.end_ts >= required_end

    def _should_submit_force_fire_canary_entry(self) -> bool:
        if not self._force_fire_canary_enabled():
            return False
        if self._force_fire_canary_entry_count() > 0:
            return False
        state = self.strategy_engine.state
        if state.position_side != PositionSide.FLAT or state.internal_position_qty > 0:
            return False
        if state.open_broker_order_id is not None:
            return False
        if not state.entries_enabled or state.operator_halt or state.fault_code is not None:
            return False
        return True

    def _should_submit_force_fire_canary_exit(self, bar: Bar) -> bool:
        if not self._force_fire_canary_enabled():
            return False
        if self._force_fire_canary_entry_count() <= self._force_fire_canary_exit_count():
            return False
        state = self.strategy_engine.state
        if state.position_side != PositionSide.LONG or state.internal_position_qty <= 0:
            return False
        if state.open_broker_order_id is not None:
            return False
        if state.entry_timestamp is None:
            return False
        required_end = state.entry_timestamp + timedelta(minutes=timeframe_minutes(self.settings.timeframe))
        return bar.end_ts >= required_end

    def _force_fire_canary_enabled(self) -> bool:
        return bool(self._force_fire_canary_token())

    def _force_fire_canary_token(self) -> str:
        return _normalize_canary_force_fire_token(
            self.settings.probationary_paper_execution_canary_force_fire_once_token
        )

    def _force_fire_canary_entry_reason_code(self) -> str:
        token = self._force_fire_canary_token()
        if not token:
            return PAPER_EXECUTION_CANARY_FORCE_ENTRY_REASON
        return f"{PAPER_EXECUTION_CANARY_FORCE_ENTRY_REASON}:{token}"

    def _force_fire_canary_exit_reason_code(self) -> str:
        token = self._force_fire_canary_token()
        if not token:
            return PAPER_EXECUTION_CANARY_FORCE_EXIT_REASON
        return f"{PAPER_EXECUTION_CANARY_FORCE_EXIT_REASON}:{token}"

    def _force_fire_canary_entry_count(self) -> int:
        return _order_reason_count(
            self.repositories,
            reason_code=self._force_fire_canary_entry_reason_code(),
        )

    def _force_fire_canary_exit_count(self) -> int:
        return _order_reason_count(
            self.repositories,
            reason_code=self._force_fire_canary_exit_reason_code(),
        )

    def _canary_entry_already_recorded(self, session_date: date) -> bool:
        return self._canary_entry_count(session_date) > 0

    def _canary_entry_count(self, session_date: date) -> int:
        return _session_order_reason_count(
            self.repositories,
            session_date=session_date,
            timezone_info=self.settings.timezone_info,
            reason_code=PAPER_EXECUTION_CANARY_ENTRY_REASON,
        )

    def _canary_exit_already_recorded(self, session_date: date) -> bool:
        return self._canary_exit_count(session_date) > 0

    def _canary_exit_count(self, session_date: date) -> int:
        return _session_order_reason_count(
            self.repositories,
            session_date=session_date,
            timezone_info=self.settings.timezone_info,
            reason_code=PAPER_EXECUTION_CANARY_EXIT_REASON,
        )


class ProbationaryTemporaryPaperLaneRuntime(ProbationaryPaperLaneRuntime):
    """Experimental temporary paper strategy surfaced inline with regular paper strategies."""

    def config_row_extras(self) -> dict[str, Any]:
        return {
            "experimental_status": self.spec.experimental_status,
            "paper_only": self.spec.paper_only,
            "non_approved": self.spec.non_approved,
            "quality_bucket_policy": self.spec.quality_bucket_policy,
            "observer_side": self.spec.observer_side,
            "observer_variant_id": self.spec.observer_variant_id,
            "scope_label": "Temporary Paper Strategy / Experimental / Non-Approved",
            "execution_authority": False,
        }

    def supervisor_status_extras(self) -> dict[str, Any]:
        return {
            "experimental_status": self.spec.experimental_status,
            "paper_only": self.spec.paper_only,
            "non_approved": self.spec.non_approved,
            "quality_bucket_policy": self.spec.quality_bucket_policy,
            "observer_side": self.spec.observer_side,
            "observer_variant_id": self.spec.observer_variant_id,
            "temporary_paper_strategy": True,
            "execution_authority": False,
            "operator_status_line": (
                f"{self.spec.display_name} | TEMP PAPER | signals-only experimental observer | "
                f"processed_bars={self.repositories.processed_bars.count()}"
            ),
        }

    def eligibility_snapshot(self, now: datetime) -> dict[str, Any]:
        last_processed_end = self.repositories.processed_bars.latest_end_ts()
        current_session = label_session_phase(now)
        latest_completed_bar_end = _latest_completed_probationary_bar_end(now, self.settings.timeframe)
        warmup_required = self.settings.warmup_bars_required()
        warmup_bars_loaded = len(self.strategy_engine._bar_history)  # noqa: SLF001
        warmup_complete = warmup_bars_loaded >= warmup_required
        state = self.strategy_engine.state
        session_allowed = current_session == "LONDON_OPEN"
        within_branch_window = gc_mgc_london_open_acceptance_window_matches(
            end_ts=latest_completed_bar_end,
            timezone_info=self.settings.timezone_info,
        )
        eligible_now = True
        blocker_reason = None
        blocker_detail = None

        if state.fault_code is not None:
            eligible_now = False
            blocker_reason = "fault"
            blocker_detail = state.fault_code
        elif state.operator_halt:
            eligible_now = False
            blocker_reason = "operator_halt"
        elif not state.entries_enabled:
            eligible_now = False
            blocker_reason = "entries_disabled"
        elif not session_allowed:
            eligible_now = False
            blocker_reason = "wrong_session"
        elif not within_branch_window:
            eligible_now = False
            blocker_reason = "outside_branch_window"
            blocker_detail = ",".join(value.strftime("%H:%M") for value in GC_MGC_LONDON_OPEN_ACCEPTANCE_FIRST_THREE_BARS)
        elif not warmup_complete:
            eligible_now = False
            blocker_reason = "warmup_incomplete"
            blocker_detail = f"{warmup_bars_loaded}/{warmup_required}"
        elif last_processed_end is None or last_processed_end < latest_completed_bar_end:
            eligible_now = False
            blocker_reason = "no_new_completed_bar"

        return {
            "current_detected_session": current_session,
            "eligible_now": eligible_now,
            "eligibility_reason": blocker_reason,
            "eligibility_detail": blocker_detail,
            "allowed_session_match": session_allowed and within_branch_window,
            "warmup_complete": warmup_complete,
            "warmup_bars_loaded": warmup_bars_loaded,
            "warmup_bars_required": warmup_required,
            "latest_completed_bar_end_ts": latest_completed_bar_end.isoformat(),
            "last_processed_bar_end_ts": last_processed_end.isoformat() if last_processed_end is not None else None,
        }


class ProbationaryAtpeCanaryLaneRuntime(ProbationaryPaperLaneRuntime):
    """Paper-only ATPE temp-paper lane that evaluates live and submits paper intents/fills."""

    def __init__(
        self,
        *,
        observed_instruments: Sequence[str],
        variant: PatternVariant,
        live_polling_services_by_instrument: dict[str, LivePollingService],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._observed_instruments = tuple(str(value).strip().upper() for value in observed_instruments if str(value).strip())
        if len(self._observed_instruments) != 1:
            raise ValueError(
                f"ATPE paper-executable lanes require exactly one observed instrument, got {self._observed_instruments!r}"
            )
        self._instrument = self._observed_instruments[0]
        self._variant = variant
        self._canary_root = Path(self.settings.probationary_artifacts_path).parent.parent
        self._services_by_instrument = dict(live_polling_services_by_instrument)
        self._bars_1m: dict[str, list[ResearchBar]] = {self._instrument: []}
        self._latest_polled_end_ts: dict[str, datetime | None] = {self._instrument: None}
        self._signal_rows: list[dict[str, Any]] = _read_jsonl(self._lane_file("signals.jsonl"))
        self._event_rows: list[dict[str, Any]] = _read_jsonl(self._lane_file("events.jsonl"))
        self._seen_decision_ids = {
            str(row.get("decision_id") or "").strip()
            for row in self._signal_rows
            if str(row.get("decision_id") or "").strip()
        }
        self._last_supervisor_status: dict[str, Any] = {}
        self._kill_switch_active = False
        self._pending_candidates: list[dict[str, Any]] = []
        self._pending_entry_plan: dict[str, Any] | None = None
        self._active_trade_plan: dict[str, Any] | None = None
        self._restore_bars_from_artifacts()
        self._restore_runtime_state()

    def restore_startup(self) -> str | None:
        now = datetime.now(timezone.utc)
        self._kill_switch_active = self._kill_switch_is_active()
        startup_reason = super().restore_startup()
        self.strategy_engine.set_operator_halt(now, self._kill_switch_active)
        if startup_reason is not None:
            return startup_reason
        self._sync_execution_artifacts()
        self._write_runtime_state()
        self._ensure_runtime_snapshot(now)
        self._write_lane_operator_status(now)
        return None

    def poll_and_process(
        self,
        higher_priority_signals: Sequence[HigherPrioritySignal] = (),
    ) -> tuple[int, dict[str, Any], Path]:
        now = datetime.now(timezone.utc)
        self._kill_switch_active = self._kill_switch_is_active()
        self.strategy_engine.set_operator_halt(now, self._kill_switch_active)

        instrument = self._instrument
        latest_end = self._latest_polled_end_ts.get(instrument)
        bars = self._services_by_instrument[instrument].poll_bars(
            SchwabLivePollRequest(
                internal_symbol=instrument,
                since=latest_end,
            ),
            internal_timeframe="1m",
            default_is_final=True,
        )
        fresh_signal_rows: list[dict[str, Any]] = []
        features_by_instrument: dict[str, list[dict[str, Any]]] = {}
        for bar in bars:
            self.repositories.bars.save(bar)
            self.repositories.processed_bars.mark_processed(bar)
            research_bar = _research_bar_from_domain_bar(bar)
            existing = self._bars_1m.setdefault(instrument, [])
            if not any(item.end_ts == research_bar.end_ts for item in existing):
                existing.append(research_bar)
            self._latest_polled_end_ts[instrument] = bar.end_ts
            self._apply_due_runtime_fills(bar)
            self._evaluate_exit_triggers(bar)
            self._evaluate_entry_triggers(bar)
            features_by_instrument, bar_signal_rows = self._recompute_live_decisions(
                higher_priority_signals=higher_priority_signals,
                observed_at=bar.end_ts,
            )
            if bar_signal_rows:
                self._signal_rows.extend(bar_signal_rows)
                fresh_signal_rows.extend(bar_signal_rows)

        self._bars_1m[instrument] = _prune_research_bars(
            self._bars_1m.get(instrument, []),
            keep_minutes=max(int(self.spec.live_poll_lookback_minutes or 1440), 60),
        )
        if bars or fresh_signal_rows:
            self._rewrite_processed_bars()
            self._rewrite_features(features_by_instrument)
        self._rewrite_signals()
        self._rewrite_events()
        self._sync_execution_artifacts()
        self._write_runtime_state()
        self._ensure_runtime_snapshot(now)
        heartbeat_reconciliation, reconciliation, _ = _run_reconciliation_heartbeat(
            settings=self.settings,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            heartbeat_status=self._heartbeat_reconciliation,
        )
        order_timeout_watchdog, _, _ = _run_order_timeout_watchdog(
            settings=self.settings,
            repositories=self.repositories,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            structured_logger=self.structured_logger,
            alert_dispatcher=self.alert_dispatcher,
            watchdog_status=self._order_timeout_watchdog,
            occurred_at=now,
        )
        self._heartbeat_reconciliation = heartbeat_reconciliation
        self._order_timeout_watchdog = order_timeout_watchdog
        if reconciliation is not None:
            self._last_reconciliation_payload = dict(reconciliation)
        effective_reconciliation = dict(reconciliation or self._last_reconciliation_payload or {})
        effective_reconciliation.update(
            {
                "lane_id": self.spec.lane_id,
                "runtime_kind": self.spec.runtime_kind,
                "new_bars_last_cycle": len(bars),
                "recent_signal_count": len(self._signal_rows),
                "recent_event_count": len(self._event_rows),
            }
        )
        status_path = self._write_lane_operator_status(now)
        return len(bars), effective_reconciliation, status_path

    def config_row_extras(self) -> dict[str, Any]:
        return {
            "experimental_status": self.spec.experimental_status,
            "paper_only": self.spec.paper_only,
            "non_approved": self.spec.non_approved,
            "quality_bucket_policy": self.spec.quality_bucket_policy,
            "observer_side": self.spec.observer_side,
            "observer_variant_id": self.spec.observer_variant_id,
            "observed_instruments": list(self._observed_instruments),
            "scope_label": "Experimental Canary / Paper Only / Non-Approved",
            "execution_authority": False,
        }

    def supervisor_status_extras(self) -> dict[str, Any]:
        return dict(self._last_supervisor_status)

    def eligibility_snapshot(self, now: datetime) -> dict[str, Any]:
        last_processed_end = self.repositories.processed_bars.latest_end_ts()
        state = self.strategy_engine.state
        eligible_now = (
            not self._kill_switch_active
            and last_processed_end is not None
            and state.fault_code is None
            and not state.operator_halt
            and state.position_side == PositionSide.FLAT
            and state.open_broker_order_id is None
        )
        blocker_reason = None
        if self._kill_switch_active:
            blocker_reason = "kill_switch_active"
        elif last_processed_end is None:
            blocker_reason = "awaiting_live_bars"
        elif state.fault_code is not None:
            blocker_reason = "fault"
        elif state.operator_halt:
            blocker_reason = "operator_halt"
        elif state.position_side != PositionSide.FLAT or state.open_broker_order_id is not None:
            blocker_reason = "strategy_not_ready"
        return {
            "current_detected_session": label_session_phase(now),
            "eligible_now": eligible_now,
            "eligibility_reason": blocker_reason,
            "eligibility_detail": state.fault_code if blocker_reason == "fault" else None,
            "allowed_session_match": True,
            "warmup_complete": last_processed_end is not None,
            "warmup_bars_loaded": len(self._bars_1m.get(self._instrument, [])),
            "warmup_bars_required": 1,
            "latest_completed_bar_end_ts": (
                last_processed_end.isoformat() if last_processed_end is not None else None
            ),
            "last_processed_bar_end_ts": (
                last_processed_end.isoformat() if last_processed_end is not None else None
            ),
        }

    def _lane_file(self, file_name: str) -> Path:
        return Path(self.settings.probationary_artifacts_path) / file_name

    def _kill_switch_is_active(self) -> bool:
        return (self._canary_root / "DISABLE_ACTIVE_TREND_PARTICIPATION_CANARY").exists()

    def _runtime_state_path(self) -> Path:
        return self._lane_file("runtime_state.json")

    def _restore_bars_from_artifacts(self) -> None:
        restored: list[ResearchBar] = []
        for row in _read_jsonl(self._lane_file("processed_bars.jsonl")):
            if str(row.get("symbol") or "").upper() != self._instrument:
                continue
            start_ts = row.get("start_ts")
            end_ts = row.get("end_ts")
            if not start_ts or not end_ts:
                continue
            restored.append(
                ResearchBar(
                    instrument=self._instrument,
                    timeframe=str(row.get("timeframe") or "1m"),
                    start_ts=datetime.fromisoformat(str(start_ts)),
                    end_ts=datetime.fromisoformat(str(end_ts)),
                    open=float(row.get("open") or 0.0),
                    high=float(row.get("high") or 0.0),
                    low=float(row.get("low") or 0.0),
                    close=float(row.get("close") or 0.0),
                    volume=int(row.get("volume") or 0),
                    session_label=str(row.get("session_label") or "UNKNOWN"),
                    session_segment=str(row.get("session_segment") or "UNKNOWN"),
                    source="jsonl_restore",
                    provenance=str(row.get("provenance") or "probationary_paper_runtime"),
                )
            )
        self._bars_1m[self._instrument] = sorted(restored, key=lambda item: item.end_ts)
        if self._bars_1m[self._instrument]:
            self._latest_polled_end_ts[self._instrument] = self._bars_1m[self._instrument][-1].end_ts

    def _restore_runtime_state(self) -> None:
        payload = _read_json(self._runtime_state_path())
        self._pending_candidates = list(payload.get("pending_candidates") or [])
        self._pending_entry_plan = dict(payload.get("pending_entry_plan") or {}) or None
        self._active_trade_plan = dict(payload.get("active_trade_plan") or {}) or None

    def _write_runtime_state(self) -> None:
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "lane_id": self.spec.lane_id,
            "instrument": self._instrument,
            "pending_candidates": self._pending_candidates,
            "pending_entry_plan": self._pending_entry_plan,
            "active_trade_plan": self._active_trade_plan,
            "latest_polled_end_ts": (
                self._latest_polled_end_ts[self._instrument].isoformat()
                if self._latest_polled_end_ts.get(self._instrument) is not None
                else None
            ),
        }
        self._runtime_state_path().write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def _recompute_live_decisions(
        self,
        *,
        higher_priority_signals: Sequence[HigherPrioritySignal],
        observed_at: datetime,
    ) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
        features_by_instrument: dict[str, list[dict[str, Any]]] = {}
        fresh_signal_rows: list[dict[str, Any]] = []
        bars_1m = sorted(self._bars_1m.get(self._instrument, []), key=lambda item: item.end_ts)
        if not bars_1m:
            return features_by_instrument, fresh_signal_rows
        bars_5m = _resample_research_bars_5m(bars_1m)
        if bars_5m:
            first_1m_ts = bars_1m[0].end_ts
            last_1m_ts = bars_1m[-1].end_ts
            bars_5m = [bar for bar in bars_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
        features = build_feature_states(bars_5m=bars_5m, bars_1m=bars_1m)
        features_by_instrument[self._instrument] = [_runtime_feature_row(feature, self.spec) for feature in features]
        decisions = generate_signal_decisions(
            feature_rows=features,
            variants=[self._variant],
            higher_priority_signals=higher_priority_signals,
        )
        state = self.strategy_engine.state
        for decision in decisions:
            if self.spec.quality_bucket_policy == "MEDIUM_HIGH_ONLY" and decision.setup_quality_bucket not in {"MEDIUM", "HIGH"}:
                continue
            if self.spec.quality_bucket_policy == "HIGH_ONLY" and decision.setup_quality_bucket != "HIGH":
                continue
            if decision.decision_id in self._seen_decision_ids:
                continue
            signal_row = _runtime_atpe_signal_row(
                spec=self.spec,
                decision=decision,
                kill_switch_active=self._kill_switch_active,
                observed_instruments=self._observed_instruments,
            )
            fresh_signal_rows.append(signal_row)
            self._seen_decision_ids.add(decision.decision_id)
            self.structured_logger.log_branch_source(signal_row)
            if signal_row["decision"] == "blocked":
                self.structured_logger.log_rule_block(signal_row)
            elif state.position_side == PositionSide.FLAT and state.open_broker_order_id is None and self._pending_entry_plan is None:
                trigger_price = _atpe_entry_trigger_price(decision=decision, variant=self._variant)
                self._pending_candidates.append(
                    {
                        "decision_id": decision.decision_id,
                        "instrument": decision.instrument,
                        "variant_id": decision.variant_id,
                        "side": decision.side,
                        "decision_ts": decision.decision_ts.isoformat(),
                        "expires_at": (
                            decision.decision_ts + timedelta(minutes=self._variant.entry_window_bars_1m)
                        ).isoformat(),
                        "decision_bar_high": decision.decision_bar_high,
                        "decision_bar_low": decision.decision_bar_low,
                        "average_range": decision.average_range,
                        "trigger_price": trigger_price,
                        "setup_signature": decision.setup_signature,
                        "setup_quality_bucket": decision.setup_quality_bucket,
                        "max_hold_bars_1m": self._variant.max_hold_bars_1m,
                        "stop_atr_multiple": self._variant.stop_atr_multiple,
                        "target_r_multiple": self._variant.target_r_multiple,
                        "reason_code": decision.variant_id,
                        "signal_source": decision.variant_id,
                    }
                )
                self._event_rows.append(
                    {
                        "timestamp": observed_at.isoformat(),
                        "lane_id": self.spec.lane_id,
                        "event_type": "ATPE_CANDIDATE_ARMED",
                        "decision_id": decision.decision_id,
                        "symbol": decision.instrument,
                        "trigger_price": trigger_price,
                    }
                )
            self._event_rows.append(
                {
                    "timestamp": observed_at.isoformat(),
                    "lane_id": self.spec.lane_id,
                    "event_type": "ATPE_SIGNAL",
                    "decision_id": decision.decision_id,
                    "decision": signal_row["decision"],
                    "allow_block_reason": signal_row["allow_block_reason"],
                    "override_reason": signal_row["override_reason"],
                    "symbol": decision.instrument,
                }
            )
        return features_by_instrument, fresh_signal_rows

    def _apply_due_runtime_fills(self, bar: Bar) -> None:
        for pending in self.execution_engine.pop_due_replay_fills(bar, self.settings):
            fill = self.execution_engine.materialize_replay_fill(pending, bar)
            self.strategy_engine._persist_order_intent(  # noqa: SLF001
                pending.intent,
                fill.broker_order_id or pending.broker_order_id,
                order_status=OrderStatus.FILLED,
            )
            self.strategy_engine.apply_fill(
                fill_event=fill,
                signal_bar_id=pending.signal_bar_id,
                long_entry_family=pending.long_entry_family,
                short_entry_family=pending.short_entry_family,
                short_entry_source=pending.short_entry_source,
            )
            if pending.intent.is_entry:
                if self._pending_entry_plan is not None and self._pending_entry_plan.get("order_intent_id") == pending.intent.order_intent_id:
                    plan = dict(self._pending_entry_plan)
                    risk = max(float(plan["average_range"]) * float(plan["stop_atr_multiple"]), 0.25)
                    if plan["side"] == "LONG":
                        stop_price = float(plan["decision_bar_low"]) - risk
                        target_price = (
                            float(fill.fill_price) + risk * float(plan["target_r_multiple"])
                            if plan.get("target_r_multiple") is not None
                            else None
                        )
                    else:
                        stop_price = float(plan["decision_bar_high"]) + risk
                        target_price = (
                            float(fill.fill_price) - risk * float(plan["target_r_multiple"])
                            if plan.get("target_r_multiple") is not None
                            else None
                        )
                    self._active_trade_plan = {
                        **plan,
                        "entry_fill_timestamp": fill.fill_timestamp.isoformat(),
                        "entry_fill_price": float(fill.fill_price),
                        "risk_points": risk,
                        "stop_price": stop_price,
                        "initial_stop_price": stop_price,
                        "target_price": target_price,
                        "target_checkpoint_price": target_price,
                        "target_checkpoint_reached": False,
                        "target_checkpoint_reached_at": None,
                        "exit_policy": (
                            ATPE_EXIT_POLICY_TARGET_CHECKPOINT
                            if str(plan.get("side")).upper() == "LONG"
                            else ATPE_EXIT_POLICY_HARD_TARGET
                        ),
                        "max_exit_timestamp": (
                            fill.fill_timestamp + timedelta(minutes=int(plan["max_hold_bars_1m"]))
                        ).isoformat(),
                    }
                    self._pending_entry_plan = None
                self._event_rows.append(
                    {
                        "timestamp": fill.fill_timestamp.isoformat(),
                        "lane_id": self.spec.lane_id,
                        "event_type": "ATPE_ENTRY_FILL",
                        "order_intent_id": fill.order_intent_id,
                        "fill_price": str(fill.fill_price),
                        "symbol": pending.intent.symbol,
                    }
                )
            else:
                self._event_rows.append(
                    {
                        "timestamp": fill.fill_timestamp.isoformat(),
                        "lane_id": self.spec.lane_id,
                        "event_type": "ATPE_EXIT_FILL",
                        "order_intent_id": fill.order_intent_id,
                        "fill_price": str(fill.fill_price),
                        "symbol": pending.intent.symbol,
                    }
                )
                self._active_trade_plan = None
                self._pending_entry_plan = None

    def _evaluate_entry_triggers(self, bar: Bar) -> None:
        if self._pending_entry_plan is not None:
            return
        state = self.strategy_engine.state
        if state.position_side != PositionSide.FLAT or state.open_broker_order_id is not None:
            return
        if state.operator_halt or state.fault_code is not None or not state.entries_enabled:
            return
        remaining_candidates: list[dict[str, Any]] = []
        for candidate in self._pending_candidates:
            decision_ts = datetime.fromisoformat(str(candidate["decision_ts"]))
            expires_at = datetime.fromisoformat(str(candidate["expires_at"]))
            if bar.end_ts <= decision_ts:
                remaining_candidates.append(candidate)
                continue
            if bar.end_ts > expires_at:
                self._event_rows.append(
                    {
                        "timestamp": bar.end_ts.isoformat(),
                        "lane_id": self.spec.lane_id,
                        "event_type": "ATPE_TRIGGER_MISSED",
                        "decision_id": candidate["decision_id"],
                        "symbol": candidate["instrument"],
                    }
                )
                continue
            trigger_price = float(candidate["trigger_price"])
            side = str(candidate["side"]).upper()
            triggered = (side == "LONG" and float(bar.high) >= trigger_price) or (
                side == "SHORT" and float(bar.low) <= trigger_price
            )
            if not triggered:
                remaining_candidates.append(candidate)
                continue
            intent = self.strategy_engine.submit_runtime_entry_intent(
                bar,
                side=side,
                signal_source=str(candidate["signal_source"]),
                reason_code=str(candidate["reason_code"]),
                symbol=self._instrument,
                long_entry_family=LongEntryFamily.K if side == "LONG" else LongEntryFamily.NONE,
                short_entry_family=ShortEntryFamily.BEAR_SNAP if side == "SHORT" else ShortEntryFamily.NONE,
            )
            if intent is None:
                remaining_candidates.append(candidate)
                self._event_rows.append(
                    {
                        "timestamp": bar.end_ts.isoformat(),
                        "lane_id": self.spec.lane_id,
                        "event_type": "ATPE_ENTRY_BLOCKED",
                        "decision_id": candidate["decision_id"],
                        "symbol": candidate["instrument"],
                        "blocked_reason": (
                            "same_underlying_entry_hold"
                            if self.strategy_engine.state.same_underlying_entry_hold
                            else "runtime_entry_rejected"
                        ),
                    }
                )
                continue
            self._pending_entry_plan = {
                **candidate,
                "order_intent_id": intent.order_intent_id,
                "entry_intent_timestamp": intent.created_at.isoformat(),
                "entry_trigger_bar_id": bar.bar_id,
            }
            self._event_rows.append(
                {
                    "timestamp": bar.end_ts.isoformat(),
                    "lane_id": self.spec.lane_id,
                    "event_type": "ATPE_ENTRY_INTENT",
                    "decision_id": candidate["decision_id"],
                    "order_intent_id": intent.order_intent_id,
                    "symbol": candidate["instrument"],
                    "side": side,
                }
            )
        self._pending_candidates = remaining_candidates

    def _evaluate_exit_triggers(self, bar: Bar) -> None:
        plan = self._active_trade_plan
        if plan is None:
            return
        state = self.strategy_engine.state
        if state.position_side == PositionSide.FLAT or state.open_broker_order_id is not None:
            return
        side = str(plan["side"]).upper()
        stop_price = float(plan["stop_price"])
        target_price = float(plan["target_price"]) if plan.get("target_price") is not None else None
        max_exit_timestamp = datetime.fromisoformat(str(plan["max_exit_timestamp"]))
        exit_reason = None
        latest_feature = _latest_atpe_feature_state_from_bars(
            bars_1m=self._bars_1m.get(self._instrument, []),
            instrument=self._instrument,
        )
        if bool(plan.get("target_checkpoint_reached")):
            ratcheted_stop = _atpe_target_checkpoint_stop_price(
                plan=plan,
                bar=bar,
                side=side,
            )
            if ratcheted_stop != stop_price:
                plan["stop_price"] = ratcheted_stop
                stop_price = ratcheted_stop
        if side == "LONG":
            stop_hit = float(bar.low) <= stop_price
            target_hit = (
                not bool(plan.get("target_checkpoint_reached"))
                and target_price is not None
                and float(bar.high) >= target_price
            )
            if stop_hit and target_hit:
                exit_reason = "atpe_stop_first_conflict"
            elif stop_hit:
                exit_reason = "atpe_checkpoint_stop" if bool(plan.get("target_checkpoint_reached")) else "atpe_stop"
            elif target_hit:
                if _atpe_target_checkpoint_should_continue(latest_feature=latest_feature, side=side):
                    plan["target_checkpoint_reached"] = True
                    plan["target_checkpoint_reached_at"] = bar.end_ts.isoformat()
                    plan["stop_price"] = _atpe_target_checkpoint_stop_price(
                        plan=plan,
                        bar=bar,
                        side=side,
                    )
                    plan["target_price"] = None
                    self._event_rows.append(
                        {
                            "timestamp": bar.end_ts.isoformat(),
                            "lane_id": self.spec.lane_id,
                            "event_type": "ATPE_TARGET_CHECKPOINT_CONTINUE",
                            "decision_id": plan["decision_id"],
                            "symbol": plan["instrument"],
                            "checkpoint_price": plan.get("target_checkpoint_price"),
                            "tightened_stop_price": plan["stop_price"],
                            "health_summary": _atpe_target_health_summary(latest_feature=latest_feature, side=side),
                        }
                    )
                    return
                exit_reason = "atpe_target"
            elif bool(plan.get("target_checkpoint_reached")) and not _atpe_target_checkpoint_should_continue(latest_feature=latest_feature, side=side):
                exit_reason = "atpe_target_momentum_fade"
        else:
            stop_hit = float(bar.high) >= stop_price
            target_hit = (
                not bool(plan.get("target_checkpoint_reached"))
                and target_price is not None
                and float(bar.low) <= target_price
            )
            if stop_hit and target_hit:
                exit_reason = "atpe_stop_first_conflict"
            elif stop_hit:
                exit_reason = "atpe_checkpoint_stop" if bool(plan.get("target_checkpoint_reached")) else "atpe_stop"
            elif target_hit:
                if _atpe_target_checkpoint_should_continue(latest_feature=latest_feature, side=side):
                    plan["target_checkpoint_reached"] = True
                    plan["target_checkpoint_reached_at"] = bar.end_ts.isoformat()
                    plan["stop_price"] = _atpe_target_checkpoint_stop_price(
                        plan=plan,
                        bar=bar,
                        side=side,
                    )
                    plan["target_price"] = None
                    self._event_rows.append(
                        {
                            "timestamp": bar.end_ts.isoformat(),
                            "lane_id": self.spec.lane_id,
                            "event_type": "ATPE_TARGET_CHECKPOINT_CONTINUE",
                            "decision_id": plan["decision_id"],
                            "symbol": plan["instrument"],
                            "checkpoint_price": plan.get("target_checkpoint_price"),
                            "tightened_stop_price": plan["stop_price"],
                            "health_summary": _atpe_target_health_summary(latest_feature=latest_feature, side=side),
                        }
                    )
                    return
                exit_reason = "atpe_target"
            elif bool(plan.get("target_checkpoint_reached")) and not _atpe_target_checkpoint_should_continue(latest_feature=latest_feature, side=side):
                exit_reason = "atpe_target_momentum_fade"
        if exit_reason is None and bar.end_ts >= max_exit_timestamp:
            exit_reason = "atpe_time_stop"
        if exit_reason is None:
            return
        intent = self.strategy_engine.submit_operator_flatten_intent(bar.end_ts, reason_code=exit_reason)
        if intent is None:
            return
        self._event_rows.append(
            {
                "timestamp": bar.end_ts.isoformat(),
                "lane_id": self.spec.lane_id,
                "event_type": "ATPE_EXIT_INTENT",
                "order_intent_id": intent.order_intent_id,
                "decision_id": plan["decision_id"],
                "symbol": plan["instrument"],
                "exit_reason": exit_reason,
            }
        )

    def _rewrite_processed_bars(self) -> None:
        rows = [
            _runtime_processed_bar_row(bar, self.spec)
            for bar in sorted(self._bars_1m.get(self._instrument, []), key=lambda item: item.end_ts)
        ]
        _write_jsonl(self._lane_file("processed_bars.jsonl"), rows)

    def _rewrite_features(self, features_by_instrument: dict[str, list[dict[str, Any]]]) -> None:
        _write_jsonl(self._lane_file("features.jsonl"), list(features_by_instrument.get(self._instrument, [])))

    def _rewrite_signals(self) -> None:
        _write_jsonl(self._lane_file("signals.jsonl"), self._signal_rows)

    def _rewrite_events(self) -> None:
        _write_jsonl(self._lane_file("events.jsonl"), self._event_rows)

    def _sync_execution_artifacts(self) -> None:
        order_intents = sorted(self.repositories.order_intents.list_all(), key=lambda row: str(row.get("created_at") or ""))
        fills = sorted(self.repositories.fills.list_all(), key=lambda row: str(row.get("fill_timestamp") or ""))
        _write_jsonl(self._lane_file("order_intents.jsonl"), order_intents)
        _write_jsonl(self._lane_file("fills.jsonl"), fills)
        bars = [
            _research_bar_to_domain_bar(bar)
            for bar in sorted(self._bars_1m.get(self._instrument, []), key=lambda item: item.end_ts)
        ]
        ledger = build_trade_ledger(
            order_intents,
            fills,
            build_session_lookup(bars),
            point_value=Decimal(str(self.spec.point_value)),
            fee_per_fill=Decimal("0"),
            slippage_per_fill=Decimal("0"),
            bars=bars,
        )
        _write_jsonl(
            self._lane_file("trades.jsonl"),
            [
                {
                    "trade_id": f"{self.spec.lane_id}:{row.trade_id}",
                    "symbol": self._instrument,
                    "direction": row.direction,
                    "entry_timestamp": row.entry_ts.isoformat(),
                    "exit_timestamp": row.exit_ts.isoformat(),
                    "entry_price": str(row.entry_px),
                    "exit_price": str(row.exit_px),
                    "quantity": row.qty,
                    "realized_pnl": str(row.net_pnl),
                    "gross_pnl": str(row.gross_pnl),
                    "fees_paid": str(row.fees),
                    "slippage_cost": str(row.slippage),
                    "exit_reason": row.exit_reason,
                    "setup_family": row.setup_family,
                    "quality_bucket_policy": self.spec.quality_bucket_policy,
                }
                for row in ledger
            ],
        )

    def _write_lane_operator_status(self, observed_at: datetime) -> Path:
        summary = _allow_block_override_summary(self._signal_rows)
        last_processed_end = self.repositories.processed_bars.latest_end_ts()
        intent_count = len(self.repositories.order_intents.list_all())
        fill_count = len(self.repositories.fills.list_all())
        latest_feature = _latest_atpe_feature_state_from_bars(
            bars_1m=self._bars_1m.get(self._instrument, []),
            instrument=self._instrument,
        )
        latest_atp_state = latest_atp_state_summary(latest_feature)
        latest_atp_entry_state = _latest_atpe_phase2_entry_state_from_bars(
            bars_1m=self._bars_1m.get(self._instrument, []),
            instrument=self._instrument,
            runtime_ready=(
                bool(self.strategy_engine.state.entries_enabled)
                and not bool(self.strategy_engine.state.operator_halt)
                and _effective_reconciliation_clean(self._last_reconciliation_payload)
            ),
            position_flat=str(self.strategy_engine.state.position_side.value).upper() == "FLAT",
            one_position_rule_clear=str(self.strategy_engine.state.position_side.value).upper() == "FLAT",
        )
        latest_atp_timing_state = _latest_atpe_phase3_timing_state_from_bars(
            bars_1m=self._bars_1m.get(self._instrument, []),
            instrument=self._instrument,
            runtime_ready=(
                bool(self.strategy_engine.state.entries_enabled)
                and not bool(self.strategy_engine.state.operator_halt)
                and _effective_reconciliation_clean(self._last_reconciliation_payload)
            ),
            position_flat=str(self.strategy_engine.state.position_side.value).upper() == "FLAT",
            one_position_rule_clear=str(self.strategy_engine.state.position_side.value).upper() == "FLAT",
        )
        payload = {
            "generated_at": observed_at.isoformat(),
            "updated_at": observed_at.isoformat(),
            "lane_id": self.spec.lane_id,
            "lane_name": self.spec.display_name,
            "display_name": self.spec.display_name,
            "experimental_status": self.spec.experimental_status,
            "paper_only": self.spec.paper_only,
            "non_approved": self.spec.non_approved,
            "enabled": not self._kill_switch_active,
            "entries_enabled": self.strategy_engine.state.entries_enabled,
            "operator_halt": self.strategy_engine.state.operator_halt,
            "kill_switch_active": self._kill_switch_active,
            "kill_switch_path": str(self._canary_root / "DISABLE_ACTIVE_TREND_PARTICIPATION_CANARY"),
            "quality_bucket_policy": self.spec.quality_bucket_policy,
            "side": self.spec.observer_side,
            "observer_variant_id": self.spec.observer_variant_id,
            "observed_instruments": list(self._observed_instruments),
            "signal_count": len(self._signal_rows),
            "recent_signal_count": len(self._signal_rows),
            "event_count": len(self._event_rows),
            "recent_event_count": len(self._event_rows),
            "intent_count": intent_count,
            "fill_count": fill_count,
            "allow_block_override_summary": summary,
            "latest_atp_state": latest_atp_state,
            "latest_atp_entry_state": latest_atp_entry_state,
            "latest_atp_timing_state": latest_atp_timing_state,
            "position_side": self.strategy_engine.state.position_side.value,
            "entry_timestamp": (
                self.strategy_engine.state.entry_timestamp.isoformat()
                if self.strategy_engine.state.entry_timestamp is not None
                else None
            ),
            "entry_price": (
                str(self.strategy_engine.state.entry_price)
                if self.strategy_engine.state.entry_price is not None
                else None
            ),
            "strategy_status": "RUNNING_PAPER_ONLY_EXPERIMENTAL_CANARY",
            "last_processed_bar_end_ts": (
                last_processed_end.isoformat() if last_processed_end is not None else None
            ),
            "reconciliation": self._last_reconciliation_payload or {},
            "heartbeat_reconciliation": self._heartbeat_reconciliation,
            "order_timeout_watchdog": self._order_timeout_watchdog,
            "live_runtime_mode": "probationary_paper_soak_temp_paper",
            "priority_tier": "lower_priority_than_live_strategies",
            "notes": ["Experimental Canary", "Paper Only", "Non-Approved"],
        }
        self._last_supervisor_status = {
            "experimental_status": self.spec.experimental_status,
            "paper_only": self.spec.paper_only,
            "non_approved": self.spec.non_approved,
            "quality_bucket_policy": self.spec.quality_bucket_policy,
            "recent_signal_count": len(self._signal_rows),
            "recent_event_count": len(self._event_rows),
            "intent_count": intent_count,
            "fill_count": fill_count,
            "allow_block_override_summary": summary,
            "kill_switch_active": self._kill_switch_active,
            "latest_atp_state": latest_atp_state,
            "latest_atp_entry_state": latest_atp_entry_state,
            "latest_atp_timing_state": latest_atp_timing_state,
            "operator_status_line": (
                f"{self.spec.display_name} | {'ENABLED' if not self._kill_switch_active else 'DISABLED'} | "
                f"signals={len(self._signal_rows)} | intents={intent_count} | fills={fill_count} | "
                f"bias={latest_atp_state.get('bias_state')} | pullback={latest_atp_state.get('pullback_state')} | "
                f"entry={latest_atp_entry_state.get('entry_state')} | timing={latest_atp_timing_state.get('timing_state')} | "
                f"blocker={latest_atp_timing_state.get('primary_blocker') or latest_atp_entry_state.get('primary_blocker') or '-'}"
            ),
            "strategy_status": "RUNNING_PAPER_ONLY_EXPERIMENTAL_CANARY",
            "position_side": self.strategy_engine.state.position_side.value,
            "source_family": ATPE_CANARY_SOURCE_FAMILY,
            "symbol": self.spec.symbol,
            "instrument_scope": ",".join(self._observed_instruments),
        }
        return self.structured_logger.write_operator_status(payload)

    def _ensure_runtime_snapshot(self, observed_at: datetime) -> None:
        root = self._canary_root
        snapshot_path = root / "experimental_canaries_snapshot.json"
        payload = _read_json(snapshot_path)
        rows = _default_atpe_runtime_snapshot_rows(root, instruments=self.settings.probationary_atpe_canary_instruments)
        updated_rows: list[dict[str, Any]] = []
        for row in rows:
            if str(row.get("lane_id")) == self.spec.lane_id:
                updated_rows.append(
                    {
                        **row,
                        "generated_at": observed_at.isoformat(),
                        "paper_only": True,
                        "experimental_status": self.spec.experimental_status,
                        "quality_bucket_policy": self.spec.quality_bucket_policy,
                        "side": self.spec.observer_side,
                        "symbols": list(self._observed_instruments),
                        "latest_atp_state": latest_atp_state_summary(
                            _latest_atpe_feature_state_from_bars(
                                bars_1m=self._bars_1m.get(self._instrument, []),
                                instrument=self._instrument,
                            )
                        ),
                        "latest_atp_entry_state": _latest_atpe_phase2_entry_state_from_bars(
                            bars_1m=self._bars_1m.get(self._instrument, []),
                            instrument=self._instrument,
                            runtime_ready=(
                                bool(self.strategy_engine.state.entries_enabled)
                                and not bool(self.strategy_engine.state.operator_halt)
                                and _effective_reconciliation_clean(self._last_reconciliation_payload)
                            ),
                            position_flat=str(self.strategy_engine.state.position_side.value).upper() == "FLAT",
                            one_position_rule_clear=str(self.strategy_engine.state.position_side.value).upper() == "FLAT",
                        ),
                        "latest_atp_timing_state": _latest_atpe_phase3_timing_state_from_bars(
                            bars_1m=self._bars_1m.get(self._instrument, []),
                            instrument=self._instrument,
                            runtime_ready=(
                                bool(self.strategy_engine.state.entries_enabled)
                                and not bool(self.strategy_engine.state.operator_halt)
                                and _effective_reconciliation_clean(self._last_reconciliation_payload)
                            ),
                            position_flat=str(self.strategy_engine.state.position_side.value).upper() == "FLAT",
                            one_position_rule_clear=str(self.strategy_engine.state.position_side.value).upper() == "FLAT",
                        ),
                    }
                )
            else:
                updated_rows.append(row)
        snapshot_payload = {
            **payload,
            "generated_at": observed_at.isoformat(),
            "module": "Active Trend Participation Engine",
            "status": "available",
            "scope_label": "Experimental paper canaries for Active Trend Participation Engine",
            "separation_note": "Canary metrics are isolated from approved production-strategy metrics.",
            "kill_switch": {
                "path": str(root / "DISABLE_ACTIVE_TREND_PARTICIPATION_CANARY"),
                "active": self._kill_switch_active,
                "operator_action": "Use the enable/disable scripts to control canary visibility.",
            },
            "rows": updated_rows,
        }
        (root / "experimental_canaries_snapshot.md").write_text(
            "# Active Trend Participation Engine Experimental Canary\n",
            encoding="utf-8",
        )
        (root / "operator_summary.md").write_text(
            "\n".join(
                [
                    "# Operator Summary",
                    "Paper-only experimental canary lane statuses are now maintained by the probationary paper runtime.",
                    "These ATPE lanes now emit paper intents and deterministic next-bar-open paper fills.",
                ]
            )
            + "\n",
            encoding="utf-8",
        )
        snapshot_path.write_text(json.dumps(snapshot_payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


class ProbationaryAtpCompanionBenchmarkLaneRuntime(ProbationaryPaperLaneRuntime):
    """Continuous paper runtime for the frozen ATP companion benchmark."""

    def __init__(
        self,
        *,
        observed_instruments: Sequence[str],
        **kwargs: Any,
    ) -> None:
        super().__init__(**kwargs)
        self._observed_instruments = tuple(str(value).strip().upper() for value in observed_instruments if str(value).strip())
        if len(self._observed_instruments) != 1:
            raise ValueError(
                f"ATP companion benchmark runtime requires exactly one observed instrument, got {self._observed_instruments!r}"
            )
        self._instrument = self._observed_instruments[0]
        self._benchmark_root = Path(self.settings.probationary_artifacts_path).parent.parent
        self._bars_1m: dict[str, list[ResearchBar]] = {self._instrument: []}
        self._latest_polled_end_ts: dict[str, datetime | None] = {self._instrument: None}
        self._signal_rows: list[dict[str, Any]] = _read_jsonl(self._lane_file("signals.jsonl"))
        self._event_rows: list[dict[str, Any]] = _read_jsonl(self._lane_file("events.jsonl"))
        self._emitted_signal_keys: set[str] = set()
        self._duplicate_bar_suppression_count = 0
        self._latest_feature_rows: list[Any] = []
        self._latest_entry_states: list[Any] = []
        self._latest_timing_states: list[Any] = []
        self._pending_entry_plan: dict[str, Any] | None = None
        self._active_trade_plan: dict[str, Any] | None = None
        self._restore_bars_from_artifacts()
        self._restore_runtime_state()

    def restore_startup(self) -> str | None:
        now = datetime.now(timezone.utc)
        startup_reason = super().restore_startup()
        if startup_reason is not None:
            return startup_reason
        self._sync_execution_artifacts()
        self._write_runtime_state()
        self._write_lane_operator_status(now)
        return None

    def poll_and_process(
        self,
        higher_priority_signals: Sequence[HigherPrioritySignal] = (),
    ) -> tuple[int, dict[str, Any], Path]:
        now = datetime.now(timezone.utc)
        fresh_signal_rows: list[dict[str, Any]] = []
        features_by_instrument: dict[str, list[dict[str, Any]]] = {}
        new_bar_count = 0
        instrument = self._instrument
        latest_end = self._latest_polled_end_ts.get(instrument)
        bars = self.live_polling_service.poll_bars(
            SchwabLivePollRequest(
                internal_symbol=instrument,
                since=latest_end,
            ),
            internal_timeframe="1m",
            default_is_final=True,
        )
        for bar in bars:
            self.repositories.bars.save(bar)
            research_bar = _research_bar_from_domain_bar(bar)
            existing = self._bars_1m.setdefault(instrument, [])
            if any(item.end_ts == research_bar.end_ts for item in existing):
                self._duplicate_bar_suppression_count += 1
                continue
            self.repositories.processed_bars.mark_processed(bar)
            existing.append(research_bar)
            self._latest_polled_end_ts[instrument] = bar.end_ts
            new_bar_count += 1
            self._apply_due_runtime_fills(bar)
            features_by_instrument, bar_signal_rows = self._recompute_live_decisions(
                higher_priority_signals=higher_priority_signals,
                observed_at=bar.end_ts,
            )
            if bar_signal_rows:
                self._signal_rows.extend(bar_signal_rows)
                fresh_signal_rows.extend(bar_signal_rows)
            self._evaluate_exit_triggers(bar)
            self._evaluate_entry_triggers(bar)

        self._bars_1m[instrument] = _prune_research_bars(
            self._bars_1m.get(instrument, []),
            keep_minutes=max(int(self.spec.live_poll_lookback_minutes or 1440), 60),
        )
        if new_bar_count or fresh_signal_rows:
            self._rewrite_processed_bars()
            self._rewrite_features(features_by_instrument)
        self._rewrite_signals()
        self._rewrite_events()
        self._sync_execution_artifacts()
        self._write_runtime_state()
        heartbeat_reconciliation, reconciliation, _ = _run_reconciliation_heartbeat(
            settings=self.settings,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            heartbeat_status=self._heartbeat_reconciliation,
        )
        order_timeout_watchdog, _, _ = _run_order_timeout_watchdog(
            settings=self.settings,
            repositories=self.repositories,
            strategy_engine=self.strategy_engine,
            execution_engine=self.execution_engine,
            structured_logger=self.structured_logger,
            alert_dispatcher=self.alert_dispatcher,
            watchdog_status=self._order_timeout_watchdog,
            occurred_at=now,
        )
        self._heartbeat_reconciliation = heartbeat_reconciliation
        self._order_timeout_watchdog = order_timeout_watchdog
        if reconciliation is not None:
            self._last_reconciliation_payload = dict(reconciliation)
        effective_reconciliation = dict(reconciliation or self._last_reconciliation_payload or {})
        effective_reconciliation.update(
            {
                "lane_id": self.spec.lane_id,
                "runtime_kind": self.spec.runtime_kind,
                "new_bars_last_cycle": new_bar_count,
                "recent_signal_count": len(self._signal_rows),
                "recent_event_count": len(self._event_rows),
                "duplicate_bar_suppression_count": self._duplicate_bar_suppression_count,
            }
        )
        status_path = self._write_lane_operator_status(now)
        return new_bar_count, effective_reconciliation, status_path

    def config_row_extras(self) -> dict[str, Any]:
        runtime_identity = _atp_runtime_identity_payload(self.spec)
        return {
            "experimental_status": self.spec.experimental_status,
            "paper_only": self.spec.paper_only,
            "non_approved": self.spec.non_approved,
            "quality_bucket_policy": self.spec.quality_bucket_policy,
            "observer_side": self.spec.observer_side,
            "observer_variant_id": self.spec.observer_variant_id,
            "observed_instruments": list(self._observed_instruments),
            "scope_label": runtime_identity["scope_label"],
            "execution_authority": False,
            "benchmark_designation": runtime_identity["benchmark_designation"],
            "tracked_strategy_id": runtime_identity["tracked_strategy_id"],
            "participation_policy": runtime_identity["participation_policy"],
        }

    def supervisor_status_extras(self) -> dict[str, Any]:
        latest_timing_state = self._latest_timing_states[-1] if self._latest_timing_states else None
        latest_entry_state = self._latest_entry_states[-1] if self._latest_entry_states else None
        runtime_identity = _atp_runtime_identity_payload(self.spec)
        return {
            "benchmark_designation": runtime_identity["benchmark_designation"],
            "tracked_strategy_id": runtime_identity["tracked_strategy_id"],
            "quality_bucket_policy": self.spec.quality_bucket_policy,
            "scope_label": runtime_identity["scope_label"],
            "strategy_status": runtime_identity["strategy_status"],
            "live_runtime_mode": runtime_identity["live_runtime_mode"],
            "participation_policy": runtime_identity["participation_policy"],
            "latest_atp_state": latest_atp_state_summary(self._latest_feature_rows[-1] if self._latest_feature_rows else None),
            "latest_atp_entry_state": latest_atp_entry_state_summary(latest_entry_state),
            "latest_atp_timing_state": latest_atp_timing_state_summary(latest_timing_state),
            "duplicate_bar_suppression_count": self._duplicate_bar_suppression_count,
            "runtime_attached": True,
        }

    def eligibility_snapshot(self, now: datetime) -> dict[str, Any]:
        last_processed_end = self.repositories.processed_bars.latest_end_ts()
        current_session = label_session_phase(now)
        coarse_session = _phase_coarse_session_group(current_session)
        state = self.strategy_engine.state
        warmup_bars_loaded = len(self._bars_1m.get(self._instrument, []))
        warmup_complete = warmup_bars_loaded >= max(PHASE2_WARMUP_BARS, 1)
        session_allowed = coarse_session in PHASE2_ALLOWED_SESSIONS
        eligible_now = (
            last_processed_end is not None
            and warmup_complete
            and session_allowed
            and state.fault_code is None
            and not state.operator_halt
            and bool(state.entries_enabled)
            and state.position_side == PositionSide.FLAT
            and state.open_broker_order_id is None
        )
        blocker_reason = None
        if last_processed_end is None:
            blocker_reason = "awaiting_live_bars"
        elif not warmup_complete:
            blocker_reason = "warmup_incomplete"
        elif not session_allowed:
            blocker_reason = "wrong_session"
        elif state.fault_code is not None:
            blocker_reason = "fault"
        elif state.operator_halt:
            blocker_reason = "operator_halt"
        elif not state.entries_enabled:
            blocker_reason = "entries_disabled"
        elif state.position_side != PositionSide.FLAT or state.open_broker_order_id is not None:
            blocker_reason = "strategy_not_ready"
        return {
            "current_detected_session": coarse_session,
            "eligible_now": eligible_now,
            "eligibility_reason": blocker_reason,
            "eligibility_detail": state.fault_code if blocker_reason == "fault" else None,
            "allowed_session_match": session_allowed,
            "warmup_complete": warmup_complete,
            "warmup_bars_loaded": warmup_bars_loaded,
            "warmup_bars_required": PHASE2_WARMUP_BARS,
            "latest_completed_bar_end_ts": last_processed_end.isoformat() if last_processed_end is not None else None,
            "last_processed_bar_end_ts": last_processed_end.isoformat() if last_processed_end is not None else None,
        }

    def _lane_file(self, file_name: str) -> Path:
        return Path(self.settings.probationary_artifacts_path) / file_name

    def _runtime_state_path(self) -> Path:
        return self._lane_file("runtime_state.json")

    def _restore_bars_from_artifacts(self) -> None:
        restored: list[ResearchBar] = []
        for row in _read_jsonl(self._lane_file("processed_bars.jsonl")):
            if str(row.get("symbol") or "").upper() != self._instrument:
                continue
            start_ts = row.get("start_ts")
            end_ts = row.get("end_ts")
            if not start_ts or not end_ts:
                continue
            restored.append(
                ResearchBar(
                    instrument=self._instrument,
                    timeframe=str(row.get("timeframe") or "1m"),
                    start_ts=datetime.fromisoformat(str(start_ts)),
                    end_ts=datetime.fromisoformat(str(end_ts)),
                    open=float(row.get("open") or 0.0),
                    high=float(row.get("high") or 0.0),
                    low=float(row.get("low") or 0.0),
                    close=float(row.get("close") or 0.0),
                    volume=int(row.get("volume") or 0),
                    session_label=str(row.get("session_label") or "UNKNOWN"),
                    session_segment=str(row.get("session_segment") or "UNKNOWN"),
                    source="jsonl_restore",
                    provenance=str(row.get("provenance") or "probationary_paper_runtime"),
                )
            )
        self._bars_1m[self._instrument] = sorted(restored, key=lambda item: item.end_ts)
        if self._bars_1m[self._instrument]:
            self._latest_polled_end_ts[self._instrument] = self._bars_1m[self._instrument][-1].end_ts

    def _restore_runtime_state(self) -> None:
        payload = _read_json(self._runtime_state_path())
        self._pending_entry_plan = dict(payload.get("pending_entry_plan") or {}) or None
        self._active_trade_plan = dict(payload.get("active_trade_plan") or {}) or None
        self._duplicate_bar_suppression_count = int(payload.get("duplicate_bar_suppression_count") or 0)
        self._emitted_signal_keys = {
            str(value).strip()
            for value in list(payload.get("emitted_signal_keys") or [])
            if str(value).strip()
        }
        latest_polled = payload.get("latest_polled_end_ts")
        if latest_polled:
            self._latest_polled_end_ts[self._instrument] = datetime.fromisoformat(str(latest_polled))

    def _write_runtime_state(self) -> None:
        latest_entry_state = self._latest_entry_states[-1] if self._latest_entry_states else None
        latest_timing_state = self._latest_timing_states[-1] if self._latest_timing_states else None
        lifecycle_contract = _atp_paper_runtime_lifecycle_contract(
            latest_atp_entry_state=latest_atp_entry_state_summary(latest_entry_state),
            latest_atp_timing_state=latest_atp_timing_state_summary(latest_timing_state),
            order_intents=self.repositories.order_intents.list_all(),
            fills=self.repositories.fills.list_all(),
            trade_rows=_read_jsonl(self._lane_file("trades.jsonl")),
            artifact_context="ATP_COMPANION_PAPER_RUNTIME_STATE",
        )
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "lane_id": self.spec.lane_id,
            "instrument": self._instrument,
            "pending_entry_plan": self._pending_entry_plan,
            "active_trade_plan": self._active_trade_plan,
            "duplicate_bar_suppression_count": self._duplicate_bar_suppression_count,
            "emitted_signal_keys": sorted(self._emitted_signal_keys)[-512:],
            "latest_polled_end_ts": (
                self._latest_polled_end_ts[self._instrument].isoformat()
                if self._latest_polled_end_ts.get(self._instrument) is not None
                else None
            ),
            **lifecycle_contract,
        }
        self._runtime_state_path().write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")

    def _recompute_live_decisions(
        self,
        *,
        higher_priority_signals: Sequence[HigherPrioritySignal],
        observed_at: datetime,
    ) -> tuple[dict[str, list[dict[str, Any]]], list[dict[str, Any]]]:
        del higher_priority_signals
        features_by_instrument: dict[str, list[dict[str, Any]]] = {}
        fresh_signal_rows: list[dict[str, Any]] = []
        bars_1m = sorted(self._bars_1m.get(self._instrument, []), key=lambda item: item.end_ts)
        if not bars_1m:
            self._latest_feature_rows = []
            self._latest_entry_states = []
            self._latest_timing_states = []
            return features_by_instrument, fresh_signal_rows
        bars_5m = _resample_research_bars_5m(bars_1m)
        if bars_5m:
            first_1m_ts = bars_1m[0].end_ts
            last_1m_ts = bars_1m[-1].end_ts
            bars_5m = [bar for bar in bars_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
        features = build_feature_states(bars_5m=bars_5m, bars_1m=bars_1m)
        self._latest_feature_rows = [row for row in features if row.instrument == self._instrument]
        features_by_instrument[self._instrument] = [_runtime_feature_row(feature, self.spec) for feature in self._latest_feature_rows]
        runtime_ready = (
            bool(self.strategy_engine.state.entries_enabled)
            and not bool(self.strategy_engine.state.operator_halt)
            and _effective_reconciliation_clean(self._last_reconciliation_payload)
        )
        position_flat = str(self.strategy_engine.state.position_side.value).upper() == "FLAT"
        self._latest_entry_states = classify_entry_states(
            feature_rows=self._latest_feature_rows,
            allowed_sessions=frozenset({"ASIA", "US"}),
            runtime_ready=runtime_ready,
            position_flat=position_flat,
            one_position_rule_clear=position_flat,
        )
        self._latest_timing_states = classify_timing_states(
            entry_states=self._latest_entry_states,
            bars_1m=bars_1m,
        )
        latest_timing_state = self._latest_timing_states[-1] if self._latest_timing_states else None
        if latest_timing_state is None:
            return features_by_instrument, fresh_signal_rows
        signal_key = "|".join(
            [
                self._instrument,
                latest_timing_state.family_name,
                latest_timing_state.decision_ts.isoformat(),
                latest_timing_state.timing_state,
                latest_timing_state.primary_blocker or "",
                latest_timing_state.timing_bar_ts.isoformat() if latest_timing_state.timing_bar_ts is not None else "",
            ]
        )
        if signal_key not in self._emitted_signal_keys:
            signal_row = _runtime_atp_companion_signal_row(
                spec=self.spec,
                entry_state=self._latest_entry_states[-1] if self._latest_entry_states else None,
                timing_state=latest_timing_state,
                observed_instruments=self._observed_instruments,
            )
            self._emitted_signal_keys.add(signal_key)
            self.structured_logger.log_branch_source(signal_row)
            if signal_row["decision"] == "blocked":
                self.structured_logger.log_rule_block(signal_row)
            self._event_rows.append(
                {
                    "timestamp": observed_at.isoformat(),
                    "lane_id": self.spec.lane_id,
                    "event_type": "ATP_COMPANION_SIGNAL",
                    "decision_ts": latest_timing_state.decision_ts.isoformat(),
                    "timing_state": latest_timing_state.timing_state,
                    "primary_blocker": latest_timing_state.primary_blocker,
                    "entry_executable": latest_timing_state.executable_entry,
                }
            )
            fresh_signal_rows.append(signal_row)

        if (
            latest_timing_state.context_entry_state == ENTRY_ELIGIBLE
            and latest_timing_state.timing_state == ATP_TIMING_CONFIRMED
            and latest_timing_state.executable_entry
            and latest_timing_state.entry_ts is not None
            and latest_timing_state.entry_price is not None
            and self._pending_entry_plan is None
            and self.strategy_engine.state.position_side == PositionSide.FLAT
            and self.strategy_engine.state.open_broker_order_id is None
        ):
            decision_id = f"{latest_timing_state.instrument}|{latest_timing_state.family_name}|{latest_timing_state.decision_ts.isoformat()}"
            self._pending_entry_plan = {
                "decision_id": decision_id,
                "decision_ts": latest_timing_state.decision_ts.isoformat(),
                "entry_ts": latest_timing_state.entry_ts.isoformat(),
                "entry_price": float(latest_timing_state.entry_price),
                "decision_bar_low": float(latest_timing_state.feature_snapshot.get("decision_bar_low") or latest_timing_state.entry_price),
                "decision_bar_high": float(latest_timing_state.feature_snapshot.get("decision_bar_high") or latest_timing_state.entry_price),
                "average_range": max(float(latest_timing_state.feature_snapshot.get("average_range") or 0.25), 0.25),
                "setup_signature": str(latest_timing_state.feature_snapshot.get("setup_signature") or latest_timing_state.family_name),
                "setup_state_signature": str(
                    latest_timing_state.feature_snapshot.get("setup_state_signature")
                    or latest_timing_state.feature_snapshot.get("setup_signature")
                    or latest_timing_state.family_name
                ),
                "setup_quality_bucket": str(latest_timing_state.feature_snapshot.get("setup_quality_bucket") or "MEDIUM"),
                "session_segment": latest_timing_state.session_segment,
                "timing_bar_ts": latest_timing_state.timing_bar_ts.isoformat() if latest_timing_state.timing_bar_ts is not None else None,
                "max_hold_bars_1m": atp_phase2_variant().max_hold_bars_1m,
                "stop_atr_multiple": atp_phase2_variant().stop_atr_multiple,
                "target_r_multiple": atp_phase2_variant().target_r_multiple,
                "reason_code": ATP_V1_LONG_CONTINUATION_VARIANT_ID,
                "signal_source": latest_timing_state.family_name,
            }
            self._event_rows.append(
                {
                    "timestamp": observed_at.isoformat(),
                    "lane_id": self.spec.lane_id,
                    "event_type": "ATP_COMPANION_SETUP_ARMED",
                    "decision_id": decision_id,
                    "decision_ts": latest_timing_state.decision_ts.isoformat(),
                    "entry_ts": latest_timing_state.entry_ts.isoformat(),
                    "entry_price": latest_timing_state.entry_price,
                    "setup_signature": self._pending_entry_plan["setup_signature"],
                    "setup_state_signature": self._pending_entry_plan["setup_state_signature"],
                    "signal_source": latest_timing_state.family_name,
                    "vwap_price_quality_state": latest_timing_state.vwap_price_quality_state,
                }
            )
        return features_by_instrument, fresh_signal_rows

    def _apply_due_runtime_fills(self, bar: Bar) -> None:
        for pending in self.execution_engine.pop_due_replay_fills(bar, self.settings):
            fill = self.execution_engine.materialize_replay_fill(pending, bar)
            self.strategy_engine._persist_order_intent(  # noqa: SLF001
                pending.intent,
                fill.broker_order_id or pending.broker_order_id,
                order_status=OrderStatus.FILLED,
            )
            self.strategy_engine.apply_fill(
                fill_event=fill,
                signal_bar_id=pending.signal_bar_id,
                long_entry_family=pending.long_entry_family,
                short_entry_family=pending.short_entry_family,
                short_entry_source=pending.short_entry_source,
            )
            if pending.intent.is_entry:
                if self._pending_entry_plan is not None and self._pending_entry_plan.get("order_intent_id") == pending.intent.order_intent_id:
                    plan = dict(self._pending_entry_plan)
                    risk = max(float(plan["average_range"]) * float(plan["stop_atr_multiple"]), 0.25)
                    self._active_trade_plan = {
                        **plan,
                        "entry_fill_timestamp": fill.fill_timestamp.isoformat(),
                        "entry_fill_price": float(fill.fill_price),
                        "risk_points": risk,
                        "stop_price": float(plan["decision_bar_low"]) - risk,
                        "target_price": float(fill.fill_price) + risk * float(plan["target_r_multiple"]),
                        "max_exit_timestamp": (
                            fill.fill_timestamp + timedelta(minutes=int(plan["max_hold_bars_1m"]))
                        ).isoformat(),
                    }
                    self._pending_entry_plan = None
                self._event_rows.append(
                    {
                        "timestamp": fill.fill_timestamp.isoformat(),
                        "lane_id": self.spec.lane_id,
                        "event_type": "ATP_COMPANION_ENTRY_FILL",
                        "decision_id": plan.get("decision_id"),
                        "decision_ts": plan.get("decision_ts"),
                        "order_intent_id": fill.order_intent_id,
                        "fill_price": str(fill.fill_price),
                        "setup_signature": plan.get("setup_signature"),
                        "setup_state_signature": plan.get("setup_state_signature"),
                        "signal_source": plan.get("signal_source"),
                        "symbol": pending.intent.symbol,
                    }
                )
            else:
                exit_plan = dict(self._active_trade_plan or {})
                self._event_rows.append(
                    {
                        "timestamp": fill.fill_timestamp.isoformat(),
                        "lane_id": self.spec.lane_id,
                        "event_type": "ATP_COMPANION_EXIT_FILL",
                        "decision_id": exit_plan.get("decision_id"),
                        "decision_ts": exit_plan.get("decision_ts"),
                        "order_intent_id": fill.order_intent_id,
                        "fill_price": str(fill.fill_price),
                        "setup_signature": exit_plan.get("setup_signature"),
                        "setup_state_signature": exit_plan.get("setup_state_signature"),
                        "signal_source": exit_plan.get("signal_source"),
                        "exit_reason": exit_plan.get("exit_reason"),
                        "symbol": pending.intent.symbol,
                    }
                )
                self._active_trade_plan = None
                self._pending_entry_plan = None

    def _evaluate_entry_triggers(self, bar: Bar) -> None:
        if self._pending_entry_plan is None:
            return
        state = self.strategy_engine.state
        if state.position_side != PositionSide.FLAT or state.open_broker_order_id is not None:
            return
        if state.operator_halt or state.fault_code is not None or not state.entries_enabled:
            return
        entry_ts = datetime.fromisoformat(str(self._pending_entry_plan["entry_ts"]))
        if bar.end_ts < entry_ts:
            return
        if bar.end_ts > entry_ts:
            self._event_rows.append(
                {
                    "timestamp": bar.end_ts.isoformat(),
                    "lane_id": self.spec.lane_id,
                    "event_type": "ATP_COMPANION_ENTRY_TIMING_MISSED",
                    "decision_id": self._pending_entry_plan.get("decision_id"),
                    "decision_ts": self._pending_entry_plan["decision_ts"],
                }
            )
            self._pending_entry_plan = None
            return
        intent = self.strategy_engine.submit_runtime_entry_intent(
            bar,
            side="LONG",
            signal_source=str(self._pending_entry_plan["signal_source"]),
            reason_code=str(self._pending_entry_plan["reason_code"]),
            symbol=self._instrument,
            long_entry_family=LongEntryFamily.K,
            short_entry_family=ShortEntryFamily.NONE,
        )
        if intent is None:
            self._event_rows.append(
                {
                    "timestamp": bar.end_ts.isoformat(),
                    "lane_id": self.spec.lane_id,
                    "event_type": "ATP_COMPANION_ENTRY_BLOCKED",
                    "decision_id": self._pending_entry_plan.get("decision_id"),
                    "decision_ts": self._pending_entry_plan["decision_ts"],
                    "blocked_reason": "runtime_entry_rejected",
                }
            )
            return
        self._pending_entry_plan = {
            **self._pending_entry_plan,
            "order_intent_id": intent.order_intent_id,
            "entry_intent_timestamp": intent.created_at.isoformat(),
            "entry_trigger_bar_id": bar.bar_id,
        }
        self._event_rows.append(
            {
                "timestamp": bar.end_ts.isoformat(),
                "lane_id": self.spec.lane_id,
                "event_type": "ATP_COMPANION_ENTRY_INTENT",
                "decision_id": self._pending_entry_plan.get("decision_id"),
                "decision_ts": self._pending_entry_plan["decision_ts"],
                "order_intent_id": intent.order_intent_id,
                "setup_signature": self._pending_entry_plan.get("setup_signature"),
                "setup_state_signature": self._pending_entry_plan.get("setup_state_signature"),
                "signal_source": self._pending_entry_plan.get("signal_source"),
                "symbol": self._instrument,
                "side": "LONG",
            }
        )

    def _evaluate_exit_triggers(self, bar: Bar) -> None:
        plan = self._active_trade_plan
        if plan is None:
            return
        state = self.strategy_engine.state
        if state.position_side == PositionSide.FLAT or state.open_broker_order_id is not None:
            return
        stop_price = float(plan["stop_price"])
        target_price = float(plan["target_price"]) if plan.get("target_price") is not None else None
        max_exit_timestamp = datetime.fromisoformat(str(plan["max_exit_timestamp"]))
        exit_reason = None
        stop_hit = float(bar.low) <= stop_price
        target_hit = target_price is not None and float(bar.high) >= target_price
        if stop_hit and target_hit:
            exit_reason = "atp_companion_stop_first_conflict"
        elif stop_hit:
            exit_reason = "atp_companion_stop"
        elif target_hit:
            exit_reason = "atp_companion_target"
        elif bar.end_ts >= max_exit_timestamp:
            exit_reason = "atp_companion_time_stop"
        if exit_reason is None:
            return
        plan["exit_reason"] = exit_reason
        intent = self.strategy_engine.submit_operator_flatten_intent(bar.end_ts, reason_code=exit_reason)
        if intent is None:
            return
        self._event_rows.append(
            {
                "timestamp": bar.end_ts.isoformat(),
                "lane_id": self.spec.lane_id,
                "event_type": "ATP_COMPANION_EXIT_INTENT",
                "decision_id": plan.get("decision_id"),
                "order_intent_id": intent.order_intent_id,
                "decision_ts": plan["decision_ts"],
                "setup_signature": plan.get("setup_signature"),
                "setup_state_signature": plan.get("setup_state_signature"),
                "signal_source": plan.get("signal_source"),
                "symbol": self._instrument,
                "exit_reason": exit_reason,
            }
        )

    def _rewrite_processed_bars(self) -> None:
        rows = [
            _runtime_processed_bar_row(bar, self.spec)
            for bar in sorted(self._bars_1m.get(self._instrument, []), key=lambda item: item.end_ts)
        ]
        _write_jsonl(self._lane_file("processed_bars.jsonl"), rows)

    def _rewrite_features(self, features_by_instrument: dict[str, list[dict[str, Any]]]) -> None:
        _write_jsonl(self._lane_file("features.jsonl"), list(features_by_instrument.get(self._instrument, [])))

    def _rewrite_signals(self) -> None:
        _write_jsonl(self._lane_file("signals.jsonl"), self._signal_rows)

    def _rewrite_events(self) -> None:
        _write_jsonl(self._lane_file("events.jsonl"), self._event_rows)

    def _sync_execution_artifacts(self) -> None:
        order_intents = sorted(self.repositories.order_intents.list_all(), key=lambda row: str(row.get("created_at") or ""))
        fills = sorted(self.repositories.fills.list_all(), key=lambda row: str(row.get("fill_timestamp") or ""))
        _write_jsonl(self._lane_file("order_intents.jsonl"), order_intents)
        _write_jsonl(self._lane_file("fills.jsonl"), fills)
        bars = [
            _research_bar_to_domain_bar(bar)
            for bar in sorted(self._bars_1m.get(self._instrument, []), key=lambda item: item.end_ts)
        ]
        ledger = build_trade_ledger(
            order_intents,
            fills,
            build_session_lookup(bars),
            point_value=Decimal(str(self.spec.point_value)),
            fee_per_fill=Decimal("0"),
            slippage_per_fill=Decimal("0"),
            bars=bars,
        )
        entry_fill_events_by_timestamp = {
            str(row.get("timestamp") or ""): row
            for row in self._event_rows
            if str(row.get("event_type") or "") == "ATP_COMPANION_ENTRY_FILL"
        }
        exit_fill_events_by_timestamp = {
            str(row.get("timestamp") or ""): row
            for row in self._event_rows
            if str(row.get("event_type") or "") == "ATP_COMPANION_EXIT_FILL"
        }
        _write_jsonl(
            self._lane_file("trades.jsonl"),
            [
                {
                    "trade_id": f"{self.spec.lane_id}:{row.trade_id}",
                    "symbol": self._instrument,
                    "direction": row.direction,
                    "entry_timestamp": row.entry_ts.isoformat(),
                    "exit_timestamp": row.exit_ts.isoformat(),
                    "entry_price": str(row.entry_px),
                    "exit_price": str(row.exit_px),
                    "quantity": row.qty,
                    "realized_pnl": str(row.net_pnl),
                    "gross_pnl": str(row.gross_pnl),
                    "fees_paid": str(row.fees),
                    "slippage_cost": str(row.slippage),
                    "exit_reason": row.exit_reason,
                    "setup_family": row.setup_family,
                    "decision_id": (
                        entry_fill_events_by_timestamp.get(row.entry_ts.isoformat(), {}).get("decision_id")
                        or exit_fill_events_by_timestamp.get(row.exit_ts.isoformat(), {}).get("decision_id")
                    ),
                    "decision_ts": (
                        entry_fill_events_by_timestamp.get(row.entry_ts.isoformat(), {}).get("decision_ts")
                        or exit_fill_events_by_timestamp.get(row.exit_ts.isoformat(), {}).get("decision_ts")
                    ),
                    "setup_signature": (
                        entry_fill_events_by_timestamp.get(row.entry_ts.isoformat(), {}).get("setup_signature")
                        or exit_fill_events_by_timestamp.get(row.exit_ts.isoformat(), {}).get("setup_signature")
                    ),
                    "setup_state_signature": (
                        entry_fill_events_by_timestamp.get(row.entry_ts.isoformat(), {}).get("setup_state_signature")
                        or exit_fill_events_by_timestamp.get(row.exit_ts.isoformat(), {}).get("setup_state_signature")
                    ),
                    "entry_source_family": (
                        entry_fill_events_by_timestamp.get(row.entry_ts.isoformat(), {}).get("signal_source")
                        or exit_fill_events_by_timestamp.get(row.exit_ts.isoformat(), {}).get("signal_source")
                    ),
                    "decision_context_linkage_available": bool(
                        entry_fill_events_by_timestamp.get(row.entry_ts.isoformat(), {}).get("decision_id")
                        or exit_fill_events_by_timestamp.get(row.exit_ts.isoformat(), {}).get("decision_id")
                        or entry_fill_events_by_timestamp.get(row.entry_ts.isoformat(), {}).get("decision_ts")
                        or exit_fill_events_by_timestamp.get(row.exit_ts.isoformat(), {}).get("decision_ts")
                    ),
                    "quality_bucket_policy": self.spec.quality_bucket_policy,
                }
                for row in ledger
            ],
        )

    def _write_lane_operator_status(self, observed_at: datetime) -> Path:
        summary = _allow_block_override_summary(self._signal_rows)
        last_processed_end = self.repositories.processed_bars.latest_end_ts()
        intent_count = len(self.repositories.order_intents.list_all())
        fill_count = len(self.repositories.fills.list_all())
        latest_feature = self._latest_feature_rows[-1] if self._latest_feature_rows else None
        latest_entry_state = self._latest_entry_states[-1] if self._latest_entry_states else None
        latest_timing_state = self._latest_timing_states[-1] if self._latest_timing_states else None
        latest_atp_state = latest_atp_state_summary(latest_feature)
        latest_atp_entry_state = latest_atp_entry_state_summary(latest_entry_state)
        latest_atp_timing_state = latest_atp_timing_state_summary(latest_timing_state)
        trade_rows = _read_jsonl(self._lane_file("trades.jsonl"))
        lifecycle_contract = _atp_paper_runtime_lifecycle_contract(
            latest_atp_entry_state=latest_atp_entry_state,
            latest_atp_timing_state=latest_atp_timing_state,
            order_intents=self.repositories.order_intents.list_all(),
            fills=self.repositories.fills.list_all(),
            trade_rows=trade_rows,
            artifact_context="ATP_COMPANION_PAPER_RUNTIME_STATUS",
        )
        runtime_identity = _atp_runtime_identity_payload(self.spec)
        latest_update_age_seconds = (
            max((observed_at - last_processed_end).total_seconds(), 0.0)
            if last_processed_end is not None
            else None
        )
        payload = {
            "generated_at": observed_at.isoformat(),
            "updated_at": observed_at.isoformat(),
            "lane_id": self.spec.lane_id,
            "lane_name": self.spec.display_name,
            "display_name": self.spec.display_name,
            "experimental_status": self.spec.experimental_status,
            "paper_only": self.spec.paper_only,
            "non_approved": self.spec.non_approved,
            "enabled": True,
            "entries_enabled": self.strategy_engine.state.entries_enabled,
            "operator_halt": self.strategy_engine.state.operator_halt,
            "quality_bucket_policy": self.spec.quality_bucket_policy,
            "side": self.spec.observer_side,
            "observer_variant_id": self.spec.observer_variant_id,
            "observed_instruments": list(self._observed_instruments),
            "signal_count": len(self._signal_rows),
            "recent_signal_count": len(self._signal_rows),
            "event_count": len(self._event_rows),
            "recent_event_count": len(self._event_rows),
            "intent_count": intent_count,
            "fill_count": fill_count,
            "allow_block_override_summary": summary,
            "latest_atp_state": latest_atp_state,
            "latest_atp_entry_state": latest_atp_entry_state,
            "latest_atp_timing_state": latest_atp_timing_state,
            "position_side": self.strategy_engine.state.position_side.value,
            "entry_timestamp": (
                self.strategy_engine.state.entry_timestamp.isoformat()
                if self.strategy_engine.state.entry_timestamp is not None
                else None
            ),
            "entry_price": (
                str(self.strategy_engine.state.entry_price)
                if self.strategy_engine.state.entry_price is not None
                else None
            ),
            "strategy_status": runtime_identity["strategy_status"],
            "scope_label": runtime_identity["scope_label"],
            "participation_policy": runtime_identity["participation_policy"],
            "last_processed_bar_end_ts": (
                last_processed_end.isoformat() if last_processed_end is not None else None
            ),
            "reconciliation": self._last_reconciliation_payload or {},
            "heartbeat_reconciliation": self._heartbeat_reconciliation,
            "order_timeout_watchdog": self._order_timeout_watchdog,
            "live_runtime_mode": runtime_identity["live_runtime_mode"],
            "priority_tier": "paper_tracking_pre_live_soak",
            "benchmark_designation": runtime_identity["benchmark_designation"],
            "tracked_strategy_id": runtime_identity["tracked_strategy_id"],
            "runtime_attached": True,
            "runtime_heartbeat_at": observed_at.isoformat(),
            "runtime_heartbeat_age_seconds": 0.0,
            "data_stale": bool(latest_update_age_seconds is not None and latest_update_age_seconds > 180),
            "latest_bar_age_seconds": latest_update_age_seconds,
            "duplicate_bar_suppression_count": self._duplicate_bar_suppression_count,
            "notes": runtime_identity["notes"],
            **lifecycle_contract,
        }
        return self.structured_logger.write_operator_status(payload)

    def _ensure_runtime_snapshot(self, observed_at: datetime) -> None:
        del observed_at
        return None


class ProbationaryShadowRunner:
    """Poll completed live bars and run the promoted branches in shadow mode."""

    def __init__(
        self,
        settings: StrategySettings,
        repositories: RepositorySet,
        strategy_engine: StrategyEngine,
        live_polling_service: LivePollingService,
        structured_logger: StructuredLogger,
        alert_dispatcher: AlertDispatcher,
        broker_truth_service: SchwabProductionLinkService | None = None,
    ) -> None:
        self._settings = settings
        self._repositories = repositories
        self._strategy_engine = strategy_engine
        self._live_polling_service = live_polling_service
        self._structured_logger = structured_logger
        self._alert_dispatcher = alert_dispatcher
        self._broker_truth_service = broker_truth_service

    def run(self, poll_once: bool = False, max_cycles: int | None = None) -> ProbationaryShadowSummary:
        cycles = 0
        new_bars = 0
        while True:
            processed_before = self._repositories.processed_bars.count()
            latest_processed_end_ts = self._repositories.processed_bars.latest_end_ts()
            bars = self._live_polling_service.poll_bars(
                SchwabLivePollRequest(
                    internal_symbol=self._settings.symbol,
                    since=latest_processed_end_ts,
                ),
                internal_timeframe=self._settings.timeframe,
                default_is_final=True,
            )
            for bar in bars:
                self._strategy_engine.process_bar(bar)
            processed_after = self._repositories.processed_bars.count()
            processed_this_cycle = max(processed_after - processed_before, 0)
            new_bars += processed_this_cycle

            broker_truth_snapshot = self._load_broker_truth_snapshot()
            live_shadow_summary = _build_live_shadow_summary(
                settings=self._settings,
                repositories=self._repositories,
                strategy_engine=self._strategy_engine,
                broker_truth_snapshot=broker_truth_snapshot,
                observed_at=datetime.now(timezone.utc),
            )

            snapshot = self._build_health_snapshot(broker_truth_snapshot=broker_truth_snapshot)
            status_path = self._structured_logger.write_operator_status(
                {
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "health": asdict(snapshot),
                    "last_processed_bar_end_ts": (
                        self._repositories.processed_bars.latest_end_ts().isoformat()
                        if self._repositories.processed_bars.latest_end_ts() is not None
                        else None
                    ),
                    "processed_bars": self._repositories.processed_bars.count(),
                    "new_bars_last_cycle": processed_this_cycle,
                    "position_side": self._strategy_engine.state.position_side.value,
                    "strategy_status": self._strategy_engine.state.strategy_status.value,
                    "fault_code": self._strategy_engine.state.fault_code,
                    "entries_enabled": self._strategy_engine.state.entries_enabled,
                    "operator_halt": self._strategy_engine.state.operator_halt,
                    "approved_long_entry_sources": sorted(self._settings.approved_long_entry_sources),
                    "approved_short_entry_sources": sorted(self._settings.approved_short_entry_sources),
                    "shadow_mode_no_submit": True,
                    "broker_truth_summary": live_shadow_summary.get("broker_truth_summary"),
                    "live_shadow_summary": live_shadow_summary,
                }
            )
            self._structured_logger.log_live_shadow_event(live_shadow_summary)
            self._structured_logger.write_live_shadow_state(live_shadow_summary)
            _sync_runtime_health_alerts(
                alert_dispatcher=self._alert_dispatcher,
                snapshot=snapshot,
                runtime_name="probationary_shadow",
                occurred_at=datetime.now(timezone.utc),
                operator_status_path=str(status_path),
            )

            cycles += 1
            if poll_once or (max_cycles is not None and cycles >= max_cycles):
                return ProbationaryShadowSummary(
                    processed_bars=self._repositories.processed_bars.count(),
                    new_bars=new_bars,
                    last_processed_bar_end_ts=(
                        self._repositories.processed_bars.latest_end_ts().isoformat()
                        if self._repositories.processed_bars.latest_end_ts() is not None
                        else None
                    ),
                    operator_status_path=str(status_path),
                    artifacts_dir=str(self._structured_logger.artifact_dir),
                )
            time_module.sleep(self._settings.live_poll_interval_seconds)

    def _load_broker_truth_snapshot(self) -> dict[str, Any]:
        if self._broker_truth_service is None:
            return {}
        try:
            payload = self._broker_truth_service.snapshot(force_refresh=True)
            return dict(payload) if isinstance(payload, dict) else {}
        except Exception as exc:
            return {
                "status": "degraded",
                "detail": str(exc),
                "health": {
                    "broker_reachable": {"ok": False, "label": "BROKER DEGRADED", "detail": str(exc)},
                    "account_selected": {"ok": False, "label": "ACCOUNT UNKNOWN", "detail": str(exc)},
                    "orders_fresh": {"ok": False, "label": "ORDERS STALE", "detail": str(exc)},
                    "positions_fresh": {"ok": False, "label": "POSITIONS STALE", "detail": str(exc)},
                    "auth": {"ok": False, "label": "AUTH UNKNOWN", "detail": str(exc)},
                },
                "reconciliation": {
                    "status": "blocked",
                    "label": "BROKER TRUTH DEGRADED",
                    "detail": str(exc),
                    "mismatch_count": None,
                },
            }

    def _build_health_snapshot(self, *, broker_truth_snapshot: dict[str, Any] | None = None) -> HealthSnapshot:
        latest_processed_end_ts = self._repositories.processed_bars.latest_end_ts()
        market_data_ok = True
        if latest_processed_end_ts is not None:
            allowed_delay = timedelta(minutes=timeframe_minutes(self._settings.timeframe) * 2)
            market_data_ok = datetime.now(self._settings.timezone_info) - latest_processed_end_ts <= allowed_delay
        broker_truth_summary = _shadow_broker_truth_summary(
            broker_truth_snapshot=broker_truth_snapshot,
            strategy_engine=self._strategy_engine,
            symbol=self._settings.symbol,
        )
        broker_ok = bool(broker_truth_summary.get("broker_reachable", False))
        persistence_ok = True
        reconciliation_clean = not bool(broker_truth_summary.get("requires_reconciliation"))
        invariants_ok = self._strategy_engine.state.fault_code is None
        snapshot = HealthSnapshot(
            market_data_ok=market_data_ok,
            broker_ok=broker_ok,
            persistence_ok=persistence_ok,
            reconciliation_clean=reconciliation_clean,
            invariants_ok=invariants_ok,
            health_status=HealthStatus.HEALTHY,
        )
        return HealthSnapshot(
            market_data_ok=snapshot.market_data_ok,
            broker_ok=snapshot.broker_ok,
            persistence_ok=snapshot.persistence_ok,
            reconciliation_clean=snapshot.reconciliation_clean,
            invariants_ok=snapshot.invariants_ok,
            health_status=derive_health_status(snapshot),
        )


class ProbationaryPaperRunner:
    """Poll completed live bars, materialize deterministic paper fills, and reconcile runtime state."""

    def __init__(
        self,
        settings: StrategySettings,
        repositories: RepositorySet,
        strategy_engine: StrategyEngine,
        execution_engine: ExecutionEngine,
        live_polling_service: LivePollingService,
        structured_logger: StructuredLogger,
        alert_dispatcher: AlertDispatcher,
        runtime_registry: StrategyRuntimeRegistry | None = None,
    ) -> None:
        self._settings = settings
        self._repositories = repositories
        self._strategy_engine = strategy_engine
        self._execution_engine = execution_engine
        self._live_polling_service = live_polling_service
        self._structured_logger = structured_logger
        self._alert_dispatcher = alert_dispatcher
        self._stop_requested = False
        self._runtime_registry = runtime_registry
        self._last_reconciliation_payload: dict[str, Any] | None = None
        self._heartbeat_reconciliation = _initial_reconciliation_heartbeat_status(
            self._settings.reconciliation_heartbeat_interval_seconds
        )
        self._order_timeout_watchdog = _initial_order_timeout_watchdog_status(self._settings)
        self._startup_restore_validation: dict[str, Any] = {}
        self._runtime_started_at = datetime.now(timezone.utc)

    def run(self, poll_once: bool = False, max_cycles: int | None = None) -> ProbationaryPaperSummary:
        stop_reason: str | None = None
        previous_handlers = self._install_signal_handlers()
        try:
            stop_reason = self._restore_and_reconcile_startup()
            if stop_reason is not None:
                return self._finalize_summary(new_bars=0, reconciliation_clean=False, stop_reason=stop_reason)

            cycles = 0
            new_bars = 0
            while True:
                control_result = _apply_probationary_operator_control(
                    settings=self._settings,
                    repositories=self._repositories,
                    strategy_engine=self._strategy_engine,
                    execution_engine=self._execution_engine,
                    structured_logger=self._structured_logger,
                    alert_dispatcher=self._alert_dispatcher,
                )
                latest_processed_end_ts = self._repositories.processed_bars.latest_end_ts()
                bars = self._live_polling_service.poll_bars(
                    SchwabLivePollRequest(
                        internal_symbol=self._settings.symbol,
                        since=latest_processed_end_ts,
                    ),
                    internal_timeframe=self._settings.timeframe,
                    default_is_final=True,
                )
                for bar in bars:
                    self._strategy_engine.process_bar(bar)
                new_bars += len(bars)

                heartbeat_reconciliation, reconciliation, _ = _run_reconciliation_heartbeat(
                    settings=self._settings,
                    strategy_engine=self._strategy_engine,
                    execution_engine=self._execution_engine,
                    heartbeat_status=self._heartbeat_reconciliation,
                )
                order_timeout_watchdog, _, _ = _run_order_timeout_watchdog(
                    settings=self._settings,
                    repositories=self._repositories,
                    strategy_engine=self._strategy_engine,
                    execution_engine=self._execution_engine,
                    structured_logger=self._structured_logger,
                    alert_dispatcher=self._alert_dispatcher,
                    watchdog_status=self._order_timeout_watchdog,
                )
                self._heartbeat_reconciliation = heartbeat_reconciliation
                self._order_timeout_watchdog = order_timeout_watchdog
                if reconciliation is not None:
                    self._last_reconciliation_payload = dict(reconciliation)
                effective_reconciliation = self._last_reconciliation_payload or {}
                snapshot = self._build_health_snapshot(_effective_reconciliation_clean(effective_reconciliation))
                status_path = self._structured_logger.write_operator_status(
                    {
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "health": asdict(snapshot),
                        "last_processed_bar_end_ts": (
                            self._repositories.processed_bars.latest_end_ts().isoformat()
                            if self._repositories.processed_bars.latest_end_ts() is not None
                            else None
                        ),
                        "processed_bars": self._repositories.processed_bars.count(),
                        "new_bars_last_cycle": len(bars),
                        "position_side": self._strategy_engine.state.position_side.value,
                        "strategy_status": self._strategy_engine.state.strategy_status.value,
                        "fault_code": self._strategy_engine.state.fault_code,
                        "entries_enabled": self._strategy_engine.state.entries_enabled,
                        "operator_halt": self._strategy_engine.state.operator_halt,
                        "approved_long_entry_sources": sorted(self._settings.approved_long_entry_sources),
                        "approved_short_entry_sources": sorted(self._settings.approved_short_entry_sources),
                        "open_paper_order_ids": effective_reconciliation.get("broker_open_order_ids", []),
                        "reconciliation": effective_reconciliation,
                        "heartbeat_reconciliation": self._heartbeat_reconciliation,
                        "order_timeout_watchdog": self._order_timeout_watchdog,
                        "startup_restore_validation": self._startup_restore_validation,
                        "live_timing_summary": _build_live_timing_summary(
                            settings=self._settings,
                            repositories=self._repositories,
                            strategy_engine=self._strategy_engine,
                            execution_engine=self._execution_engine,
                            latest_reconciliation=effective_reconciliation,
                            latest_watchdog=self._order_timeout_watchdog,
                            latest_restore=self._startup_restore_validation,
                            observed_at=datetime.now(timezone.utc),
                        ),
                        "runtime_started_at": self._runtime_started_at.isoformat(),
                        "latest_operator_control": control_result,
                    }
                )
                self._structured_logger.write_live_timing_state(
                    _build_live_timing_summary(
                        settings=self._settings,
                        repositories=self._repositories,
                        strategy_engine=self._strategy_engine,
                        execution_engine=self._execution_engine,
                        latest_reconciliation=effective_reconciliation,
                        latest_watchdog=self._order_timeout_watchdog,
                        latest_restore=self._startup_restore_validation,
                        observed_at=datetime.now(timezone.utc),
                    )
                )

                if not _effective_reconciliation_clean(effective_reconciliation):
                    stop_reason = "paper_reconciliation_mismatch"
                    self._alert_dispatcher.emit(
                        "error",
                        "paper_reconciliation_mismatch",
                        "Paper runtime reconciliation failed; stopping paper soak.",
                        effective_reconciliation,
                    )
                    self._strategy_engine.force_fault(datetime.now(timezone.utc), stop_reason)
                    return ProbationaryPaperSummary(
                        processed_bars=self._repositories.processed_bars.count(),
                        new_bars=new_bars,
                        last_processed_bar_end_ts=(
                            self._repositories.processed_bars.latest_end_ts().isoformat()
                            if self._repositories.processed_bars.latest_end_ts() is not None
                            else None
                        ),
                        operator_status_path=str(status_path),
                        artifacts_dir=str(self._structured_logger.artifact_dir),
                        reconciliation_clean=False,
                        stop_reason=stop_reason,
                    )

                _sync_runtime_health_alerts(
                    alert_dispatcher=self._alert_dispatcher,
                    snapshot=snapshot,
                    runtime_name="probationary_paper",
                    occurred_at=datetime.now(timezone.utc),
                    operator_status_path=str(status_path),
                )

                if _stop_after_cycle_is_safe(control_result, self._strategy_engine, self._execution_engine):
                    finalized = dict(control_result)
                    finalized["status"] = "completed"
                    finalized["completed_at"] = datetime.now(timezone.utc).isoformat()
                    finalized["message"] = "Stop After Current Cycle completed; paper runtime stopped at a safe flat point."
                    control_path = self._settings.resolved_probationary_operator_control_path
                    control_path.write_text(json.dumps(finalized, sort_keys=True, indent=2) + "\n", encoding="utf-8")
                    self._structured_logger.log_operator_control(finalized)
                    self._alert_dispatcher.emit(
                        "info",
                        "operator_control_applied",
                        finalized["message"],
                        finalized,
                    )
                    stop_reason = "operator_stop_after_cycle"
                    return ProbationaryPaperSummary(
                        processed_bars=self._repositories.processed_bars.count(),
                        new_bars=new_bars,
                        last_processed_bar_end_ts=(
                            self._repositories.processed_bars.latest_end_ts().isoformat()
                            if self._repositories.processed_bars.latest_end_ts() is not None
                            else None
                        ),
                        operator_status_path=str(status_path),
                        artifacts_dir=str(self._structured_logger.artifact_dir),
                        reconciliation_clean=True,
                        stop_reason=stop_reason,
                    )

                cycles += 1
                if poll_once or (max_cycles is not None and cycles >= max_cycles) or self._stop_requested:
                    stop_reason = "signal_stop_requested" if self._stop_requested else stop_reason
                    return ProbationaryPaperSummary(
                        processed_bars=self._repositories.processed_bars.count(),
                        new_bars=new_bars,
                        last_processed_bar_end_ts=(
                            self._repositories.processed_bars.latest_end_ts().isoformat()
                            if self._repositories.processed_bars.latest_end_ts() is not None
                            else None
                        ),
                        operator_status_path=str(status_path),
                        artifacts_dir=str(self._structured_logger.artifact_dir),
                        reconciliation_clean=True,
                        stop_reason=stop_reason,
                    )
                time_module.sleep(self._settings.live_poll_interval_seconds)
        finally:
            self._restore_signal_handlers(previous_handlers)

    def _build_health_snapshot(self, reconciliation_clean: bool) -> HealthSnapshot:
        latest_processed_end_ts = self._repositories.processed_bars.latest_end_ts()
        market_data_ok = True
        if latest_processed_end_ts is not None:
            allowed_delay = timedelta(minutes=timeframe_minutes(self._settings.timeframe) * 2)
            market_data_ok = datetime.now(self._settings.timezone_info) - latest_processed_end_ts <= allowed_delay
        broker_ok = bool(self._execution_engine.broker.is_connected())
        persistence_ok = True
        invariants_ok = self._strategy_engine.state.fault_code is None
        snapshot = HealthSnapshot(
            market_data_ok=market_data_ok,
            broker_ok=broker_ok,
            persistence_ok=persistence_ok,
            reconciliation_clean=reconciliation_clean,
            invariants_ok=invariants_ok,
            health_status=HealthStatus.HEALTHY,
        )
        return HealthSnapshot(
            market_data_ok=snapshot.market_data_ok,
            broker_ok=snapshot.broker_ok,
            persistence_ok=snapshot.persistence_ok,
            reconciliation_clean=snapshot.reconciliation_clean,
            invariants_ok=snapshot.invariants_ok,
            health_status=derive_health_status(snapshot),
        )

    def _restore_and_reconcile_startup(self) -> str | None:
        restore_started_at = datetime.now(timezone.utc)
        pre_restore_state = _restore_validation_state_snapshot(
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
        )
        pre_restore_counts = _restore_validation_record_counts(self._repositories)
        _restore_paper_runtime_state(
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
        )
        reconciliation = _reconcile_paper_runtime(
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
            trigger="startup",
            apply_repairs=True,
        )
        self._last_reconciliation_payload = dict(reconciliation)
        self._startup_restore_validation = _record_restore_validation(
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
            structured_logger=self._structured_logger,
            alert_dispatcher=self._alert_dispatcher,
            restore_started_at=restore_started_at,
            reconciliation=reconciliation,
            scope_label="Paper soak runtime",
            runtime_name="probationary_paper",
            instrument=self._settings.symbol,
            before_state_summary=pre_restore_state,
            before_counts=pre_restore_counts,
        )
        self._structured_logger.write_live_timing_state(
            _build_live_timing_summary(
                settings=self._settings,
                repositories=self._repositories,
                strategy_engine=self._strategy_engine,
                execution_engine=self._execution_engine,
                latest_reconciliation=self._last_reconciliation_payload,
                latest_watchdog=self._order_timeout_watchdog,
                latest_restore=self._startup_restore_validation,
                observed_at=datetime.now(timezone.utc),
            )
        )
        if reconciliation["clean"] or reconciliation.get("classification") == "safe_repair":
            return None
        self._alert_dispatcher.emit(
            severity="BLOCKING",
            code="paper_startup_reconciliation_failed",
            message="Paper soak startup reconciliation failed; refusing to run.",
            payload=reconciliation,
            category="state_restore_failure",
            title="State Restore Failure",
            dedup_key="probationary_paper:startup_reconciliation_failed",
            recommended_action="Inspect broker/internal state before restarting paper soak.",
            active=True,
        )
        return "paper_startup_reconciliation_failed"

    def _finalize_summary(
        self,
        *,
        new_bars: int,
        reconciliation_clean: bool,
        stop_reason: str | None,
    ) -> ProbationaryPaperSummary:
        status_path = self._structured_logger.artifact_dir / "operator_status.json"
        return ProbationaryPaperSummary(
            processed_bars=self._repositories.processed_bars.count(),
            new_bars=new_bars,
            last_processed_bar_end_ts=(
                self._repositories.processed_bars.latest_end_ts().isoformat()
                if self._repositories.processed_bars.latest_end_ts() is not None
                else None
            ),
            operator_status_path=str(status_path),
            artifacts_dir=str(self._structured_logger.artifact_dir),
            reconciliation_clean=reconciliation_clean,
            stop_reason=stop_reason,
        )

    def _install_signal_handlers(self):
        previous = {
            signal.SIGINT: signal.getsignal(signal.SIGINT),
            signal.SIGTERM: signal.getsignal(signal.SIGTERM),
        }

        def _request_stop(signum, _frame) -> None:
            self._stop_requested = True
            self._alert_dispatcher.emit(
                severity="INFO",
                code="paper_runtime_stop_requested",
                message=f"Received signal {signum}; stopping after the current cycle.",
                payload={"signal": signum},
                category="runtime_recovery",
                title="Runtime Stop Requested",
                dedup_key=f"paper_runtime_stop_requested:{signum}",
                active=False,
                coalesce=False,
            )

        signal.signal(signal.SIGINT, _request_stop)
        signal.signal(signal.SIGTERM, _request_stop)
        return previous

    def _restore_signal_handlers(self, previous_handlers) -> None:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


class ProbationaryPaperSupervisor:
    """Supervise multiple probationary paper lanes under one desk-level paper runtime."""

    def __init__(
        self,
        *,
        settings: StrategySettings,
        lanes: Sequence[ProbationaryPaperLaneRuntime],
        structured_logger: StructuredLogger,
        alert_dispatcher: AlertDispatcher,
        runtime_registry: StrategyRuntimeRegistry | None = None,
    ) -> None:
        self._settings = settings
        self._lanes = list(lanes)
        self._structured_logger = structured_logger
        self._alert_dispatcher = alert_dispatcher
        self._stop_requested = False
        self._runtime_registry = runtime_registry

    def run(self, poll_once: bool = False, max_cycles: int | None = None) -> ProbationaryPaperSummary:
        stop_reason: str | None = None
        previous_handlers = self._install_signal_handlers()
        try:
            _write_probationary_paper_config_in_force(self._settings, self._lanes, self._structured_logger)
            risk_state = _load_probationary_paper_risk_state(self._settings)

            for lane in self._lanes:
                startup_reason = lane.restore_startup()
                if startup_reason is not None:
                    stop_reason = f"{lane.spec.lane_id}:{startup_reason}"
                    _write_probationary_supervisor_operator_status(
                        settings=self._settings,
                        lanes=self._lanes,
                        structured_logger=self._structured_logger,
                        risk_state=risk_state,
                        latest_operator_control=None,
                        lane_metrics=None,
                    )
                    return self._finalize_summary(new_bars=0, reconciliation_clean=False, stop_reason=stop_reason)

            cycles = 0
            new_bars = 0
            while True:
                risk_state = _ensure_probationary_paper_risk_state_session(
                    risk_state,
                    _resolve_probationary_supervisor_session_date(self._settings, self._lanes),
                )
                control_result = _apply_probationary_supervisor_operator_control(
                    settings=self._settings,
                    lanes=self._lanes,
                    structured_logger=self._structured_logger,
                    alert_dispatcher=self._alert_dispatcher,
                    risk_state=risk_state,
                )
                _apply_probationary_same_underlying_entry_holds(
                    settings=self._settings,
                    lanes=self._lanes,
                    structured_logger=self._structured_logger,
                    alert_dispatcher=self._alert_dispatcher,
                )
                if control_result is not None:
                    _write_probationary_supervisor_operator_status(
                        settings=self._settings,
                        lanes=self._lanes,
                        structured_logger=self._structured_logger,
                        risk_state=risk_state,
                        latest_operator_control=control_result,
                        lane_metrics=None,
                    )

                reconciliation_clean = True
                higher_priority_signals = _probationary_supervisor_higher_priority_signals(self._lanes)
                for lane in self._lanes:
                    if getattr(lane.spec, "runtime_kind", "") in {
                        ATPE_CANARY_RUNTIME_KIND,
                        ATP_COMPANION_BENCHMARK_RUNTIME_KIND,
                    }:
                        lane_new_bars, reconciliation, _ = lane.poll_and_process(
                            higher_priority_signals=higher_priority_signals
                        )
                    else:
                        lane_new_bars, reconciliation, _ = lane.poll_and_process()
                    new_bars += lane_new_bars
                    if not reconciliation["clean"]:
                        reconciliation_clean = False

                session_date = _resolve_probationary_supervisor_session_date(self._settings, self._lanes)
                risk_state = _ensure_probationary_paper_risk_state_session(risk_state, session_date)
                lane_metrics = {
                    lane.spec.lane_id: _build_probationary_paper_lane_metrics(lane, session_date)
                    for lane in self._lanes
                }
                risk_state, risk_events = _apply_probationary_paper_risk_controls(
                    settings=self._settings,
                    lanes=self._lanes,
                    lane_metrics=lane_metrics,
                    risk_state=risk_state,
                    structured_logger=self._structured_logger,
                    alert_dispatcher=self._alert_dispatcher,
                )
                _write_probationary_paper_risk_artifacts(
                    settings=self._settings,
                    lanes=self._lanes,
                    lane_metrics=lane_metrics,
                    risk_state=risk_state,
                    structured_logger=self._structured_logger,
                    risk_events=risk_events,
                )
                status_path = _write_probationary_supervisor_operator_status(
                    settings=self._settings,
                    lanes=self._lanes,
                    structured_logger=self._structured_logger,
                    risk_state=risk_state,
                    latest_operator_control=control_result,
                    lane_metrics=lane_metrics,
                )

                if not reconciliation_clean:
                    stop_reason = "paper_reconciliation_mismatch"
                    return ProbationaryPaperSummary(
                        processed_bars=sum(lane.repositories.processed_bars.count() for lane in self._lanes),
                        new_bars=new_bars,
                        last_processed_bar_end_ts=_latest_probationary_lane_processed_ts(self._lanes),
                        operator_status_path=str(status_path),
                        artifacts_dir=str(self._structured_logger.artifact_dir),
                        reconciliation_clean=False,
                        stop_reason=stop_reason,
                    )

                cycles += 1
                if (
                    poll_once
                    or (max_cycles is not None and cycles >= max_cycles)
                    or self._stop_requested
                    or _stop_after_cycle_is_safe_for_supervisor(control_result, self._lanes)
                ):
                    if self._stop_requested and stop_reason is None:
                        stop_reason = "signal_stop_requested"
                    elif _stop_after_cycle_is_safe_for_supervisor(control_result, self._lanes):
                        stop_reason = "operator_stop_after_cycle"
                    return ProbationaryPaperSummary(
                        processed_bars=sum(lane.repositories.processed_bars.count() for lane in self._lanes),
                        new_bars=new_bars,
                        last_processed_bar_end_ts=_latest_probationary_lane_processed_ts(self._lanes),
                        operator_status_path=str(status_path),
                        artifacts_dir=str(self._structured_logger.artifact_dir),
                        reconciliation_clean=True,
                        stop_reason=stop_reason,
                    )
                time_module.sleep(self._settings.live_poll_interval_seconds)
        finally:
            self._restore_signal_handlers(previous_handlers)

    def _finalize_summary(
        self,
        *,
        new_bars: int,
        reconciliation_clean: bool,
        stop_reason: str | None,
    ) -> ProbationaryPaperSummary:
        status_path = self._structured_logger.artifact_dir / "operator_status.json"
        return ProbationaryPaperSummary(
            processed_bars=sum(lane.repositories.processed_bars.count() for lane in self._lanes),
            new_bars=new_bars,
            last_processed_bar_end_ts=_latest_probationary_lane_processed_ts(self._lanes),
            operator_status_path=str(status_path),
            artifacts_dir=str(self._structured_logger.artifact_dir),
            reconciliation_clean=reconciliation_clean,
            stop_reason=stop_reason,
        )

    def _install_signal_handlers(self):
        previous = {
            signal.SIGINT: signal.getsignal(signal.SIGINT),
            signal.SIGTERM: signal.getsignal(signal.SIGTERM),
        }

        def _request_stop(signum, _frame) -> None:
            self._stop_requested = True
            self._alert_dispatcher.emit(
                severity="INFO",
                code="paper_runtime_stop_requested",
                message=f"Received signal {signum}; stopping after the current cycle.",
                payload={"signal": signum},
                category="runtime_recovery",
                title="Runtime Stop Requested",
                dedup_key=f"paper_runtime_stop_requested:{signum}",
                active=False,
                coalesce=False,
            )

        signal.signal(signal.SIGINT, _request_stop)
        signal.signal(signal.SIGTERM, _request_stop)
        return previous

    def _restore_signal_handlers(self, previous_handlers) -> None:
        for signum, handler in previous_handlers.items():
            signal.signal(signum, handler)


class ProbationaryLiveStrategyPilotRunner:
    """Run the real MGC runtime on live bars with broker submits allowed only under pilot gates."""

    def __init__(
        self,
        settings: StrategySettings,
        repositories: RepositorySet,
        strategy_engine: StrategyEngine,
        execution_engine: ExecutionEngine,
        live_polling_service: LivePollingService,
        structured_logger: StructuredLogger,
        alert_dispatcher: AlertDispatcher,
        broker_truth_service: SchwabProductionLinkService | Any | None = None,
    ) -> None:
        self._settings = settings
        self._repositories = repositories
        self._strategy_engine = strategy_engine
        self._execution_engine = execution_engine
        self._live_polling_service = live_polling_service
        self._structured_logger = structured_logger
        self._alert_dispatcher = alert_dispatcher
        self._broker_truth_service = broker_truth_service
        self._last_reconciliation_payload: dict[str, Any] | None = None
        self._heartbeat_reconciliation = _initial_reconciliation_heartbeat_status(
            self._settings.reconciliation_heartbeat_interval_seconds
        )
        self._order_timeout_watchdog = _initial_order_timeout_watchdog_status(self._settings)
        self._startup_restore_validation: dict[str, Any] = {}
        self._latest_broker_truth_snapshot: dict[str, Any] = {}

    def run(self, poll_once: bool = False, max_cycles: int | None = None) -> ProbationaryLiveStrategyPilotSummary:
        stop_reason = self._restore_and_reconcile_startup()
        latest_fill_sync: dict[str, Any] = {}
        status_path = self._structured_logger.artifact_dir / "operator_status.json"
        summary_path = self._write_live_strategy_pilot_summary(datetime.now(timezone.utc), latest_fill_sync=latest_fill_sync)
        cycle_path = self._write_live_strategy_pilot_cycle_summary(datetime.now(timezone.utc))
        if stop_reason is not None:
            return ProbationaryLiveStrategyPilotSummary(
                processed_bars=self._repositories.processed_bars.count(),
                new_bars=0,
                last_processed_bar_end_ts=(
                    self._repositories.processed_bars.latest_end_ts().isoformat()
                    if self._repositories.processed_bars.latest_end_ts() is not None
                    else None
                ),
                operator_status_path=str(status_path),
                artifacts_dir=str(self._structured_logger.artifact_dir),
                summary_path=str(summary_path),
                stop_reason=stop_reason,
            )

        cycles = 0
        new_bars = 0
        while True:
            control_result = _apply_probationary_operator_control(
                settings=self._settings,
                repositories=self._repositories,
                strategy_engine=self._strategy_engine,
                execution_engine=self._execution_engine,
                structured_logger=self._structured_logger,
                alert_dispatcher=self._alert_dispatcher,
            )
            self._latest_broker_truth_snapshot = _load_live_strategy_broker_truth_snapshot(
                execution_engine=self._execution_engine,
                broker_truth_service=self._broker_truth_service,
                force_refresh=True,
            )
            latest_processed_end_ts = self._repositories.processed_bars.latest_end_ts()
            bars = self._live_polling_service.poll_bars(
                SchwabLivePollRequest(
                    internal_symbol=self._settings.symbol,
                    since=latest_processed_end_ts,
                ),
                internal_timeframe=self._settings.timeframe,
                default_is_final=True,
            )
            for bar in bars:
                self._strategy_engine.process_bar(bar)
            new_bars += len(bars)

            self._latest_broker_truth_snapshot = _load_live_strategy_broker_truth_snapshot(
                execution_engine=self._execution_engine,
                broker_truth_service=self._broker_truth_service,
                force_refresh=True,
            )
            latest_fill_sync = _run_live_strategy_fill_sync(
                repositories=self._repositories,
                strategy_engine=self._strategy_engine,
                execution_engine=self._execution_engine,
                observed_at=datetime.now(timezone.utc),
            )
            heartbeat_reconciliation, reconciliation, _ = _run_reconciliation_heartbeat(
                settings=self._settings,
                strategy_engine=self._strategy_engine,
                execution_engine=self._execution_engine,
                heartbeat_status=self._heartbeat_reconciliation,
            )
            order_timeout_watchdog, _, _ = _run_order_timeout_watchdog(
                settings=self._settings,
                repositories=self._repositories,
                strategy_engine=self._strategy_engine,
                execution_engine=self._execution_engine,
                structured_logger=self._structured_logger,
                alert_dispatcher=self._alert_dispatcher,
                watchdog_status=self._order_timeout_watchdog,
            )
            self._heartbeat_reconciliation = heartbeat_reconciliation
            self._order_timeout_watchdog = order_timeout_watchdog
            if reconciliation is not None:
                self._last_reconciliation_payload = dict(reconciliation)
            effective_reconciliation = dict(reconciliation or self._last_reconciliation_payload or {})

            status_path = self._structured_logger.write_operator_status(
                {
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                    "health": asdict(self._build_health_snapshot(self._latest_broker_truth_snapshot)),
                    "last_processed_bar_end_ts": (
                        self._repositories.processed_bars.latest_end_ts().isoformat()
                        if self._repositories.processed_bars.latest_end_ts() is not None
                        else None
                    ),
                    "processed_bars": self._repositories.processed_bars.count(),
                    "new_bars_last_cycle": len(bars),
                    "position_side": self._strategy_engine.state.position_side.value,
                    "strategy_status": self._strategy_engine.state.strategy_status.value,
                    "fault_code": self._strategy_engine.state.fault_code,
                    "entries_enabled": self._strategy_engine.state.entries_enabled,
                    "operator_halt": self._strategy_engine.state.operator_halt,
                    "approved_long_entry_sources": sorted(self._settings.approved_long_entry_sources),
                    "approved_short_entry_sources": sorted(self._settings.approved_short_entry_sources),
                    "reconciliation": effective_reconciliation,
                    "heartbeat_reconciliation": self._heartbeat_reconciliation,
                    "order_timeout_watchdog": self._order_timeout_watchdog,
                    "startup_restore_validation": self._startup_restore_validation,
                    "live_timing_summary": _build_live_timing_summary(
                        settings=self._settings,
                        repositories=self._repositories,
                        strategy_engine=self._strategy_engine,
                        execution_engine=self._execution_engine,
                        latest_reconciliation=effective_reconciliation,
                        latest_watchdog=self._order_timeout_watchdog,
                        latest_restore=self._startup_restore_validation,
                        observed_at=datetime.now(timezone.utc),
                    ),
                    "live_strategy_pilot_enabled": self._settings.live_strategy_pilot_enabled,
                    "live_strategy_pilot_submit_enabled": self._settings.live_strategy_pilot_submit_enabled,
                    "live_strategy_pilot_summary": _build_live_strategy_pilot_summary(
                        settings=self._settings,
                        repositories=self._repositories,
                        strategy_engine=self._strategy_engine,
                        execution_engine=self._execution_engine,
                        broker_truth_snapshot=self._latest_broker_truth_snapshot,
                        latest_reconciliation=effective_reconciliation,
                        latest_watchdog=self._order_timeout_watchdog,
                        latest_restore=self._startup_restore_validation,
                        latest_fill_sync=latest_fill_sync,
                        observed_at=datetime.now(timezone.utc),
                    ),
                    "live_strategy_pilot_cycle": _build_live_strategy_pilot_cycle_summary(
                        settings=self._settings,
                        repositories=self._repositories,
                        strategy_engine=self._strategy_engine,
                        execution_engine=self._execution_engine,
                        latest_reconciliation=effective_reconciliation,
                        latest_watchdog=self._order_timeout_watchdog,
                        latest_restore=self._startup_restore_validation,
                        observed_at=datetime.now(timezone.utc),
                    ),
                    "latest_operator_control": control_result,
                }
            )
            summary_path = self._write_live_strategy_pilot_summary(datetime.now(timezone.utc), latest_fill_sync=latest_fill_sync)
            cycle_path = self._write_live_strategy_pilot_cycle_summary(datetime.now(timezone.utc))
            _sync_runtime_health_alerts(
                alert_dispatcher=self._alert_dispatcher,
                snapshot=self._build_health_snapshot(self._latest_broker_truth_snapshot),
                runtime_name="probationary_live_strategy_pilot",
                occurred_at=datetime.now(timezone.utc),
                operator_status_path=str(status_path),
            )
            cycle_state = _read_json(Path(cycle_path))
            cycles += 1
            if (
                poll_once
                or (max_cycles is not None and cycles >= max_cycles)
                or str(cycle_state.get("final_result") or "") in LIVE_STRATEGY_PILOT_CYCLE_TERMINAL_RESULTS
            ):
                return ProbationaryLiveStrategyPilotSummary(
                    processed_bars=self._repositories.processed_bars.count(),
                    new_bars=new_bars,
                    last_processed_bar_end_ts=(
                        self._repositories.processed_bars.latest_end_ts().isoformat()
                        if self._repositories.processed_bars.latest_end_ts() is not None
                        else None
                    ),
                    operator_status_path=str(status_path),
                    artifacts_dir=str(self._structured_logger.artifact_dir),
                    summary_path=str(summary_path),
                    stop_reason=(
                        str(cycle_state.get("auto_stop_reason") or "")
                        or (None if poll_once or (max_cycles is not None and cycles >= max_cycles) else None)
                    ),
                )
            time_module.sleep(self._settings.live_poll_interval_seconds)

    def _restore_and_reconcile_startup(self) -> str | None:
        restore_started_at = datetime.now(timezone.utc)
        pre_restore_state = _restore_validation_state_snapshot(
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
        )
        pre_restore_counts = _restore_validation_record_counts(self._repositories)
        self._latest_broker_truth_snapshot = _restore_live_runtime_state(
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
            broker_truth_service=self._broker_truth_service,
        )
        reconciliation = _reconcile_paper_runtime(
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
            trigger="startup",
            apply_repairs=True,
        )
        self._last_reconciliation_payload = dict(reconciliation)
        self._startup_restore_validation = _record_restore_validation(
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
            structured_logger=self._structured_logger,
            alert_dispatcher=self._alert_dispatcher,
            restore_started_at=restore_started_at,
            reconciliation=reconciliation,
            scope_label="Live strategy pilot",
            runtime_name="probationary_live_strategy_pilot",
            instrument=self._settings.symbol,
            before_state_summary=pre_restore_state,
            before_counts=pre_restore_counts,
        )
        if reconciliation["clean"] or reconciliation.get("classification") == "safe_repair":
            return None
        self._alert_dispatcher.emit(
            severity="BLOCKING",
            code="live_strategy_pilot_startup_reconciliation_failed",
            message="Live strategy pilot startup reconciliation failed; refusing to run.",
            payload={**reconciliation, "instrument": self._settings.symbol},
            category="state_restore_failure",
            title="Live Strategy Pilot Restore Failure",
            dedup_key="live_strategy_pilot_startup_reconciliation_failed",
            recommended_action="Inspect broker/internal state before restarting the live strategy pilot.",
            active=True,
        )
        return "live_strategy_pilot_startup_reconciliation_failed"

    def _write_live_strategy_pilot_summary(self, observed_at: datetime, *, latest_fill_sync: dict[str, Any]) -> Path:
        signal_observability = _build_live_strategy_signal_observability_summary(
            settings=self._settings,
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            observed_at=observed_at,
        )
        self._structured_logger.write_live_strategy_signal_observability_state(signal_observability)
        payload = _build_live_strategy_pilot_summary(
            settings=self._settings,
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
            broker_truth_snapshot=self._latest_broker_truth_snapshot,
            latest_reconciliation=self._last_reconciliation_payload,
            latest_watchdog=self._order_timeout_watchdog,
            latest_restore=self._startup_restore_validation,
            latest_fill_sync=latest_fill_sync,
            signal_observability=signal_observability,
            observed_at=observed_at,
        )
        self._structured_logger.log_live_strategy_pilot_event(payload)
        return self._structured_logger.write_live_strategy_pilot_state(payload)

    def _write_live_strategy_pilot_cycle_summary(self, observed_at: datetime) -> Path:
        payload = _build_live_strategy_pilot_cycle_summary(
            settings=self._settings,
            repositories=self._repositories,
            strategy_engine=self._strategy_engine,
            execution_engine=self._execution_engine,
            latest_reconciliation=self._last_reconciliation_payload,
            latest_watchdog=self._order_timeout_watchdog,
            latest_restore=self._startup_restore_validation,
            observed_at=observed_at,
        )
        self._structured_logger.log_live_strategy_pilot_cycle_event(payload)
        return self._structured_logger.write_live_strategy_pilot_cycle_state(payload)

    def _build_health_snapshot(self, broker_truth_snapshot: dict[str, Any]) -> HealthSnapshot:
        latest_processed_end_ts = self._repositories.processed_bars.latest_end_ts()
        market_data_ok = True
        if latest_processed_end_ts is not None:
            allowed_delay = timedelta(minutes=timeframe_minutes(self._settings.timeframe) * 2)
            market_data_ok = datetime.now(self._settings.timezone_info) - latest_processed_end_ts <= allowed_delay
        broker_truth_summary = _shadow_broker_truth_summary(
            broker_truth_snapshot=broker_truth_snapshot,
            strategy_engine=self._strategy_engine,
            symbol=self._settings.symbol,
        )
        broker_ok = bool(broker_truth_summary.get("broker_reachable", False))
        snapshot = HealthSnapshot(
            market_data_ok=market_data_ok,
            broker_ok=broker_ok,
            persistence_ok=True,
            reconciliation_clean=not bool(broker_truth_summary.get("requires_reconciliation")),
            invariants_ok=self._strategy_engine.state.fault_code is None,
            health_status=HealthStatus.HEALTHY,
        )
        return HealthSnapshot(
            market_data_ok=snapshot.market_data_ok,
            broker_ok=snapshot.broker_ok,
            persistence_ok=snapshot.persistence_ok,
            reconciliation_clean=snapshot.reconciliation_clean,
            invariants_ok=snapshot.invariants_ok,
            health_status=derive_health_status(snapshot),
        )


def build_probationary_shadow_runner(
    config_paths: Sequence[str | Path],
    schwab_config_path: str | Path | None,
) -> ProbationaryShadowRunner:
    settings = load_settings_from_files(config_paths)
    repositories = RepositorySet(build_engine(settings.database_url))
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_shadow")
    live_polling_service = _build_live_polling_service(settings, repositories, schwab_config_path)
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=ExecutionEngine(),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        shadow_mode_no_submit=True,
    )
    return ProbationaryShadowRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        live_polling_service=live_polling_service,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=SchwabProductionLinkService(Path(__file__).resolve().parents[3]),
    )


def build_probationary_live_strategy_pilot_runner(
    config_paths: Sequence[str | Path],
    schwab_config_path: str | Path | None,
) -> ProbationaryLiveStrategyPilotRunner:
    settings = load_settings_from_files(config_paths)
    runtime_definitions = build_standalone_strategy_definitions(settings)
    runtime_identity = runtime_definitions[0].runtime_identity if runtime_definitions else None
    repositories = RepositorySet(build_engine(settings.database_url), runtime_identity=runtime_identity)
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(
        structured_logger,
        repositories.alerts,
        source_subsystem="probationary_live_strategy_pilot",
    )
    live_polling_service = _build_live_polling_service(settings, repositories, schwab_config_path)
    broker_truth_service = SchwabProductionLinkService(Path(__file__).resolve().parents[3])
    live_broker = LiveStrategyPilotBroker(
        settings=settings,
        repo_root=Path(__file__).resolve().parents[3],
        production_link_service=broker_truth_service,
    )
    execution_engine = ExecutionEngine(broker=live_broker)
    strategy_engine: StrategyEngine | None = None

    def _submit_gate(bar: Bar, state, intent: OrderIntent) -> str | None:
        del state
        assert strategy_engine is not None
        return _live_strategy_submit_gate_blocker(
            settings=settings,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            broker_truth_service=broker_truth_service,
            bar=bar,
            intent=intent,
        )

    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        runtime_identity=runtime_identity,
        submit_gate_evaluator=_submit_gate,
    )
    return ProbationaryLiveStrategyPilotRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=live_polling_service,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        broker_truth_service=broker_truth_service,
    )


def build_probationary_paper_runner(
    config_paths: Sequence[str | Path],
    schwab_config_path: str | Path | None,
) -> ProbationaryPaperRunner | ProbationaryPaperSupervisor:
    settings = load_settings_from_files(config_paths)
    _run_probationary_runtime_market_data_transport_probe(
        settings=settings,
        schwab_config_path=schwab_config_path,
    )
    structured_logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(structured_logger, source_subsystem="probationary_paper_supervisor")
    lane_specs = _load_probationary_paper_lane_specs(settings)
    if lane_specs:
        lanes = _build_probationary_paper_lanes(
            settings=settings,
            lane_specs=lane_specs,
            root_logger=structured_logger,
            schwab_config_path=schwab_config_path,
        )
        runtime_registry = _build_probationary_runtime_registry(settings, lanes)
        return ProbationaryPaperSupervisor(
            settings=settings,
            lanes=lanes,
            structured_logger=structured_logger,
            alert_dispatcher=alert_dispatcher,
            runtime_registry=runtime_registry,
        )

    runtime_definitions = build_standalone_strategy_definitions(settings)
    runtime_identity = runtime_definitions[0].runtime_identity if runtime_definitions else None
    repositories = RepositorySet(build_engine(settings.database_url), runtime_identity=runtime_identity)
    alert_dispatcher = AlertDispatcher(structured_logger, repositories.alerts, source_subsystem="probationary_paper")
    live_polling_service = _build_live_polling_service(settings, repositories, schwab_config_path)
    execution_engine = ExecutionEngine(broker=PaperBroker())
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        runtime_identity=runtime_identity,
    )
    runtime_registry = StrategyRuntimeRegistry(
        [
            StandaloneStrategyRuntimeInstance(
                definition=runtime_definitions[0],
                settings=settings,
                repositories=repositories,
                strategy_engine=strategy_engine,
                runtime_state_loaded=bool(strategy_engine.state.last_signal_bar_id or strategy_engine.state.last_order_intent_id),
            )
        ]
    )
    return ProbationaryPaperRunner(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=live_polling_service,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        runtime_registry=runtime_registry,
    )


def _load_probationary_paper_lane_specs(settings: StrategySettings) -> tuple[ProbationaryPaperLaneSpec, ...]:
    return _coerce_probationary_paper_lane_specs(_configured_probationary_paper_lane_rows(settings))


def _active_probationary_paper_lane_specs(settings: StrategySettings) -> tuple[ProbationaryPaperLaneSpec, ...]:
    if settings.probationary_paper_runtime_exclusive_config:
        return _load_probationary_paper_lane_specs(settings)
    runtime_payload = _read_json(settings.probationary_artifacts_path / "runtime" / "paper_config_in_force.json")
    runtime_lanes = list(runtime_payload.get("lanes") or [])
    if runtime_lanes:
        configured_by_lane_id = {
            str(row.get("lane_id")): dict(row)
            for row in _configured_probationary_paper_lane_rows(settings)
            if row.get("lane_id")
        }
        merged_rows: list[dict[str, Any]] = []
        seen_lane_ids: set[str] = set()
        for row in runtime_lanes:
            lane_id = str(row.get("lane_id") or "")
            configured = configured_by_lane_id.get(lane_id, {})
            merged_rows.append({**dict(row), **configured})
            if lane_id:
                seen_lane_ids.add(lane_id)
        for lane_id, configured in configured_by_lane_id.items():
            if lane_id not in seen_lane_ids:
                merged_rows.append(dict(configured))
        return _coerce_probationary_paper_lane_specs(merged_rows)
    return _load_probationary_paper_lane_specs(settings)


def _configured_probationary_paper_lane_rows(settings: StrategySettings) -> list[dict[str, Any]]:
    raw_specs = list(settings.probationary_paper_lane_specs)
    if not settings.probationary_paper_runtime_exclusive_config:
        raw_specs.extend(_approved_quant_probationary_paper_lane_rows())
    canary_spec = settings.probationary_paper_execution_canary_spec
    if canary_spec:
        raw_specs.append(canary_spec)
    if settings.probationary_atpe_canary_enabled:
        raw_specs.extend(_atpe_probationary_paper_lane_rows(settings))
    if settings.probationary_gc_mgc_acceptance_enabled:
        raw_specs.extend(_gc_mgc_acceptance_probationary_paper_lane_rows(settings))
    return raw_specs


def _coerce_probationary_paper_lane_specs(
    raw_specs: Sequence[dict[str, Any]],
) -> tuple[ProbationaryPaperLaneSpec, ...]:
    specs: list[ProbationaryPaperLaneSpec] = []
    seen_lane_ids: set[str] = set()
    for raw_spec in raw_specs:
        lane_id = str(raw_spec["lane_id"])
        if lane_id in seen_lane_ids:
            raise ValueError(f"Duplicate probationary paper lane_id configured: {lane_id}")
        seen_lane_ids.add(lane_id)
        if not raw_spec.get("session_restriction"):
            raise ValueError(f"Probationary paper lane {lane_id} requires an explicit session_restriction.")
        if raw_spec.get("catastrophic_open_loss") is None:
            raise ValueError(f"Probationary paper lane {lane_id} requires an explicit catastrophic_open_loss.")
        specs.append(
            ProbationaryPaperLaneSpec(
                lane_id=lane_id,
                display_name=str(raw_spec.get("display_name") or raw_spec["lane_id"]),
                symbol=str(raw_spec["symbol"]),
                standalone_strategy_id=str(raw_spec["standalone_strategy_id"]) if raw_spec.get("standalone_strategy_id") else None,
                long_sources=tuple(str(value) for value in raw_spec.get("long_sources", [])),
                short_sources=tuple(str(value) for value in raw_spec.get("short_sources", [])),
                session_restriction=(
                    str(raw_spec["session_restriction"]) if raw_spec.get("session_restriction") else None
                ),
                point_value=Decimal(str(raw_spec["point_value"])),
                trade_size=int(raw_spec.get("trade_size", 1)),
                participation_policy=ParticipationPolicy(
                    str(raw_spec.get("participation_policy") or ParticipationPolicy.SINGLE_ENTRY_ONLY.value)
                ),
                max_concurrent_entries=max(1, int(raw_spec.get("max_concurrent_entries", 1))),
                max_position_quantity=(
                    int(raw_spec["max_position_quantity"])
                    if raw_spec.get("max_position_quantity") is not None
                    else None
                ),
                max_adds_after_entry=max(0, int(raw_spec.get("max_adds_after_entry", 0))),
                add_direction_policy=AddDirectionPolicy(
                    str(raw_spec.get("add_direction_policy") or AddDirectionPolicy.SAME_DIRECTION_ONLY.value)
                ),
                catastrophic_open_loss=(
                    Decimal(str(raw_spec["catastrophic_open_loss"]))
                    if raw_spec.get("catastrophic_open_loss") is not None
                    else None
                ),
                lane_mode=str(raw_spec.get("lane_mode") or "STANDARD"),
                strategy_family=str(raw_spec.get("strategy_family") or raw_spec.get("family") or "UNKNOWN"),
                strategy_identity_root=(
                    str(raw_spec["strategy_identity_root"]) if raw_spec.get("strategy_identity_root") else None
                ),
                runtime_kind=str(raw_spec.get("runtime_kind") or "strategy_engine"),
                allowed_sessions=tuple(str(value) for value in raw_spec.get("allowed_sessions", []) if value),
                live_poll_lookback_minutes=(
                    int(raw_spec["live_poll_lookback_minutes"])
                    if raw_spec.get("live_poll_lookback_minutes") is not None
                    else None
                ),
                database_url=str(raw_spec["database_url"]) if raw_spec.get("database_url") else None,
                artifacts_dir=str(raw_spec["artifacts_dir"]) if raw_spec.get("artifacts_dir") else None,
                canary_entry_not_before_et=(
                    str(raw_spec["canary_entry_not_before_et"])
                    if raw_spec.get("canary_entry_not_before_et")
                    else None
                ),
                canary_entry_window_end_et=(
                    str(raw_spec["canary_entry_window_end_et"])
                    if raw_spec.get("canary_entry_window_end_et")
                    else None
                ),
                canary_exit_not_before_et=(
                    str(raw_spec["canary_exit_not_before_et"])
                    if raw_spec.get("canary_exit_not_before_et")
                    else None
                ),
                canary_max_entries_per_session=max(1, int(raw_spec.get("canary_max_entries_per_session", 1))),
                canary_one_shot_per_session=bool(raw_spec.get("canary_one_shot_per_session", False)),
                observed_instruments=tuple(str(value) for value in raw_spec.get("observed_instruments", []) if value),
                quality_bucket_policy=(
                    str(raw_spec["quality_bucket_policy"]) if raw_spec.get("quality_bucket_policy") else None
                ),
                experimental_status=(
                    str(raw_spec["experimental_status"]) if raw_spec.get("experimental_status") else None
                ),
                paper_only=bool(raw_spec.get("paper_only", False)),
                non_approved=bool(raw_spec.get("non_approved", False)),
                observer_variant_id=(
                    str(raw_spec["observer_variant_id"]) if raw_spec.get("observer_variant_id") else None
                ),
                observer_side=str(raw_spec["observer_side"]) if raw_spec.get("observer_side") else None,
                identity_components=tuple(str(value) for value in raw_spec.get("identity_components", []) if value),
                shared_strategy_identity=(
                    str(raw_spec["shared_strategy_identity"]) if raw_spec.get("shared_strategy_identity") else None
                ),
            )
        )
    return tuple(specs)


def _atpe_probationary_paper_lane_rows(settings: StrategySettings) -> list[dict[str, Any]]:
    canary_root = _atpe_canary_root_dir()
    runtime_dir = canary_root / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    instruments = list(settings.probationary_atpe_canary_instruments or ("MES", "MNQ"))
    rows: list[dict[str, Any]] = []
    for lane in _CANARY_LANES:
        for instrument in instruments:
            normalized_instrument = str(instrument).strip().upper()
            lane_id = atpe_runtime_lane_id(lane, normalized_instrument)
            lane_dir = canary_root / "lanes" / lane_id
            lane_dir.mkdir(parents=True, exist_ok=True)
            point_value = Decimal(str(ATPE_POINT_VALUES.get(normalized_instrument, 1.0)))
            rows.append(
                {
                    "lane_id": lane_id,
                    "display_name": atpe_runtime_lane_name(lane, normalized_instrument),
                    "symbol": normalized_instrument,
                    "long_sources": [lane.variant_id] if lane.side == "LONG" else [],
                    "short_sources": [lane.variant_id] if lane.side == "SHORT" else [],
                    "session_restriction": "ASIA/LONDON/US",
                    "allowed_sessions": ["ASIA", "LONDON", "US"],
                    "point_value": str(point_value),
                    "trade_size": 1,
                    "catastrophic_open_loss": "-500",
                    "lane_mode": ATPE_CANARY_LANE_MODE,
                    "strategy_family": ATPE_CANARY_SOURCE_FAMILY,
                    "strategy_identity_root": lane.lane_name,
                    "runtime_kind": ATPE_CANARY_RUNTIME_KIND,
                    "live_poll_lookback_minutes": settings.probationary_atpe_canary_live_poll_lookback_minutes,
                    "database_url": f"sqlite:///{runtime_dir / f'{lane_id}.sqlite3'}",
                    "artifacts_dir": str(lane_dir),
                    "observed_instruments": [normalized_instrument],
                    "quality_bucket_policy": lane.quality_bucket_policy,
                    "experimental_status": lane.experimental_status,
                    "paper_only": True,
                    "non_approved": True,
                    "observer_variant_id": lane.variant_id,
                    "observer_side": lane.side,
                }
            )
    return rows


def _gc_mgc_acceptance_probationary_paper_lane_rows(settings: StrategySettings) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for instrument in ("GC", "MGC"):
        lane_id = f"{GC_MGC_ACCEPTANCE_SOURCE_FAMILY}__{instrument}"
        point_value = APPROVED_QUANT_POINT_VALUES.get(instrument, Decimal("1"))
        catastrophic_open_loss = Decimal("-1000") if point_value > Decimal("10") else Decimal("-500")
        rows.append(
            {
                "lane_id": lane_id,
                "display_name": f"GC/MGC London-Open Acceptance Continuation Long / {instrument}",
                "symbol": instrument,
                "long_sources": [GC_MGC_ACCEPTANCE_SOURCE_FAMILY],
                "short_sources": [],
                "session_restriction": "LONDON_OPEN",
                "allowed_sessions": ["LONDON_OPEN"],
                "point_value": str(point_value),
                "trade_size": 1,
                "catastrophic_open_loss": str(catastrophic_open_loss),
                "lane_mode": GC_MGC_ACCEPTANCE_LANE_MODE,
                "strategy_family": GC_MGC_ACCEPTANCE_SOURCE_FAMILY,
                "strategy_identity_root": GC_MGC_ACCEPTANCE_SOURCE_FAMILY,
                "runtime_kind": GC_MGC_ACCEPTANCE_RUNTIME_KIND,
                "live_poll_lookback_minutes": settings.probationary_gc_mgc_acceptance_live_poll_lookback_minutes,
                "experimental_status": "experimental_temp_paper",
                "paper_only": True,
                "non_approved": True,
                "observer_variant_id": GC_MGC_ACCEPTANCE_SOURCE_FAMILY,
                "observer_side": "LONG",
            }
        )
    return rows


def _approved_quant_probationary_paper_lane_rows() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for spec in approved_quant_lane_specs():
        for instrument in spec.symbols:
            rows.append(_approved_quant_probationary_paper_lane_row(spec=spec, instrument=instrument))
    return rows


def _approved_quant_probationary_paper_lane_row(
    *,
    spec: ApprovedQuantLaneSpec,
    instrument: str,
) -> dict[str, Any]:
    lane_id = f"{spec.lane_name}__{str(instrument).upper()}"
    point_value = APPROVED_QUANT_POINT_VALUES.get(str(instrument).upper(), Decimal("1"))
    catastrophic_open_loss = Decimal("-2500")
    if point_value <= Decimal("10"):
        catastrophic_open_loss = Decimal("-500")
    elif point_value <= Decimal("100"):
        catastrophic_open_loss = Decimal("-1000")
    return {
        "lane_id": lane_id,
        "display_name": f"{spec.lane_name} / {instrument}",
        "symbol": str(instrument).upper(),
        "long_sources": [spec.family] if str(spec.direction).upper() == "LONG" else [],
        "short_sources": [spec.family] if str(spec.direction).upper() == "SHORT" else [],
        "session_restriction": "/".join(spec.allowed_sessions),
        "allowed_sessions": list(spec.allowed_sessions),
        "point_value": str(point_value),
        "trade_size": 1,
        "catastrophic_open_loss": str(catastrophic_open_loss),
        "strategy_family": spec.family,
        "strategy_identity_root": spec.lane_name,
        "runtime_kind": "approved_quant_strategy_engine",
        "live_poll_lookback_minutes": APPROVED_QUANT_RUNTIME_LOOKBACK_MINUTES,
    }


def _build_probationary_paper_lanes(
    *,
    settings: StrategySettings,
    lane_specs: Sequence[ProbationaryPaperLaneSpec],
    root_logger: StructuredLogger,
    schwab_config_path: str | Path | None,
) -> list[ProbationaryPaperLaneRuntime]:
    lanes: list[ProbationaryPaperLaneRuntime] = []
    runtime_definitions = {
        definition.lane_id: definition
        for definition in build_standalone_strategy_definitions(
            settings,
            runtime_lanes=[
                {
                    "lane_id": spec.lane_id,
                    "display_name": spec.display_name,
                    "symbol": spec.symbol,
                    "standalone_strategy_id": spec.standalone_strategy_id,
                    "strategy_family": spec.strategy_family,
                    "strategy_identity_root": spec.strategy_identity_root,
                    "identity_components": list(spec.identity_components),
                    "runtime_kind": spec.runtime_kind,
                    "long_sources": list(spec.long_sources),
                    "short_sources": list(spec.short_sources),
                    "session_restriction": spec.session_restriction,
                    "allowed_sessions": list(spec.allowed_sessions),
                    "trade_size": spec.trade_size,
                    "participation_policy": spec.participation_policy.value,
                    "max_concurrent_entries": spec.max_concurrent_entries,
                    "max_position_quantity": spec.max_position_quantity,
                    "max_adds_after_entry": spec.max_adds_after_entry,
                    "add_direction_policy": spec.add_direction_policy.value,
                    "database_url": spec.database_url or _derive_probationary_lane_database_url(settings.database_url, spec.lane_id),
                    "artifacts_dir": spec.artifacts_dir or str(settings.probationary_artifacts_path / "lanes" / spec.lane_id),
                    "observed_instruments": list(spec.observed_instruments),
                    "quality_bucket_policy": spec.quality_bucket_policy,
                    "experimental_status": spec.experimental_status,
                    "paper_only": spec.paper_only,
                    "non_approved": spec.non_approved,
                    "observer_variant_id": spec.observer_variant_id,
                    "observer_side": spec.observer_side,
                }
                for spec in lane_specs
            ],
        )
    }
    for spec in lane_specs:
        lane_settings = _build_probationary_paper_lane_settings(settings, spec)
        runtime_definition = runtime_definitions.get(spec.lane_id)
        runtime_identity = runtime_definition.runtime_identity if runtime_definition is not None else None
        repositories = RepositorySet(build_engine(lane_settings.database_url), runtime_identity=runtime_identity)
        lane_logger = ProbationaryLaneStructuredLogger(
            lane_id=spec.lane_id,
            symbol=spec.symbol,
            root_logger=root_logger,
            lane_logger=StructuredLogger(lane_settings.probationary_artifacts_path),
        )
        alert_dispatcher = AlertDispatcher(lane_logger, repositories.alerts, source_subsystem="probationary_paper_lane")
        execution_engine = ExecutionEngine(broker=PaperBroker())
        strategy_engine = _build_probationary_strategy_engine(
            spec=spec,
            settings=lane_settings,
            repositories=repositories,
            execution_engine=execution_engine,
            structured_logger=lane_logger,
            alert_dispatcher=alert_dispatcher,
            runtime_identity=runtime_identity,
        )
        if spec.runtime_kind == ATPE_CANARY_RUNTIME_KIND:
            live_services = {
                instrument: _build_live_polling_service(
                    lane_settings.model_copy(update={"symbol": instrument}),
                    repositories,
                    schwab_config_path,
                )
                for instrument in (spec.observed_instruments or settings.probationary_atpe_canary_instruments or ("MES", "MNQ"))
            }
            selected_variant = next(
                variant
                for variant in default_pattern_variants(profile="phase3_full")
                if variant.variant_id == spec.observer_variant_id
            )
            lanes.append(
                ProbationaryAtpeCanaryLaneRuntime(
                    spec=spec,
                    settings=lane_settings,
                    repositories=repositories,
                    strategy_engine=strategy_engine,
                    execution_engine=execution_engine,
                    live_polling_service=next(iter(live_services.values())),
                    live_polling_services_by_instrument=live_services,
                    structured_logger=lane_logger,
                    alert_dispatcher=alert_dispatcher,
                    observed_instruments=spec.observed_instruments or settings.probationary_atpe_canary_instruments,
                    variant=selected_variant,
                )
            )
            continue
        if spec.runtime_kind == ATP_COMPANION_BENCHMARK_RUNTIME_KIND:
            lanes.append(
                ProbationaryAtpCompanionBenchmarkLaneRuntime(
                    spec=spec,
                    settings=lane_settings,
                    repositories=repositories,
                    strategy_engine=strategy_engine,
                    execution_engine=execution_engine,
                    live_polling_service=_build_live_polling_service(
                        lane_settings.model_copy(update={"symbol": next(iter(spec.observed_instruments), lane_settings.symbol)}),
                        repositories,
                        schwab_config_path,
                    ),
                    structured_logger=lane_logger,
                    alert_dispatcher=alert_dispatcher,
                    observed_instruments=spec.observed_instruments or (lane_settings.symbol,),
                )
            )
            continue
        if spec.runtime_kind == GC_MGC_ACCEPTANCE_RUNTIME_KIND:
            lanes.append(
                ProbationaryTemporaryPaperLaneRuntime(
                    spec=spec,
                    settings=lane_settings,
                    repositories=repositories,
                    strategy_engine=strategy_engine,
                    execution_engine=execution_engine,
                    live_polling_service=_build_live_polling_service(lane_settings, repositories, schwab_config_path),
                    structured_logger=lane_logger,
                    alert_dispatcher=alert_dispatcher,
                )
            )
            continue
        lanes.append(
            ProbationaryPaperLaneRuntime(
                spec=spec,
                settings=lane_settings,
                repositories=repositories,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
                live_polling_service=_build_live_polling_service(lane_settings, repositories, schwab_config_path),
                structured_logger=lane_logger,
                alert_dispatcher=alert_dispatcher,
            )
        )
    return lanes


def _build_probationary_runtime_registry(
    settings: StrategySettings,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
) -> StrategyRuntimeRegistry:
    definitions = {
        definition.lane_id: definition
        for definition in build_standalone_strategy_definitions(
            settings,
            runtime_lanes=[
                {
                    "lane_id": lane.spec.lane_id,
                    "display_name": lane.spec.display_name,
                    "symbol": lane.spec.symbol,
                    "strategy_family": getattr(lane.spec, "strategy_family", "UNKNOWN"),
                    "strategy_identity_root": getattr(lane.spec, "strategy_identity_root", None),
                    "runtime_kind": getattr(lane.spec, "runtime_kind", "strategy_engine"),
                    "long_sources": list(lane.spec.long_sources),
                    "short_sources": list(lane.spec.short_sources),
                    "session_restriction": lane.spec.session_restriction,
                    "allowed_sessions": list(getattr(lane.spec, "allowed_sessions", ()) or ()),
                    "trade_size": lane.spec.trade_size,
                    "database_url": lane.settings.database_url,
                    "artifacts_dir": str(lane.settings.probationary_artifacts_path),
                    **_lane_config_row_extras(lane),
                }
                for lane in lanes
            ],
        )
    }
    return StrategyRuntimeRegistry(
        [
            StandaloneStrategyRuntimeInstance(
                definition=definitions.get(lane.spec.lane_id),
                settings=lane.settings,
                repositories=lane.repositories,
                strategy_engine=lane.strategy_engine,
                runtime_state_loaded=bool(
                    lane.strategy_engine.state.last_signal_bar_id or lane.strategy_engine.state.last_order_intent_id
                ),
            )
            for lane in lanes
            if definitions.get(lane.spec.lane_id) is not None
        ]
    )


def _build_probationary_strategy_engine(
    *,
    spec: ProbationaryPaperLaneSpec,
    settings: StrategySettings,
    repositories: RepositorySet,
    execution_engine: ExecutionEngine,
    structured_logger: ProbationaryLaneStructuredLogger,
    alert_dispatcher: AlertDispatcher,
    runtime_identity: dict[str, Any] | None,
) -> StrategyEngine:
    if spec.runtime_kind == "approved_quant_strategy_engine":
        quant_spec = next(
            candidate
            for candidate in approved_quant_lane_specs()
            if candidate.family == spec.strategy_family and spec.symbol in candidate.symbols
        )
        return ApprovedQuantStrategyEngine(
            quant_spec=quant_spec,
            settings=settings,
            repositories=repositories,
            execution_engine=execution_engine,
            structured_logger=structured_logger,
            alert_dispatcher=alert_dispatcher,
            runtime_identity=runtime_identity,
        )
    if spec.runtime_kind == GC_MGC_ACCEPTANCE_RUNTIME_KIND:
        return GcMgcLondonOpenAcceptanceContinuationStrategyEngine(
            settings=settings,
            repositories=repositories,
            execution_engine=execution_engine,
            structured_logger=structured_logger,
            alert_dispatcher=alert_dispatcher,
            runtime_identity=runtime_identity,
        )
    return StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
        runtime_identity=runtime_identity,
    )


def _build_probationary_paper_lane_settings(
    settings: StrategySettings,
    spec: ProbationaryPaperLaneSpec,
) -> StrategySettings:
    updates: dict[str, Any] = {
        "symbol": spec.symbol,
        "trade_size": spec.trade_size,
        "participation_policy": spec.participation_policy,
        "max_concurrent_entries": spec.max_concurrent_entries,
        "max_position_quantity": spec.max_position_quantity,
        "max_adds_after_entry": spec.max_adds_after_entry,
        "add_direction_policy": spec.add_direction_policy,
        "database_url": spec.database_url or _derive_probationary_lane_database_url(settings.database_url, spec.lane_id),
        "probationary_artifacts_dir": spec.artifacts_dir or str(settings.probationary_artifacts_path / "lanes" / spec.lane_id),
        "probationary_paper_lane_id": spec.lane_id,
        "probationary_paper_lane_display_name": spec.display_name,
        "probationary_paper_lane_session_restriction": spec.session_restriction or "",
        "enable_us_late_pause_resume_longs": False,
        "enable_asia_early_normal_breakout_retest_hold_longs": False,
        "enable_asia_early_pause_resume_shorts": False,
        "probationary_extra_approved_long_entry_sources_json": "[]",
        "probationary_extra_approved_short_entry_sources_json": "[]",
        "probationary_enforce_approved_branches": True,
    }
    if spec.live_poll_lookback_minutes is not None:
        updates["live_poll_lookback_minutes"] = spec.live_poll_lookback_minutes
    if spec.lane_mode == PAPER_EXECUTION_CANARY_MODE:
        updates.update(
            {
                "use_long_swing_exit": False,
                "use_long_integrity_exit": False,
                "use_long_time_exit": False,
            }
        )
    for source in spec.long_sources:
        field_name = APPROVED_LONG_SOURCE_FIELDS.get(source)
        if field_name is None:
            extra_sources = json.loads(str(updates["probationary_extra_approved_long_entry_sources_json"]))
            extra_sources.append(source)
            updates["probationary_extra_approved_long_entry_sources_json"] = json.dumps(sorted(dict.fromkeys(extra_sources)))
        else:
            updates[field_name] = True
    for source in spec.short_sources:
        field_name = APPROVED_SHORT_SOURCE_FIELDS.get(source)
        if field_name is None:
            extra_sources = json.loads(str(updates["probationary_extra_approved_short_entry_sources_json"]))
            extra_sources.append(source)
            updates["probationary_extra_approved_short_entry_sources_json"] = json.dumps(sorted(dict.fromkeys(extra_sources)))
        else:
            updates[field_name] = True
    return settings.model_copy(update=updates)


def _derive_probationary_lane_database_url(database_url: str, lane_id: str) -> str:
    if not database_url.startswith("sqlite:///"):
        raise ValueError("Probationary paper lanes require sqlite:/// database URLs.")
    raw_path = database_url.removeprefix("sqlite:///")
    path = Path(raw_path)
    if path.name == ":memory:":
        return database_url
    suffix = path.suffix or ".sqlite3"
    derived_path = path.with_name(f"{path.stem}__{lane_id}{suffix}")
    return f"sqlite:///{derived_path}"


def _write_probationary_paper_config_in_force(
    settings: StrategySettings,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
    structured_logger: StructuredLogger,
) -> Path:
    runtime_dir = settings.probationary_artifacts_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "desk_halt_new_entries_loss": str(settings.probationary_paper_desk_halt_new_entries_loss),
        "desk_flatten_and_halt_loss": str(settings.probationary_paper_desk_flatten_and_halt_loss),
        "lane_realized_loser_limit_per_session": settings.probationary_paper_lane_realized_loser_limit_per_session,
        "canary_force_fire_once_token": _normalize_canary_force_fire_token(
            settings.probationary_paper_execution_canary_force_fire_once_token
        )
        or None,
        "lane_warning_open_loss": {
            key: str(value) for key, value in settings.probationary_paper_lane_warning_open_loss.items()
        },
        "lanes": [
            {
                "lane_id": lane.spec.lane_id,
                "display_name": lane.spec.display_name,
                "symbol": lane.spec.symbol,
                "source_family": getattr(lane.spec, "strategy_family", "UNKNOWN"),
                "strategy_family": getattr(lane.spec, "strategy_family", "UNKNOWN"),
                "strategy_identity_root": getattr(lane.spec, "strategy_identity_root", None),
                "runtime_kind": getattr(lane.spec, "runtime_kind", "strategy_engine"),
                "long_sources": list(lane.spec.long_sources),
                "short_sources": list(lane.spec.short_sources),
                "session_restriction": lane.spec.session_restriction,
                "allowed_sessions": list(getattr(lane.spec, "allowed_sessions", ()) or ()),
                "lane_mode": getattr(lane.spec, "lane_mode", "STANDARD"),
                "point_value": str(lane.spec.point_value),
                "trade_size": getattr(lane.spec, "trade_size", lane.settings.trade_size),
                "participation_policy": lane.settings.participation_policy.value,
                "max_concurrent_entries": lane.settings.max_concurrent_entries,
                "max_position_quantity": lane.settings.max_position_quantity,
                "max_adds_after_entry": lane.settings.max_adds_after_entry,
                "add_direction_policy": lane.settings.add_direction_policy.value,
                "catastrophic_open_loss": (
                    str(lane.spec.catastrophic_open_loss) if lane.spec.catastrophic_open_loss is not None else None
                ),
                "canary_entry_not_before_et": getattr(lane.spec, "canary_entry_not_before_et", None),
                "canary_entry_window_end_et": getattr(lane.spec, "canary_entry_window_end_et", None),
                "canary_exit_not_before_et": getattr(lane.spec, "canary_exit_not_before_et", None),
                "canary_max_entries_per_session": getattr(lane.spec, "canary_max_entries_per_session", 1),
                "canary_one_shot_per_session": getattr(lane.spec, "canary_one_shot_per_session", False),
                "canary_force_fire_once_active": (
                    bool(
                        _normalize_canary_force_fire_token(
                            lane.settings.probationary_paper_execution_canary_force_fire_once_token
                        )
                    )
                    if getattr(lane.spec, "lane_mode", "STANDARD") == PAPER_EXECUTION_CANARY_MODE
                    else False
                ),
                "database_url": lane.settings.database_url,
                "artifacts_dir": str(lane.settings.probationary_artifacts_path),
                "live_poll_lookback_minutes": lane.settings.live_poll_lookback_minutes,
                **_lane_config_row_extras(lane),
            }
            for lane in lanes
        ],
    }
    return structured_logger._write_json(runtime_dir / "paper_config_in_force.json", payload)  # noqa: SLF001


def _load_probationary_paper_risk_state(settings: StrategySettings) -> ProbationaryPaperRiskRuntimeState:
    runtime_dir = settings.probationary_artifacts_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    path = runtime_dir / "paper_risk_runtime_state.json"
    payload = _read_json(path)
    if not payload:
        return ProbationaryPaperRiskRuntimeState(
            session_date=datetime.now(settings.timezone_info).date().isoformat(),
        )
    return ProbationaryPaperRiskRuntimeState(
        session_date=str(payload.get("session_date") or datetime.now(settings.timezone_info).date().isoformat()),
        desk_halt_new_entries_triggered=bool(payload.get("desk_halt_new_entries_triggered", False)),
        desk_flatten_and_halt_triggered=bool(payload.get("desk_flatten_and_halt_triggered", False)),
        desk_last_trigger_reason=payload.get("desk_last_trigger_reason"),
        desk_last_triggered_at=payload.get("desk_last_triggered_at"),
        desk_last_cleared_at=payload.get("desk_last_cleared_at"),
        desk_last_cleared_action=payload.get("desk_last_cleared_action"),
        lane_states=dict(payload.get("lane_states") or {}),
    )


def _ensure_probationary_paper_risk_state_session(
    risk_state: ProbationaryPaperRiskRuntimeState,
    session_date: date,
) -> ProbationaryPaperRiskRuntimeState:
    resolved = session_date.isoformat()
    if risk_state.session_date == resolved:
        return risk_state
    lane_states: dict[str, dict[str, Any]] = {}
    for lane_id, state in risk_state.lane_states.items():
        next_state = dict(state)
        if next_state.get("session_override_active"):
            next_state["session_override_active"] = False
            next_state["session_override_expired_at"] = datetime.now(timezone.utc).isoformat()
            next_state["session_override_expired_reason"] = "session_reset"
            next_state.pop("session_override_session_date", None)
            next_state.pop("session_override_reason", None)
            next_state.pop("session_override_applied_at", None)
            next_state.pop("session_override_applied_by", None)
            next_state.pop("session_override_note", None)
            next_state.pop("session_override_confirmed", None)
        if str(next_state.get("halt_reason") or "") == REALIZED_LOSER_SESSION_OVERRIDE_REASON:
            cleared_at = datetime.now(timezone.utc).isoformat()
            next_state["degradation_triggered"] = False
            next_state["risk_state"] = "OK"
            next_state["halt_reason"] = None
            next_state["unblock_action"] = "No action needed; session reset auto-cleared stale halt"
            next_state["last_cleared_at"] = cleared_at
            next_state["last_cleared_action"] = "session_reset_auto_clear"
            next_state["session_reset_auto_cleared"] = True
            next_state["session_reset_auto_cleared_at"] = cleared_at
        else:
            next_state["session_reset_auto_cleared"] = False
        lane_states[lane_id] = next_state
    return ProbationaryPaperRiskRuntimeState(
        session_date=resolved,
        desk_halt_new_entries_triggered=risk_state.desk_halt_new_entries_triggered,
        desk_flatten_and_halt_triggered=risk_state.desk_flatten_and_halt_triggered,
        desk_last_trigger_reason=risk_state.desk_last_trigger_reason,
        desk_last_triggered_at=risk_state.desk_last_triggered_at,
        desk_last_cleared_at=risk_state.desk_last_cleared_at,
        desk_last_cleared_action=risk_state.desk_last_cleared_action,
        lane_states=lane_states,
    )


def _resolve_probationary_supervisor_session_date(
    settings: StrategySettings,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
) -> date:
    latest_ts = _latest_probationary_lane_processed_ts(lanes)
    if latest_ts is None:
        return datetime.now(settings.timezone_info).date()
    return datetime.fromisoformat(latest_ts).astimezone(settings.timezone_info).date()


def _latest_probationary_lane_processed_ts(lanes: Sequence[ProbationaryPaperLaneRuntime]) -> str | None:
    candidates = [
        lane.repositories.processed_bars.latest_end_ts()
        for lane in lanes
        if lane.repositories.processed_bars.latest_end_ts() is not None
    ]
    if not candidates:
        return None
    return max(candidates).isoformat()


def _latest_completed_probationary_bar_end(now: datetime, timeframe: str) -> datetime:
    minutes = timeframe_minutes(timeframe)
    local_now = now
    minute = local_now.minute - (local_now.minute % minutes)
    return local_now.replace(minute=minute, second=0, microsecond=0)


def _session_restriction_matches_phase(current_phase: str, restriction: str | None) -> bool:
    normalized = str(restriction or "").upper()
    if not normalized:
        return True
    if "/" in normalized:
        allowed = {part.strip() for part in normalized.split("/") if part.strip()}
        coarse = _phase_coarse_session_group(current_phase)
        return coarse in allowed or current_phase in allowed
    if normalized == "ASIA_EARLY":
        return current_phase == "ASIA_EARLY"
    if normalized == "US_LATE":
        return current_phase == "US_LATE"
    if normalized == "US_EARLY_OBSERVATION":
        return current_phase in {"US_PREOPEN_OPENING", "US_CASH_OPEN_IMPULSE", "US_OPEN_LATE"}
    return current_phase == normalized


def _gc_mgc_asia_retest_hold_london_open_extension_matches(*, symbol: str, long_sources: Sequence[str], end_ts: datetime) -> bool:
    if str(symbol or "").upper() not in {"GC", "MGC"}:
        return False
    if "asiaEarlyNormalBreakoutRetestHoldTurn" not in {str(source) for source in long_sources}:
        return False
    if label_session_phase(end_ts) != "LONDON_OPEN":
        return False
    local_time = end_ts.timetz().replace(tzinfo=None)
    return local_time in {dt_time(3, 5), dt_time(3, 10), dt_time(3, 15)}


def _phase_coarse_session_group(current_phase: str) -> str:
    normalized = str(current_phase or "").upper()
    if normalized.startswith("ASIA_"):
        return "ASIA"
    if normalized.startswith("LONDON_"):
        return "LONDON"
    if normalized.startswith("US_"):
        return "US"
    return "UNKNOWN"


def _probationary_lane_eligibility_snapshot(
    *,
    lane: ProbationaryPaperLaneRuntime,
    risk_state: ProbationaryPaperRiskRuntimeState,
    now: datetime,
) -> dict[str, Any]:
    eligibility_hook = getattr(lane, "eligibility_snapshot", None)
    if callable(eligibility_hook):
        return dict(eligibility_hook(now))
    last_processed_end = lane.repositories.processed_bars.latest_end_ts()
    current_session = label_session_phase(now)
    latest_completed_bar_end = _latest_completed_probationary_bar_end(now, lane.settings.timeframe)
    warmup_required = lane.settings.warmup_bars_required()
    warmup_bars_loaded = len(lane.strategy_engine._bar_history)  # noqa: SLF001 - operator status needs runtime truth
    warmup_complete = warmup_bars_loaded >= warmup_required
    lane_risk_state = str(risk_state.lane_states.get(lane.spec.lane_id, {}).get("risk_state", "OK") or "OK")
    session_allowed = _session_restriction_matches_phase(current_session, lane.spec.session_restriction)
    if not session_allowed and _gc_mgc_asia_retest_hold_london_open_extension_matches(
        symbol=lane.spec.symbol,
        long_sources=getattr(lane.spec, "long_sources", ()),
        end_ts=latest_completed_bar_end,
    ):
        session_allowed = True
    no_new_completed_bar = last_processed_end is None or last_processed_end < latest_completed_bar_end
    state = lane.strategy_engine.state
    eligible_now = True
    blocker_reason = None
    blocker_detail = None

    if state.fault_code is not None:
        eligible_now = False
        blocker_reason = "fault"
        blocker_detail = state.fault_code
    elif lane_risk_state.startswith("HALTED"):
        eligible_now = False
        blocker_reason = "lane_specific_risk_halt"
        blocker_detail = risk_state.lane_states.get(lane.spec.lane_id, {}).get("halt_reason")
    elif state.operator_halt:
        eligible_now = False
        blocker_reason = "operator_halt"
    elif not state.entries_enabled:
        eligible_now = False
        blocker_reason = "entries_disabled"
    elif not session_allowed:
        eligible_now = False
        blocker_reason = "wrong_session"
    elif not warmup_complete:
        eligible_now = False
        blocker_reason = "warmup_incomplete"
        blocker_detail = f"{warmup_bars_loaded}/{warmup_required}"
    elif state.strategy_status is not StrategyStatus.READY or state.position_side is not PositionSide.FLAT:
        eligible_now = False
        blocker_reason = "strategy_not_ready"
        blocker_detail = state.strategy_status.value
    elif no_new_completed_bar:
        eligible_now = False
        blocker_reason = "no_new_completed_bar"

    return {
        "current_detected_session": current_session,
        "eligible_now": eligible_now,
        "eligibility_reason": blocker_reason,
        "eligibility_detail": blocker_detail,
        "allowed_session_match": session_allowed,
        "warmup_complete": warmup_complete,
        "warmup_bars_loaded": warmup_bars_loaded,
        "warmup_bars_required": warmup_required,
        "latest_completed_bar_end_ts": latest_completed_bar_end.isoformat(),
        "last_processed_bar_end_ts": last_processed_end.isoformat() if last_processed_end is not None else None,
    }


def _lane_config_row_extras(lane: ProbationaryPaperLaneRuntime) -> dict[str, Any]:
    hook = getattr(lane, "config_row_extras", None)
    if callable(hook):
        return dict(hook())
    return {}


def _lane_status_row_extras(lane: ProbationaryPaperLaneRuntime) -> dict[str, Any]:
    hook = getattr(lane, "supervisor_status_extras", None)
    if callable(hook):
        return dict(hook())
    return {}


def _build_probationary_paper_lane_metrics(
    lane: ProbationaryPaperLaneRuntime,
    session_date: date,
) -> ProbationaryPaperLaneMetrics:
    order_intents = _load_table_rows_for_session_date(
        lane.repositories.engine,
        order_intents_table,
        timestamp_column="created_at",
        session_date=session_date,
        timezone_info=lane.settings.timezone_info,
    )
    fills = _load_table_rows_for_session_date(
        lane.repositories.engine,
        fills_table,
        timestamp_column="fill_timestamp",
        session_date=session_date,
        timezone_info=lane.settings.timezone_info,
    )
    bars = _load_bars_for_session_date(lane.repositories.engine, session_date, lane.settings)
    session_lookup = build_session_lookup(bars)
    ledger = build_trade_ledger(
        order_intents,
        fills,
        session_lookup,
        point_value=lane.spec.point_value,
        fee_per_fill=Decimal("0"),
        slippage_per_fill=Decimal("0"),
        bars=bars,
    )
    summary = build_summary_metrics(ledger)
    last_mark = _load_latest_probationary_live_mark(lane.repositories.engine, lane.settings.symbol)
    unrealized_pnl = _compute_probationary_unrealized_pnl(
        state=lane.strategy_engine.state,
        last_mark=last_mark,
        point_value=lane.spec.point_value,
    )
    return ProbationaryPaperLaneMetrics(
        session_date=session_date.isoformat(),
        realized_pnl=summary.total_net_pnl,
        unrealized_pnl=unrealized_pnl,
        total_pnl=summary.total_net_pnl + unrealized_pnl,
        closed_trades=summary.number_of_trades,
        losing_closed_trades=sum(1 for row in ledger if row.net_pnl < 0),
        intent_count=len(order_intents),
        fill_count=len(fills),
        open_order_count=len(_load_open_order_intent_rows(lane.repositories)),
        position_side=lane.strategy_engine.state.position_side.value,
        internal_position_qty=int(lane.strategy_engine.state.internal_position_qty),
        broker_position_qty=int(lane.strategy_engine.state.broker_position_qty),
        open_entry_leg_count=len(lane.strategy_engine.state.open_entry_legs),
        open_add_count=max(0, len(lane.strategy_engine.state.open_entry_legs) - 1),
        additional_entry_allowed=_probationary_lane_can_add_participation(lane),
        entry_price=lane.strategy_engine.state.entry_price,
        last_mark=last_mark,
        last_processed_bar_end_ts=(
            lane.repositories.processed_bars.latest_end_ts().isoformat()
            if lane.repositories.processed_bars.latest_end_ts() is not None
            else None
        ),
    )


def _load_latest_probationary_live_mark(engine, symbol: str) -> Decimal | None:
    with engine.begin() as connection:
        row = connection.execute(
            select(bars_table.c.close)
            .where(bars_table.c.data_source == "schwab_live_poll")
            .where(bars_table.c.symbol == symbol)
            .order_by(bars_table.c.end_ts.desc())
            .limit(1)
        ).first()
    return Decimal(str(row.close)) if row is not None and row.close is not None else None


def _compute_probationary_unrealized_pnl(
    *,
    state,
    last_mark: Decimal | None,
    point_value: Decimal,
) -> Decimal:
    if last_mark is None or state.entry_price is None or state.internal_position_qty <= 0:
        return Decimal("0")
    quantity = Decimal(str(state.internal_position_qty))
    if state.position_side == PositionSide.LONG:
        return (last_mark - state.entry_price) * quantity * point_value
    if state.position_side == PositionSide.SHORT:
        return (state.entry_price - last_mark) * quantity * point_value
    return Decimal("0")


def _probationary_lane_can_add_participation(lane: ProbationaryPaperLaneRuntime) -> bool:
    state = lane.strategy_engine.state
    if state.position_side == PositionSide.FLAT:
        return False
    if state.operator_halt or state.same_underlying_entry_hold or state.fault_code is not None:
        return False
    if state.open_broker_order_id is not None:
        return False
    if lane.settings.participation_policy is ParticipationPolicy.SINGLE_ENTRY_ONLY:
        return False
    if len(state.open_entry_legs) >= lane.settings.max_concurrent_entries:
        return False
    if max(0, len(state.open_entry_legs) - 1) >= lane.settings.max_adds_after_entry:
        return False
    max_position_quantity = lane.settings.max_position_quantity or (
        lane.settings.trade_size * lane.settings.max_concurrent_entries
    )
    return state.internal_position_qty + lane.settings.trade_size <= max_position_quantity


def _apply_probationary_paper_risk_controls(
    *,
    settings: StrategySettings,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
    lane_metrics: dict[str, ProbationaryPaperLaneMetrics],
    risk_state: ProbationaryPaperRiskRuntimeState,
    structured_logger: StructuredLogger,
    alert_dispatcher: AlertDispatcher,
) -> tuple[ProbationaryPaperRiskRuntimeState, list[dict[str, Any]]]:
    now = datetime.now(timezone.utc)
    risk_events: list[dict[str, Any]] = []
    warning_map = settings.probationary_paper_lane_warning_open_loss

    for lane in lanes:
        metrics = lane_metrics[lane.spec.lane_id]
        lane_state = risk_state.lane_states.setdefault(lane.spec.lane_id, {})
        is_canary_lane = lane.spec.lane_mode == PAPER_EXECUTION_CANARY_MODE
        lane_state["session_date"] = metrics.session_date
        lane_state["realized_losing_trades"] = metrics.losing_closed_trades
        lane_state.setdefault("halt_reason", None)
        lane_state.setdefault("risk_state", "OK")
        lane_state.setdefault("unblock_action", "No action needed; already eligible")
        if str(lane_state.get("session_override_session_date") or "") != metrics.session_date:
            lane_state["session_override_active"] = False
            lane_state.pop("session_override_session_date", None)
            lane_state.pop("session_override_reason", None)
            lane_state.pop("session_override_applied_at", None)
            lane_state.pop("session_override_applied_by", None)
            lane_state.pop("session_override_note", None)
            lane_state.pop("session_override_confirmed", None)
        session_override_active = _lane_session_override_active_for_realized_loser(
            lane_state,
            session_date=metrics.session_date,
        )
        if is_canary_lane:
            lane_state["degradation_triggered"] = False
            if lane_state.get("halt_reason") == "lane_realized_loser_limit_per_session":
                lane_state["halt_reason"] = None
            if lane.strategy_engine.state.operator_halt and lane.strategy_engine.state.fault_code is None:
                state = lane.strategy_engine.state
                if (
                    state.position_side == PositionSide.FLAT
                    and state.internal_position_qty == 0
                    and state.broker_position_qty == 0
                    and state.open_broker_order_id is None
                ):
                    lane.strategy_engine.set_operator_halt(now, False)
        if (
            lane.spec.catastrophic_open_loss is not None
            and metrics.unrealized_pnl <= lane.spec.catastrophic_open_loss
            and metrics.position_side != PositionSide.FLAT.value
            and not lane_state.get("catastrophic_triggered", False)
        ):
            _halt_probationary_lane(lane, now)
            flatten_state, flatten_reason = _flatten_probationary_lane(lane, now, "lane_catastrophic_open_loss_cap")
            lane_state.update(
                {
                    "catastrophic_triggered": True,
                    "risk_state": "HALTED_CATASTROPHIC",
                    "halt_reason": "lane_catastrophic_open_loss_cap",
                    "unblock_action": "Manual inspection required",
                    "last_triggered_at": now.isoformat(),
                    "flatten_state": flatten_state,
                }
            )
            risk_events.append(
                _probationary_risk_event(
                    lane_id=lane.spec.lane_id,
                    symbol=lane.spec.symbol,
                    severity="ACTION",
                    event_code="LANE_CATASTROPHIC_OPEN_LOSS_CAP",
                    reason=flatten_reason,
                    threshold=lane.spec.catastrophic_open_loss,
                    observed=metrics.unrealized_pnl,
                    logged_at=now,
                )
            )
            continue

        warning_threshold = warning_map.get(lane.spec.lane_id)
        if (
            warning_threshold is not None
            and metrics.unrealized_pnl <= warning_threshold
            and metrics.position_side != PositionSide.FLAT.value
            and not lane_state.get("warning_triggered", False)
            and not lane_state.get("catastrophic_triggered", False)
        ):
            _halt_probationary_lane(lane, now)
            lane_state.update(
                {
                    "warning_triggered": True,
                    "risk_state": "HALTED_DEGRADATION",
                    "halt_reason": "lane_outsized_open_loss_warning",
                    "unblock_action": "Manual inspection required",
                    "last_triggered_at": now.isoformat(),
                }
            )
            risk_events.append(
                _probationary_risk_event(
                    lane_id=lane.spec.lane_id,
                    symbol=lane.spec.symbol,
                    severity="WATCH",
                    event_code="LANE_OUTSIZED_OPEN_LOSS_WARNING",
                    reason="Lane warning open-loss threshold breached; entries halted for this lane.",
                    threshold=warning_threshold,
                    observed=metrics.unrealized_pnl,
                    logged_at=now,
                )
            )
            continue

        if (
            not is_canary_lane
            and
            metrics.losing_closed_trades >= settings.probationary_paper_lane_realized_loser_limit_per_session
            and not session_override_active
            and not lane_state.get("degradation_triggered", False)
        ):
            _halt_probationary_lane(lane, now)
            lane_state.update(
                {
                    "degradation_triggered": True,
                    "risk_state": "HALTED_DEGRADATION",
                    "halt_reason": "lane_realized_loser_limit_per_session",
                    "unblock_action": SESSION_RESET_AUTO_CLEAR_ACTION,
                    "last_triggered_at": now.isoformat(),
                }
            )
            risk_events.append(
                _probationary_risk_event(
                    lane_id=lane.spec.lane_id,
                    symbol=lane.spec.symbol,
                    severity="WATCH",
                    event_code="LANE_REALIZED_LOSER_LIMIT_PER_SESSION",
                    reason="Lane halted after reaching the session realized-loser limit.",
                    threshold=Decimal(str(settings.probationary_paper_lane_realized_loser_limit_per_session)),
                    observed=Decimal(str(metrics.losing_closed_trades)),
                    logged_at=now,
                )
            )
            continue

        if lane_state.get("catastrophic_triggered"):
            lane_state["risk_state"] = "HALTED_CATASTROPHIC"
        elif lane_state.get("warning_triggered") or lane_state.get("degradation_triggered"):
            lane_state["risk_state"] = "HALTED_DEGRADATION"
        elif session_override_active:
            lane_state["risk_state"] = "OK"
            lane_state["halt_reason"] = None
            lane_state["unblock_action"] = "Session override active for current session"
        elif metrics.unrealized_pnl < 0 or metrics.losing_closed_trades == 1:
            lane_state["risk_state"] = "WATCH"
            lane_state["halt_reason"] = None
            lane_state["unblock_action"] = "No action needed; already eligible"
        else:
            lane_state["risk_state"] = "OK"
            lane_state["halt_reason"] = None
            lane_state["unblock_action"] = "No action needed; already eligible"

    desk_realized = sum((metrics.realized_pnl for metrics in lane_metrics.values()), Decimal("0"))
    desk_unrealized = sum((metrics.unrealized_pnl for metrics in lane_metrics.values()), Decimal("0"))
    desk_total = desk_realized + desk_unrealized
    if (
        desk_total <= settings.probationary_paper_desk_flatten_and_halt_loss
        and not risk_state.desk_flatten_and_halt_triggered
    ):
        risk_state.desk_flatten_and_halt_triggered = True
        risk_state.desk_last_trigger_reason = "desk_flatten_and_halt_loss"
        risk_state.desk_last_triggered_at = now.isoformat()
        for lane in lanes:
            _halt_probationary_lane(lane, now)
            if lane.strategy_engine.state.position_side != PositionSide.FLAT:
                _flatten_probationary_lane(lane, now, "desk_flatten_and_halt_loss")
        risk_events.append(
            _probationary_risk_event(
                lane_id="DESK",
                symbol="DESK",
                severity="ACTION",
                event_code="DESK_FLATTEN_AND_HALT_LOSS",
                reason="Desk-level flatten-and-halt loss threshold breached; flattening open paper exposure.",
                threshold=settings.probationary_paper_desk_flatten_and_halt_loss,
                observed=desk_total,
                logged_at=now,
            )
        )
    elif desk_total <= settings.probationary_paper_desk_halt_new_entries_loss and not risk_state.desk_halt_new_entries_triggered:
        risk_state.desk_halt_new_entries_triggered = True
        risk_state.desk_last_trigger_reason = "desk_halt_new_entries_loss"
        risk_state.desk_last_triggered_at = now.isoformat()
        for lane in lanes:
            _halt_probationary_lane(lane, now)
        risk_events.append(
            _probationary_risk_event(
                lane_id="DESK",
                symbol="DESK",
                severity="WATCH",
                event_code="DESK_HALT_NEW_ENTRIES_LOSS",
                reason="Desk-level halt-new-entries loss threshold breached; new entries halted desk-wide.",
                threshold=settings.probationary_paper_desk_halt_new_entries_loss,
                observed=desk_total,
                logged_at=now,
            )
        )

    return risk_state, risk_events


def _halt_probationary_lane(lane: ProbationaryPaperLaneRuntime, occurred_at: datetime) -> None:
    if not lane.strategy_engine.state.operator_halt:
        lane.strategy_engine.set_operator_halt(occurred_at, True)


def _flatten_probationary_lane(
    lane: ProbationaryPaperLaneRuntime,
    occurred_at: datetime,
    reason_code: str,
) -> tuple[str, str]:
    state = lane.strategy_engine.state
    if state.open_broker_order_id is not None or lane.execution_engine.pending_executions():
        return "rejected_open_order_uncertainty", "Catastrophic flatten blocked by an existing pending paper order."
    if state.position_side == PositionSide.FLAT or state.internal_position_qty <= 0:
        return "complete", "Lane already flat."
    try:
        intent = lane.strategy_engine.submit_operator_flatten_intent(occurred_at, reason_code=reason_code)
    except ValueError as exc:
        return "rejected", str(exc)
    if intent is None:
        return "complete", "Lane already flat."
    return "pending_fill", "Lane flatten intent submitted after a risk threshold breach."


def _probationary_risk_event(
    *,
    lane_id: str,
    symbol: str,
    severity: str,
    event_code: str,
    reason: str,
    threshold: Decimal,
    observed: Decimal,
    logged_at: datetime,
) -> dict[str, Any]:
    return {
        "logged_at": logged_at.isoformat(),
        "lane_id": lane_id,
        "symbol": symbol,
        "severity": severity,
        "event_code": event_code,
        "reason": reason,
        "threshold": str(threshold),
        "observed": str(observed),
    }


def _probationary_desk_risk_summary(
    *,
    settings: StrategySettings,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
    lane_metrics: dict[str, ProbationaryPaperLaneMetrics],
    risk_state: ProbationaryPaperRiskRuntimeState,
) -> dict[str, Any]:
    desk_realized = sum((metrics.realized_pnl for metrics in lane_metrics.values()), Decimal("0"))
    desk_unrealized = sum((metrics.unrealized_pnl for metrics in lane_metrics.values()), Decimal("0"))
    desk_total = desk_realized + desk_unrealized
    reconciliation_clean = all(
        _reconcile_paper_runtime(
            repositories=lane.repositories,
            strategy_engine=lane.strategy_engine,
            execution_engine=lane.execution_engine,
        )["clean"]
        for lane in lanes
    )
    faulted = any(lane.strategy_engine.state.fault_code is not None for lane in lanes)
    desk_state = "OK"
    reason = risk_state.desk_last_trigger_reason
    unblock_action = "No action needed; already eligible"
    if faulted:
        desk_state = "FAULTED"
        reason = "active_fault"
        unblock_action = "Acknowledge / Clear Fault"
    elif not reconciliation_clean:
        desk_state = "DIRTY_RECONCILIATION"
        reason = "dirty_reconciliation"
        unblock_action = "Manual inspection required"
    elif risk_state.desk_flatten_and_halt_triggered:
        desk_state = "FLATTEN_AND_HALT"
        reason = risk_state.desk_last_trigger_reason or "desk_flatten_and_halt_loss"
        unblock_action = "Clear Risk Halts, then Resume Entries"
    elif risk_state.desk_halt_new_entries_triggered:
        desk_state = "HALT_NEW_ENTRIES"
        reason = risk_state.desk_last_trigger_reason or "desk_halt_new_entries_loss"
        unblock_action = "Clear Risk Halts, then Resume Entries"
    return {
        "desk_risk_state": desk_state,
        "desk_realized": desk_realized,
        "desk_unrealized": desk_unrealized,
        "desk_total": desk_total,
        "reconciliation_clean": reconciliation_clean,
        "faulted": faulted,
        "trigger_reason": reason,
        "unblock_action": unblock_action,
    }


def _write_probationary_paper_risk_artifacts(
    *,
    settings: StrategySettings,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
    lane_metrics: dict[str, ProbationaryPaperLaneMetrics],
    risk_state: ProbationaryPaperRiskRuntimeState,
    structured_logger: StructuredLogger,
    risk_events: Sequence[dict[str, Any]],
) -> None:
    runtime_dir = settings.probationary_artifacts_path / "runtime"
    runtime_dir.mkdir(parents=True, exist_ok=True)
    desk_summary = _probationary_desk_risk_summary(
        settings=settings,
        lanes=lanes,
        lane_metrics=lane_metrics,
        risk_state=risk_state,
    )
    structured_logger._write_json(  # noqa: SLF001
        runtime_dir / "paper_risk_runtime_state.json",
        {
            "session_date": risk_state.session_date,
            "desk_halt_new_entries_triggered": risk_state.desk_halt_new_entries_triggered,
            "desk_flatten_and_halt_triggered": risk_state.desk_flatten_and_halt_triggered,
            "desk_last_trigger_reason": risk_state.desk_last_trigger_reason,
            "desk_last_triggered_at": risk_state.desk_last_triggered_at,
            "desk_last_cleared_at": risk_state.desk_last_cleared_at,
            "desk_last_cleared_action": risk_state.desk_last_cleared_action,
            "lane_states": risk_state.lane_states,
        },
    )
    desk_payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "session_date": risk_state.session_date,
        "desk_risk_state": desk_summary["desk_risk_state"],
        "session_realized_pnl": str(desk_summary["desk_realized"]),
        "session_unrealized_pnl": str(desk_summary["desk_unrealized"]),
        "session_total_pnl": str(desk_summary["desk_total"]),
        "desk_halt_new_entries_loss": str(settings.probationary_paper_desk_halt_new_entries_loss),
        "desk_flatten_and_halt_loss": str(settings.probationary_paper_desk_flatten_and_halt_loss),
        "triggered": risk_state.desk_halt_new_entries_triggered or risk_state.desk_flatten_and_halt_triggered,
        "trigger_reason": desk_summary["trigger_reason"],
        "unblock_action_required": desk_summary["unblock_action"],
        "reconciliation_clean": desk_summary["reconciliation_clean"],
        "faulted": desk_summary["faulted"],
        "last_triggered_at": risk_state.desk_last_triggered_at,
        "last_cleared_at": risk_state.desk_last_cleared_at,
        "last_cleared_action": risk_state.desk_last_cleared_action,
    }
    lane_payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "session_date": risk_state.session_date,
        "lanes": [
            {
                "lane_id": lane.spec.lane_id,
                "display_name": lane.spec.display_name,
                "symbol": lane.spec.symbol,
                "session_restriction": lane.spec.session_restriction,
                "risk_state": risk_state.lane_states.get(lane.spec.lane_id, {}).get("risk_state", "OK"),
                "halt_reason": risk_state.lane_states.get(lane.spec.lane_id, {}).get("halt_reason"),
                "unblock_action": risk_state.lane_states.get(lane.spec.lane_id, {}).get("unblock_action"),
                "realized_losing_trades": risk_state.lane_states.get(lane.spec.lane_id, {}).get("realized_losing_trades", 0),
                "degradation_halt_active": bool(risk_state.lane_states.get(lane.spec.lane_id, {}).get("degradation_triggered", False)),
                "warning_halt_active": bool(risk_state.lane_states.get(lane.spec.lane_id, {}).get("warning_triggered", False)),
                "catastrophic_halt_active": bool(risk_state.lane_states.get(lane.spec.lane_id, {}).get("catastrophic_triggered", False)),
                "session_override_active": bool(risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_active", False)),
                "session_override_reason": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_reason"),
                "session_override_session_date": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_session_date"),
                "session_override_applied_at": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_applied_at"),
                "session_override_applied_by": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_applied_by"),
                "session_override_note": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_note"),
                "session_override_confirmed": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_confirmed"),
                "session_scoped_halt": (
                    str(risk_state.lane_states.get(lane.spec.lane_id, {}).get("halt_reason") or "") == REALIZED_LOSER_SESSION_OVERRIDE_REASON
                ),
                "auto_clear_on_session_reset": (
                    str(risk_state.lane_states.get(lane.spec.lane_id, {}).get("halt_reason") or "") == REALIZED_LOSER_SESSION_OVERRIDE_REASON
                ),
                "session_reset_auto_cleared": bool(
                    risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_reset_auto_cleared", False)
                ),
                "session_reset_auto_cleared_at": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_reset_auto_cleared_at"),
                "warning_open_loss_threshold": (
                    str(settings.probationary_paper_lane_warning_open_loss.get(lane.spec.lane_id))
                    if settings.probationary_paper_lane_warning_open_loss.get(lane.spec.lane_id) is not None
                    else None
                ),
                "catastrophic_open_loss_threshold": (
                    str(lane.spec.catastrophic_open_loss) if lane.spec.catastrophic_open_loss is not None else None
                ),
                "session_realized_pnl": str(lane_metrics[lane.spec.lane_id].realized_pnl),
                "session_unrealized_pnl": str(lane_metrics[lane.spec.lane_id].unrealized_pnl),
                "session_total_pnl": str(lane_metrics[lane.spec.lane_id].total_pnl),
                "operator_halt": lane.strategy_engine.state.operator_halt,
                "entries_enabled": lane.strategy_engine.state.entries_enabled,
                "same_underlying_entry_hold": lane.strategy_engine.state.same_underlying_entry_hold,
                "same_underlying_hold_reason": lane.strategy_engine.state.same_underlying_hold_reason,
                "last_triggered_at": risk_state.lane_states.get(lane.spec.lane_id, {}).get("last_triggered_at"),
                "last_cleared_at": risk_state.lane_states.get(lane.spec.lane_id, {}).get("last_cleared_at"),
                "last_cleared_action": risk_state.lane_states.get(lane.spec.lane_id, {}).get("last_cleared_action"),
            }
            for lane in lanes
        ],
    }
    structured_logger._write_json(runtime_dir / "paper_desk_risk_snapshot.json", desk_payload)  # noqa: SLF001
    structured_logger._write_json(runtime_dir / "paper_desk_risk_status.json", desk_payload)  # noqa: SLF001
    structured_logger._write_json(runtime_dir / "paper_lane_risk_snapshot.json", lane_payload)  # noqa: SLF001
    structured_logger._write_json(runtime_dir / "paper_lane_risk_status.json", lane_payload)  # noqa: SLF001
    for event in risk_events:
        for path in (
            settings.probationary_artifacts_path / "risk_trigger_events.jsonl",
            settings.probationary_artifacts_path / "paper_risk_events.jsonl",
        ):
            with path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(event, sort_keys=True))
                handle.write("\n")


def _write_probationary_supervisor_operator_status(
    *,
    settings: StrategySettings,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
    structured_logger: StructuredLogger,
    risk_state: ProbationaryPaperRiskRuntimeState,
    latest_operator_control: dict[str, Any] | None,
    lane_metrics: dict[str, ProbationaryPaperLaneMetrics] | None = None,
) -> Path:
    now_local = datetime.now(settings.timezone_info)
    current_detected_session = label_session_phase(now_local)
    latest_end_ts = _latest_probationary_lane_processed_ts(lanes)
    non_authority_runtime_kinds = {
        ATPE_CANARY_RUNTIME_KIND,
        ATP_COMPANION_BENCHMARK_RUNTIME_KIND,
        GC_MGC_ACCEPTANCE_RUNTIME_KIND,
    }
    executable_lanes = [lane for lane in lanes if getattr(lane.spec, "runtime_kind", "") not in non_authority_runtime_kinds] or list(lanes)
    all_flat = all(lane.strategy_engine.state.position_side == PositionSide.FLAT for lane in lanes)
    broker_ok = all(lane.execution_engine.broker.is_connected() for lane in lanes)
    reconciliation_clean = all(
        _reconcile_paper_runtime(
            repositories=lane.repositories,
            strategy_engine=lane.strategy_engine,
            execution_engine=lane.execution_engine,
        )["clean"]
        for lane in lanes
    )
    resolved_lane_metrics = lane_metrics or {
        lane.spec.lane_id: ProbationaryPaperLaneMetrics(
            session_date=risk_state.session_date,
            realized_pnl=Decimal("0"),
            unrealized_pnl=Decimal("0"),
            total_pnl=Decimal("0"),
            closed_trades=0,
            losing_closed_trades=0,
            intent_count=0,
            fill_count=0,
            open_order_count=0,
            position_side=lane.strategy_engine.state.position_side.value,
            internal_position_qty=int(lane.strategy_engine.state.internal_position_qty),
            broker_position_qty=int(lane.strategy_engine.state.broker_position_qty),
            open_entry_leg_count=len(lane.strategy_engine.state.open_entry_legs),
            open_add_count=max(0, len(lane.strategy_engine.state.open_entry_legs) - 1),
            additional_entry_allowed=_probationary_lane_can_add_participation(lane),
            entry_price=lane.strategy_engine.state.entry_price,
            last_mark=None,
            last_processed_bar_end_ts=None,
        )
        for lane in lanes
    }
    desk_risk = _probationary_desk_risk_summary(
        settings=settings,
        lanes=lanes,
        lane_metrics=resolved_lane_metrics,
        risk_state=risk_state,
    )
    restore_rows = [
        row
        for row in (_lane_status_row_extras(lane).get("startup_restore_validation") for lane in lanes)
        if isinstance(row, dict) and row
    ]
    latest_restore = max(
        restore_rows,
        key=lambda row: str(row.get("restore_completed_at") or row.get("restore_started_at") or ""),
        default=None,
    )
    payload = {
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "health": {
            "market_data_ok": True,
            "broker_ok": broker_ok,
            "persistence_ok": True,
            "reconciliation_clean": reconciliation_clean,
            "invariants_ok": all(lane.strategy_engine.state.fault_code is None for lane in lanes),
            "health_status": (
                "HEALTHY"
                if broker_ok and reconciliation_clean and all(lane.strategy_engine.state.fault_code is None for lane in lanes)
                else "DEGRADED"
            ),
        },
        "last_processed_bar_end_ts": latest_end_ts,
        "current_detected_session": current_detected_session,
        "processed_bars": sum(lane.repositories.processed_bars.count() for lane in lanes),
        "position_side": "FLAT" if all_flat else "MULTI",
        "strategy_status": "RUNNING_MULTI_LANE",
        "fault_code": next((lane.strategy_engine.state.fault_code for lane in executable_lanes if lane.strategy_engine.state.fault_code), None),
        "entries_enabled": all(lane.strategy_engine.state.entries_enabled for lane in executable_lanes),
        "operator_halt": any(lane.strategy_engine.state.operator_halt for lane in executable_lanes),
        "same_underlying_entry_hold": any(lane.strategy_engine.state.same_underlying_entry_hold for lane in executable_lanes),
        "same_underlying_hold_instruments": sorted(
            {
                lane.spec.symbol
                for lane in executable_lanes
                if lane.strategy_engine.state.same_underlying_entry_hold
            }
        ),
        "approved_long_entry_sources": sorted({source for lane in executable_lanes for source in lane.settings.approved_long_entry_sources}),
        "approved_short_entry_sources": sorted({source for lane in executable_lanes for source in lane.settings.approved_short_entry_sources}),
        "paper_lane_count": len(lanes),
        "desk_risk_state": desk_risk["desk_risk_state"],
        "desk_risk_reason": desk_risk["trigger_reason"],
        "desk_unblock_action": desk_risk["unblock_action"],
        "desk_session_realized_pnl": str(desk_risk["desk_realized"]),
        "desk_session_unrealized_pnl": str(desk_risk["desk_unrealized"]),
        "desk_session_total_pnl": str(desk_risk["desk_total"]),
        "desk_halt_new_entries_loss": str(settings.probationary_paper_desk_halt_new_entries_loss),
        "desk_flatten_and_halt_loss": str(settings.probationary_paper_desk_flatten_and_halt_loss),
        "paper_risk_events_path": str(settings.probationary_artifacts_path / "paper_risk_events.jsonl"),
        "desk_risk_runtime_state_path": str(settings.probationary_artifacts_path / "runtime" / "paper_risk_runtime_state.json"),
        "paper_desk_risk_status_path": str(settings.probationary_artifacts_path / "runtime" / "paper_desk_risk_status.json"),
        "paper_lane_risk_status_path": str(settings.probationary_artifacts_path / "runtime" / "paper_lane_risk_status.json"),
        "paper_config_in_force_path": str(settings.probationary_artifacts_path / "runtime" / "paper_config_in_force.json"),
        "latest_operator_control": latest_operator_control,
        "startup_restore_validation_summary": {
            "last_restore_completed_at": latest_restore.get("restore_completed_at") if latest_restore else None,
            "last_restore_result": latest_restore.get("restore_result") if latest_restore else "UNAVAILABLE",
            "safe_cleanup_count": sum(1 for row in restore_rows if row.get("safe_cleanup_applied")),
            "unresolved_issue_count": sum(1 for row in restore_rows if row.get("unresolved_restore_issue")),
            "duplicate_action_prevention_held": (
                all(bool(row.get("duplicate_action_prevention_held")) for row in restore_rows)
                if restore_rows
                else True
            ),
            "rows": restore_rows,
        },
        "lanes": [
            {
                "lane_id": lane.spec.lane_id,
                "display_name": lane.spec.display_name,
                "symbol": lane.spec.symbol,
                "source_family": getattr(lane.spec, "strategy_family", "UNKNOWN"),
                "strategy_family": getattr(lane.spec, "strategy_family", "UNKNOWN"),
                "strategy_identity_root": getattr(lane.spec, "strategy_identity_root", None),
                "runtime_kind": getattr(lane.spec, "runtime_kind", "strategy_engine"),
                "session_restriction": lane.spec.session_restriction,
                "allowed_sessions": list(getattr(lane.spec, "allowed_sessions", ()) or ()),
                "lane_mode": getattr(lane.spec, "lane_mode", "STANDARD"),
                "approved_long_entry_sources": sorted(lane.settings.approved_long_entry_sources),
                "approved_short_entry_sources": sorted(lane.settings.approved_short_entry_sources),
                "participation_policy": lane.settings.participation_policy.value,
                "max_concurrent_entries": lane.settings.max_concurrent_entries,
                "max_position_quantity": lane.settings.max_position_quantity,
                "max_adds_after_entry": lane.settings.max_adds_after_entry,
                "add_direction_policy": lane.settings.add_direction_policy.value,
                "position_side": lane.strategy_engine.state.position_side.value,
                "strategy_status": lane.strategy_engine.state.strategy_status.value,
                "entries_enabled": lane.strategy_engine.state.entries_enabled,
                "operator_halt": lane.strategy_engine.state.operator_halt,
                "same_underlying_entry_hold": lane.strategy_engine.state.same_underlying_entry_hold,
                "same_underlying_hold_reason": lane.strategy_engine.state.same_underlying_hold_reason,
                "fault_code": lane.strategy_engine.state.fault_code,
                "risk_state": risk_state.lane_states.get(lane.spec.lane_id, {}).get("risk_state", "OK"),
                "halt_reason": risk_state.lane_states.get(lane.spec.lane_id, {}).get("halt_reason"),
                "unblock_action": risk_state.lane_states.get(lane.spec.lane_id, {}).get("unblock_action"),
                "realized_losing_trades": risk_state.lane_states.get(lane.spec.lane_id, {}).get("realized_losing_trades", 0),
                "degradation_halt_active": bool(risk_state.lane_states.get(lane.spec.lane_id, {}).get("degradation_triggered", False)),
                "warning_halt_active": bool(risk_state.lane_states.get(lane.spec.lane_id, {}).get("warning_triggered", False)),
                "catastrophic_halt_active": bool(risk_state.lane_states.get(lane.spec.lane_id, {}).get("catastrophic_triggered", False)),
                "session_override_active": bool(risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_active", False)),
                "session_override_reason": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_reason"),
                "session_override_session_date": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_session_date"),
                "session_override_applied_at": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_applied_at"),
                "session_override_applied_by": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_applied_by"),
                "session_override_note": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_note"),
                "session_override_confirmed": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_override_confirmed"),
                "session_scoped_halt": (
                    str(risk_state.lane_states.get(lane.spec.lane_id, {}).get("halt_reason") or "") == REALIZED_LOSER_SESSION_OVERRIDE_REASON
                ),
                "auto_clear_on_session_reset": (
                    str(risk_state.lane_states.get(lane.spec.lane_id, {}).get("halt_reason") or "") == REALIZED_LOSER_SESSION_OVERRIDE_REASON
                ),
                "session_reset_auto_cleared": bool(
                    risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_reset_auto_cleared", False)
                ),
                "session_reset_auto_cleared_at": risk_state.lane_states.get(lane.spec.lane_id, {}).get("session_reset_auto_cleared_at"),
                "session_realized_pnl": str(resolved_lane_metrics[lane.spec.lane_id].realized_pnl),
                "session_unrealized_pnl": str(resolved_lane_metrics[lane.spec.lane_id].unrealized_pnl),
                "session_total_pnl": str(resolved_lane_metrics[lane.spec.lane_id].total_pnl),
                "closed_trades": resolved_lane_metrics[lane.spec.lane_id].closed_trades,
                "intent_count": resolved_lane_metrics[lane.spec.lane_id].intent_count,
                "fill_count": resolved_lane_metrics[lane.spec.lane_id].fill_count,
                "open_order_count": resolved_lane_metrics[lane.spec.lane_id].open_order_count,
                "internal_position_qty": resolved_lane_metrics[lane.spec.lane_id].internal_position_qty,
                "broker_position_qty": resolved_lane_metrics[lane.spec.lane_id].broker_position_qty,
                "open_entry_leg_count": resolved_lane_metrics[lane.spec.lane_id].open_entry_leg_count,
                "open_add_count": resolved_lane_metrics[lane.spec.lane_id].open_add_count,
                "additional_entry_allowed": resolved_lane_metrics[lane.spec.lane_id].additional_entry_allowed,
                "entry_price": (
                    str(resolved_lane_metrics[lane.spec.lane_id].entry_price)
                    if resolved_lane_metrics[lane.spec.lane_id].entry_price is not None
                    else None
                ),
                "entry_timestamp": (
                    lane.strategy_engine.state.entry_timestamp.isoformat()
                    if lane.strategy_engine.state.entry_timestamp is not None
                    else None
                ),
                "last_mark": (
                    str(resolved_lane_metrics[lane.spec.lane_id].last_mark)
                    if resolved_lane_metrics[lane.spec.lane_id].last_mark is not None
                    else None
                ),
                "last_processed_bar_end_ts": resolved_lane_metrics[lane.spec.lane_id].last_processed_bar_end_ts,
                "catastrophic_open_loss_threshold": (
                    str(lane.spec.catastrophic_open_loss) if lane.spec.catastrophic_open_loss is not None else None
                ),
                "warning_open_loss_threshold": (
                    str(settings.probationary_paper_lane_warning_open_loss.get(lane.spec.lane_id))
                    if settings.probationary_paper_lane_warning_open_loss.get(lane.spec.lane_id) is not None
                    else None
                ),
                "artifacts_dir": str(lane.settings.probationary_artifacts_path),
                "database_url": lane.settings.database_url,
                **_probationary_lane_eligibility_snapshot(
                    lane=lane,
                    risk_state=risk_state,
                    now=now_local,
                ),
                **_lane_status_row_extras(lane),
            }
            for lane in lanes
        ],
    }
    return structured_logger.write_operator_status(payload)


def _same_underlying_conflict_review_state_path(settings: StrategySettings) -> Path:
    return settings.probationary_artifacts_path.parent.parent / "operator_dashboard" / "same_underlying_conflict_review_state.json"


def _same_underlying_conflict_review_history_path(settings: StrategySettings) -> Path:
    return settings.probationary_artifacts_path.parent.parent / "operator_dashboard" / "same_underlying_conflict_review_history.jsonl"


def _same_underlying_conflict_events_path(settings: StrategySettings) -> Path:
    return settings.probationary_artifacts_path.parent.parent / "operator_dashboard" / "same_underlying_conflict_events.jsonl"


def _write_same_underlying_conflict_review_store(path: Path, records: dict[str, dict[str, Any]], *, updated_at: str, history_path: Path, events_path: Path) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    active_entry_holds = sorted(
        instrument
        for instrument, record in records.items()
        if record.get("hold_new_entries") is True and record.get("entry_hold_effective") is True
    )
    path.write_text(
        json.dumps(
            {
                "updated_at": updated_at,
                "records": records,
                "active_entry_holds": active_entry_holds,
                "history_path": str(history_path.resolve()),
                "events_path": str(events_path.resolve()),
            },
            sort_keys=True,
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _append_same_underlying_conflict_event(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, sort_keys=True))
        handle.write("\n")


def _parse_same_underlying_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value)
    except ValueError:
        return None


def _enforce_probationary_same_underlying_hold_expiry(settings: StrategySettings) -> dict[str, dict[str, Any]]:
    state_path = _same_underlying_conflict_review_state_path(settings)
    history_path = _same_underlying_conflict_review_history_path(settings)
    events_path = _same_underlying_conflict_events_path(settings)
    payload = _read_json(state_path)
    records = payload.get("records") or {}
    if not isinstance(records, dict):
        return {}
    now = datetime.now(timezone.utc)
    changed = False
    for instrument, value in list(records.items()):
        if not isinstance(value, dict):
            continue
        record = dict(value)
        hold_expires_at = _parse_same_underlying_iso_datetime(str(record.get("hold_expires_at") or ""))
        if (
            record.get("hold_new_entries") is True
            and hold_expires_at is not None
            and hold_expires_at <= now
        ):
            expiry_reason = (
                f"Same-underlying entry hold expired at {hold_expires_at.isoformat()}; "
                "new entries are no longer blocked automatically."
            )
            record.update(
                {
                    "hold_new_entries": False,
                    "entry_hold_effective": False,
                    "hold_expired": True,
                    "hold_expired_at": now.isoformat(),
                    "hold_expiry_enforced": True,
                    "hold_effective_now": False,
                    "hold_state_reason": expiry_reason,
                    "state_status": "HOLD_EXPIRED",
                }
            )
            records[instrument] = record
            changed = True
            event_payload = {
                "event_type": "conflict_hold_expired",
                "occurred_at": now.isoformat(),
                "instrument": str(instrument or "").strip().upper(),
                "standalone_strategy_ids": list((record.get("current_material_state") or {}).get("standalone_strategy_ids") or []),
                "conflict_fingerprint": record.get("current_conflict_fingerprint") or record.get("reviewed_conflict_fingerprint"),
                "conflict_version": record.get("current_conflict_fingerprint") or record.get("reviewed_conflict_fingerprint"),
                "severity": record.get("severity_at_review"),
                "conflict_kind": record.get("conflict_kind_at_review"),
                "operator_label": str(record.get("hold_set_by") or "automatic expiry"),
                "note": expiry_reason,
                "automatic": True,
                "operator_triggered": False,
                "hold_new_entries": False,
                "entry_hold_effective": False,
                "review_state_status": "HOLD_EXPIRED",
                "hold_effective_now": False,
                "hold_expired": True,
                "hold_expired_at": now.isoformat(),
                "hold_state_reason": expiry_reason,
            }
            event_payload["event_id"] = hashlib.sha256(
                json.dumps(
                    {
                        "event_type": event_payload["event_type"],
                        "occurred_at": event_payload["occurred_at"],
                        "instrument": event_payload["instrument"],
                        "conflict_fingerprint": event_payload["conflict_fingerprint"],
                    },
                    sort_keys=True,
                ).encode("utf-8")
            ).hexdigest()
            _append_same_underlying_conflict_event(events_path, event_payload)
            _append_same_underlying_conflict_event(
                history_path,
                {
                    "timestamp": now.isoformat(),
                    "event": "hold_expired",
                    "instrument": str(instrument or "").strip().upper(),
                    "operator_label": str(record.get("hold_set_by") or "automatic expiry"),
                    "note": expiry_reason,
                    "current_state": record,
                },
            )
    if changed:
        _write_same_underlying_conflict_review_store(
            state_path,
            {str(key): dict(value) for key, value in records.items() if isinstance(value, dict)},
            updated_at=now.isoformat(),
            history_path=history_path,
            events_path=events_path,
        )
    return {str(key): dict(value) for key, value in records.items() if isinstance(value, dict)}


def _load_probationary_same_underlying_entry_holds(settings: StrategySettings) -> dict[str, dict[str, Any]]:
    records = _enforce_probationary_same_underlying_hold_expiry(settings)
    holds: dict[str, dict[str, Any]] = {}
    for instrument, record in records.items():
        instrument_key = str(instrument or "").strip().upper()
        if not instrument_key or not isinstance(record, dict):
            continue
        if record.get("hold_new_entries") is True and record.get("entry_hold_effective") is True:
            holds[instrument_key] = dict(record)
    return holds


def _apply_probationary_same_underlying_entry_holds(
    *,
    settings: StrategySettings,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
    structured_logger: StructuredLogger,
    alert_dispatcher: AlertDispatcher,
) -> None:
    holds_by_instrument = _load_probationary_same_underlying_entry_holds(settings)
    now = datetime.now(timezone.utc)
    for lane in lanes:
        instrument = str(lane.spec.symbol or "").strip().upper()
        hold_record = holds_by_instrument.get(instrument)
        hold_active = hold_record is not None
        hold_reason = (
            str((hold_record or {}).get("hold_reason") or "").strip()
            or (
                f"New entries held by operator for same-underlying conflict review on {instrument}."
                if hold_active
                else ""
            )
        ) or None
        prior_held = lane.strategy_engine.state.same_underlying_entry_hold
        prior_reason = lane.strategy_engine.state.same_underlying_hold_reason
        lane.strategy_engine.set_same_underlying_entry_hold(now, hold_active, reason=hold_reason)
        if prior_held == hold_active and (prior_reason or None) == (hold_reason or None):
            continue
        payload = {
            "action": "same_underlying_entry_hold",
            "status": "applied",
            "instrument": instrument,
            "lane_id": lane.spec.lane_id,
            "standalone_strategy_id": getattr(lane.strategy_engine, "_runtime_identity", {}).get("standalone_strategy_id"),
            "hold_new_entries": hold_active,
            "message": (
                hold_reason
                if hold_active
                else f"Same-underlying entry hold cleared for {instrument}."
            ),
            "applied_at": now.isoformat(),
            "source": "same_underlying_conflict_review_state",
        }
        structured_logger.log_operator_control(payload)
        alert_dispatcher.emit(
            "warning" if hold_active else "info",
            "same_underlying_entry_hold_applied" if hold_active else "same_underlying_entry_hold_cleared",
            payload["message"],
            payload,
        )


def _resolve_probationary_supervisor_target_lane(
    payload: dict[str, Any],
    lanes: Sequence[ProbationaryPaperLaneRuntime],
) -> tuple[ProbationaryPaperLaneRuntime | None, dict[str, Any] | None]:
    lane_id = str(payload.get("lane_id") or "").strip()
    shared_strategy_identity = str(payload.get("shared_strategy_identity") or "").strip()
    if shared_strategy_identity and not lane_id:
        try:
            lane_id = get_shared_strategy_identity(shared_strategy_identity).lane_id
        except KeyError:
            return None, {
                "status": "rejected",
                "message": (
                    "Operator control rejected because "
                    f"shared strategy identity {shared_strategy_identity} is unknown."
                ),
            }
    if not lane_id:
        return None, None
    target_lane = next((lane for lane in lanes if lane.spec.lane_id == lane_id), None)
    if target_lane is None:
        return None, {
            "status": "rejected",
            "message": (
                "Operator control rejected because "
                f"lane {lane_id} is not active in the current paper runtime."
            ),
        }
    return target_lane, None


def _apply_probationary_supervisor_operator_control(
    *,
    settings: StrategySettings,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
    structured_logger: StructuredLogger,
    alert_dispatcher: AlertDispatcher,
    risk_state: ProbationaryPaperRiskRuntimeState,
) -> dict[str, Any] | None:
    control_path = settings.resolved_probationary_operator_control_path
    if not control_path.exists():
        return None
    payload = _read_json(control_path)
    now = datetime.now(timezone.utc)
    if payload.get("action") == "flatten_and_halt" and payload.get("status") in {"flatten_pending", "applied"}:
        if all(_lane_is_flat_and_safe(lane) for lane in lanes):
            result = dict(payload)
            result["status"] = "applied"
            result["flatten_state"] = "complete"
            result["completed_at"] = now.isoformat()
            result["message"] = "Flatten And Halt completed; all probationary paper lanes are flat."
            control_path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            structured_logger.log_operator_control(result)
            return result
        return payload
    if payload.get("status") != "pending":
        return payload

    action = str(payload.get("action", ""))
    result = dict(payload)
    result["applied_at"] = now.isoformat()
    result["control_path"] = str(control_path)
    target_lane, target_error = _resolve_probationary_supervisor_target_lane(payload, lanes)
    if target_error is not None:
        result.update(target_error)
        control_path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        structured_logger.log_operator_control(result)
        alert_dispatcher.emit(
            "warning",
            "operator_control_rejected",
            result["message"],
            result,
        )
        return result
    if target_lane is not None and action == "stop_after_cycle":
        result["status"] = "rejected"
        result["message"] = "Stop After Current Cycle rejected because it is a runtime-wide control and cannot target a single lane."
        control_path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        structured_logger.log_operator_control(result)
        alert_dispatcher.emit(
            "warning",
            "operator_control_rejected",
            result["message"],
            result,
        )
        return result
    selected_lanes = [target_lane] if target_lane is not None and action in _LANE_TARGETABLE_PROBATIONARY_CONTROL_ACTIONS else list(lanes)
    if target_lane is not None:
        result["lane_id"] = target_lane.spec.lane_id
        result["lane_name"] = target_lane.spec.display_name
        shared_strategy_identity = str(
            payload.get("shared_strategy_identity") or getattr(target_lane.spec, "shared_strategy_identity", "") or ""
        ).strip()
        if shared_strategy_identity:
            result["shared_strategy_identity"] = shared_strategy_identity

    if action == "halt_entries":
        for lane in selected_lanes:
            lane.strategy_engine.set_operator_halt(now, True)
        result["status"] = "applied"
        result["message"] = (
            f"Entries halted for lane {target_lane.spec.lane_id}."
            if target_lane is not None
            else "Entries halted for all probationary paper lanes."
        )
        result["halt_reason"] = "operator_halt_entries"
    elif action == "force_reconcile":
        lane_results: list[dict[str, Any]] = []
        unresolved_lanes: list[str] = []
        repaired_lanes: list[str] = []
        for lane in selected_lanes:
            reconciliation = lane.strategy_engine.force_reconcile(
                occurred_at=now,
                execution_engine=lane.execution_engine,
            )
            lane_results.append(
                {
                    "lane_id": lane.spec.lane_id,
                    "lane_name": lane.spec.display_name,
                    "symbol": lane.spec.symbol,
                    "classification": reconciliation.get("classification"),
                    "clean": reconciliation.get("clean"),
                    "recommended_action": reconciliation.get("recommended_action"),
                }
            )
            if reconciliation.get("clean") or reconciliation.get("classification") == "safe_repair":
                repaired_lanes.append(lane.spec.lane_id)
            else:
                unresolved_lanes.append(lane.spec.lane_id)
        result["status"] = "applied"
        result["message"] = (
            (
                f"Force Reconcile completed for lane {target_lane.spec.lane_id} and it is aligned."
                if target_lane is not None and not unresolved_lanes
                else f"Force Reconcile completed for lane {target_lane.spec.lane_id}, but it still requires review before entries can resume."
                if target_lane is not None
                else "Force Reconcile completed and all lanes are aligned."
            )
            if not unresolved_lanes
            else (
                f"Force Reconcile completed for lane {target_lane.spec.lane_id}, but it still requires review before entries can resume."
                if target_lane is not None
                else "Force Reconcile completed, but some lanes still require review before entries can resume."
            )
        )
        result["reconciliation"] = {
            "lane_results": lane_results,
            "unresolved_lanes": unresolved_lanes,
            "repaired_lanes": repaired_lanes,
        }
    elif action == REALIZED_LOSER_SESSION_OVERRIDE_ACTION:
        lane_id = str(payload.get("lane_id") or "").strip()
        local_operator_identity = str(payload.get("local_operator_identity") or "").strip()
        requested_confirmation = bool(payload.get("session_override_confirmed"))
        target_lane = next((lane for lane in lanes if lane.spec.lane_id == lane_id), None)
        if not lane_id:
            result["status"] = "rejected"
            result["message"] = "Session override rejected because no lane_id was provided."
        elif target_lane is None:
            result["status"] = "rejected"
            result["message"] = f"Session override rejected because lane {lane_id} is not active in the current paper runtime."
        elif not local_operator_identity:
            result["status"] = "rejected"
            result["message"] = "Session override rejected because no authenticated local operator identity was provided."
        elif not requested_confirmation:
            result["status"] = "rejected"
            result["message"] = "Session override rejected because explicit session-override confirmation was not provided."
        elif risk_state.desk_halt_new_entries_triggered or risk_state.desk_flatten_and_halt_triggered:
            result["status"] = "rejected"
            result["message"] = "Session override rejected because a desk-level paper risk guardrail is active."
        else:
            reconciliation = _reconcile_paper_runtime(
                repositories=target_lane.repositories,
                strategy_engine=target_lane.strategy_engine,
                execution_engine=target_lane.execution_engine,
            )
            state = target_lane.strategy_engine.state
            lane_state = risk_state.lane_states.setdefault(target_lane.spec.lane_id, {})
            if (
                str(lane_state.get("risk_state") or "") != "HALTED_DEGRADATION"
                or str(lane_state.get("halt_reason") or "") != REALIZED_LOSER_SESSION_OVERRIDE_REASON
            ):
                result["status"] = "rejected"
                result["message"] = "Session override rejected because the lane is not currently halted by the realized-loser-per-session policy."
            elif (
                not reconciliation["clean"]
                or state.position_side != PositionSide.FLAT
                or state.open_broker_order_id is not None
                or state.internal_position_qty != 0
                or state.broker_position_qty != 0
                or target_lane.execution_engine.pending_executions()
                or state.fault_code is not None
            ):
                result["status"] = "rejected"
                result["message"] = "Session override rejected because the lane is not safely flat, reconciled, and clear."
                result["reconciliation"] = reconciliation
            else:
                lane_state["catastrophic_triggered"] = False
                lane_state["warning_triggered"] = False
                lane_state["degradation_triggered"] = False
                lane_state["risk_state"] = "OK"
                lane_state["halt_reason"] = None
                lane_state["unblock_action"] = "Session override active for current session"
                lane_state["last_cleared_at"] = now.isoformat()
                lane_state["last_cleared_action"] = REALIZED_LOSER_SESSION_OVERRIDE_ACTION
                lane_state["session_override_active"] = True
                lane_state["session_override_session_date"] = risk_state.session_date
                lane_state["session_override_reason"] = REALIZED_LOSER_SESSION_OVERRIDE_REASON
                lane_state["session_override_applied_at"] = now.isoformat()
                lane_state["session_override_applied_by"] = local_operator_identity
                lane_state["session_override_note"] = payload.get("override_note")
                lane_state["session_override_confirmed"] = True
                target_lane.strategy_engine.set_operator_halt(now, False)
                result["status"] = "applied"
                result["message"] = (
                    f"Session override applied for {target_lane.spec.display_name} ({target_lane.spec.symbol}). "
                    "The lane-level realized-loser gate is bypassed for the current session only."
                )
                result["lane_id"] = target_lane.spec.lane_id
                result["lane_name"] = target_lane.spec.display_name
                result["symbol"] = target_lane.spec.symbol
                result["halt_reason"] = REALIZED_LOSER_SESSION_OVERRIDE_REASON
                result["session_override"] = True
                result["session_override_scope"] = "current_session_only"
                result["session_override_confirmed"] = True
                result["local_operator_identity"] = local_operator_identity
                result["audit_event_type"] = "lane_force_resume_session_override"
    elif action == "clear_risk_halts":
        blocked_lanes: list[dict[str, str]] = []
        for lane in selected_lanes:
            reconciliation = _reconcile_paper_runtime(
                repositories=lane.repositories,
                strategy_engine=lane.strategy_engine,
                execution_engine=lane.execution_engine,
            )
            state = lane.strategy_engine.state
            if (
                not reconciliation["clean"]
                or state.position_side != PositionSide.FLAT
                or state.open_broker_order_id is not None
                or state.internal_position_qty != 0
                or state.broker_position_qty != 0
                or lane.execution_engine.pending_executions()
                or state.fault_code is not None
            ):
                blocked_lanes.append(
                    {
                        "lane_id": lane.spec.lane_id,
                        "reason": "lane_not_flat_reconciled_and_clear",
                    }
                )
        active_lane_risk = any(
            state.get("catastrophic_triggered") or state.get("warning_triggered") or state.get("degradation_triggered")
            for state in risk_state.lane_states.values()
        )
        active_desk_risk = risk_state.desk_halt_new_entries_triggered or risk_state.desk_flatten_and_halt_triggered
        if blocked_lanes:
            result["status"] = "rejected"
            result["message"] = "Clear Risk Halts rejected because one or more lanes are not safely flat, reconciled, and clear."
            result["blocked_lanes"] = blocked_lanes
        elif not active_lane_risk and not active_desk_risk:
            result["status"] = "rejected"
            result["message"] = "Clear Risk Halts rejected because no active paper risk halt is persisted."
        else:
            if target_lane is None:
                risk_state.desk_halt_new_entries_triggered = False
                risk_state.desk_flatten_and_halt_triggered = False
                risk_state.desk_last_trigger_reason = None
                risk_state.desk_last_triggered_at = None
                risk_state.desk_last_cleared_at = now.isoformat()
                risk_state.desk_last_cleared_action = "clear_risk_halts"
            cleared_lanes: list[str] = []
            for lane in selected_lanes:
                lane_state = risk_state.lane_states.setdefault(lane.spec.lane_id, {})
                if lane_state.get("catastrophic_triggered") or lane_state.get("warning_triggered") or lane_state.get("degradation_triggered"):
                    cleared_lanes.append(lane.spec.lane_id)
                lane_state["catastrophic_triggered"] = False
                lane_state["warning_triggered"] = False
                lane_state["degradation_triggered"] = False
                lane_state["risk_state"] = "OK"
                lane_state["halt_reason"] = None
                lane_state["unblock_action"] = "Resume Entries"
                lane_state["last_cleared_at"] = now.isoformat()
                lane_state["last_cleared_action"] = "clear_risk_halts"
            result["status"] = "applied"
            result["message"] = (
                f"Paper risk halts cleared for lane {target_lane.spec.lane_id}. Use Resume Entries to re-arm it."
                if target_lane is not None
                else "Paper risk halts cleared. Use Resume Entries to re-arm eligible lanes."
            )
            result["cleared_lanes"] = cleared_lanes
            result["requires_resume_entries"] = True
    elif action == "resume_entries":
        if risk_state.desk_halt_new_entries_triggered or risk_state.desk_flatten_and_halt_triggered:
            result["status"] = "rejected"
            result["message"] = "Resume Entries rejected because a desk paper risk guardrail is active."
        else:
            resumed_lanes: list[str] = []
            blocked_lanes: list[dict[str, str]] = []
            for lane in selected_lanes:
                lane_state = risk_state.lane_states.setdefault(lane.spec.lane_id, {})
                if lane_state.get("catastrophic_triggered") or lane_state.get("warning_triggered") or lane_state.get("degradation_triggered"):
                    blocked_lanes.append(
                        {
                            "lane_id": lane.spec.lane_id,
                            "reason": "lane_specific_risk_halt",
                            "detail": str(lane_state.get("halt_reason") or ""),
                        }
                    )
                    continue
                if lane.strategy_engine.state.fault_code is not None:
                    blocked_lanes.append(
                        {
                            "lane_id": lane.spec.lane_id,
                            "reason": "fault",
                            "detail": str(lane.strategy_engine.state.fault_code),
                        }
                    )
                    continue
                lane.strategy_engine.set_operator_halt(now, False)
                lane_state["unblock_action"] = "No action needed; already eligible"
                resumed_lanes.append(lane.spec.lane_id)
            result["status"] = "applied" if resumed_lanes else "rejected"
            if resumed_lanes:
                if target_lane is not None:
                    result["message"] = f"Entries resumed for lane: {target_lane.spec.lane_id}."
                else:
                    result["message"] = f"Entries resumed for lanes: {', '.join(resumed_lanes)}."
            elif target_lane is not None and blocked_lanes:
                blocked = blocked_lanes[0]
                detail = f" ({blocked['detail']})" if blocked.get("detail") else ""
                result["message"] = (
                    f"Resume Entries rejected for lane {target_lane.spec.lane_id} because it remains blocked by "
                    f"{blocked['reason']}{detail}."
                )
            elif target_lane is not None:
                result["message"] = (
                    f"Resume Entries rejected for lane {target_lane.spec.lane_id} because it could not be resumed."
                )
            else:
                result["message"] = "Resume Entries rejected because all lanes remain blocked by risk or fault state."
            result["resumed_lanes"] = resumed_lanes
            result["blocked_lanes"] = blocked_lanes
            result["halt_reason"] = None
    elif action == "clear_fault":
        uncleared: list[str] = []
        cleared: list[str] = []
        for lane in selected_lanes:
            reconciliation = _reconcile_paper_runtime(
                repositories=lane.repositories,
                strategy_engine=lane.strategy_engine,
                execution_engine=lane.execution_engine,
            )
            state = lane.strategy_engine.state
            lane_state = risk_state.lane_states.get(lane.spec.lane_id, {})
            if (
                reconciliation["clean"]
                and state.position_side == PositionSide.FLAT
                and state.open_broker_order_id is None
                and state.internal_position_qty == 0
                and state.broker_position_qty == 0
                and not (lane_state.get("catastrophic_triggered") or lane_state.get("warning_triggered"))
            ):
                lane.strategy_engine.clear_fault(now)
                cleared.append(lane.spec.lane_id)
            else:
                uncleared.append(lane.spec.lane_id)
        result["status"] = "applied" if cleared else "rejected"
        result["message"] = (
            (
                f"Cleared fault for lane {target_lane.spec.lane_id}."
                if target_lane is not None and cleared
                else f"Cleared faults for lanes: {', '.join(cleared)}."
            )
            if cleared
            else (
                f"Clear fault rejected because lane {target_lane.spec.lane_id} was not safely flat and reconciled."
                if target_lane is not None
                else "Clear fault rejected because no lanes were safely flat and reconciled."
            )
        )
        result["uncleared_lanes"] = uncleared
    elif action == "flatten_and_halt":
        flatten_states: dict[str, str] = {}
        for lane in selected_lanes:
            _halt_probationary_lane(lane, now)
            flatten_state, _ = _flatten_probationary_lane(lane, now, "operator_flatten_and_halt")
            flatten_states[lane.spec.lane_id] = flatten_state
        result["status"] = "flatten_pending" if any(state == "pending_fill" for state in flatten_states.values()) else "applied"
        result["flatten_state"] = "pending_fill" if result["status"] == "flatten_pending" else "complete"
        result["message"] = (
            (
                f"Flatten intent submitted for lane {target_lane.spec.lane_id}; it remains halted until flat."
                if target_lane is not None and result["status"] == "flatten_pending"
                else "Flatten intent submitted; paper runtime remains halted until all lanes are flat."
            )
            if result["status"] == "flatten_pending"
            else (
                f"Lane {target_lane.spec.lane_id} halted and already flat."
                if target_lane is not None
                else "Runtime halted and already flat."
            )
        )
        result["halt_reason"] = "operator_flatten_and_halt"
        result["lane_flatten_states"] = flatten_states
    elif action == "stop_after_cycle":
        for lane in lanes:
            _halt_probationary_lane(lane, now)
        result["status"] = "applied"
        result["halt_reason"] = "operator_stop_after_cycle"
        result["stop_after_cycle_requested"] = True
        result["message"] = "Stop After Current Cycle requested; entries are halted across all paper lanes."
    else:
        result["status"] = "rejected"
        result["message"] = f"Unsupported control action: {action}"

    control_path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    structured_logger.log_operator_control(result)
    alert_dispatcher.emit(
        "info" if result["status"] == "applied" else "warning",
        "operator_control_applied" if result["status"] == "applied" else "operator_control_rejected",
        result["message"],
        result,
    )
    return result


def _lane_is_flat_and_safe(lane: ProbationaryPaperLaneRuntime) -> bool:
    state = lane.strategy_engine.state
    return (
        state.position_side == PositionSide.FLAT
        and state.internal_position_qty == 0
        and state.broker_position_qty == 0
        and state.open_broker_order_id is None
        and not lane.execution_engine.pending_executions()
    )


def _stop_after_cycle_is_safe_for_supervisor(
    control_result: dict[str, Any] | None,
    lanes: Sequence[ProbationaryPaperLaneRuntime],
) -> bool:
    if control_result is None:
        return False
    if control_result.get("action") != "stop_after_cycle":
        return False
    if control_result.get("status") != "applied":
        return False
    return all(_lane_is_flat_and_safe(lane) for lane in lanes)


def generate_probationary_parity_report(
    config_paths: Sequence[str | Path],
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> Path:
    settings = load_settings_from_files(config_paths)
    repositories = RepositorySet(build_engine(settings.database_url))
    live_bars = _load_captured_live_bars(repositories.engine, settings, start_timestamp, end_timestamp)
    if not live_bars:
        raise ValueError("No captured live bars were found for the requested parity window.")

    replay_settings = settings.model_copy(
        update={
            "mode": RuntimeMode.REPLAY,
            "database_url": "sqlite:///:memory:",
        }
    )
    replay_repositories = RepositorySet(build_engine(replay_settings.database_url))
    replay_engine = StrategyEngine(
        settings=replay_settings,
        repositories=replay_repositories,
        execution_engine=ExecutionEngine(),
    )
    for bar in live_bars:
        replay_engine.process_bar(bar)

    live_signal_sources = _collect_signal_sources(repositories.engine, start_timestamp, end_timestamp)
    replay_signal_sources = _collect_signal_sources(replay_repositories.engine, start_timestamp, end_timestamp)
    live_order_reasons = _collect_order_reasons(repositories.engine, start_timestamp, end_timestamp)
    replay_order_reasons = _collect_order_reasons(replay_repositories.engine, start_timestamp, end_timestamp)

    report = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "start_timestamp": start_timestamp.isoformat() if start_timestamp is not None else None,
        "end_timestamp": end_timestamp.isoformat() if end_timestamp is not None else None,
        "captured_live_bar_count": len(live_bars),
        "replay_bar_count": replay_repositories.processed_bars.count(),
        "captured_live_signal_sources": live_signal_sources,
        "replay_signal_sources": replay_signal_sources,
        "captured_live_order_reasons": live_order_reasons,
        "replay_order_reasons": replay_order_reasons,
        "signal_source_counts_match": live_signal_sources == replay_signal_sources,
        "order_reason_counts_match": live_order_reasons == replay_order_reasons,
    }
    logger = StructuredLogger(settings.probationary_artifacts_path)
    report_name = _parity_report_name(start_timestamp, end_timestamp)
    return logger.write_parity_report(report_name, report)


def inspect_probationary_shadow_session(
    config_paths: Sequence[str | Path],
    session_date: date | None = None,
) -> ProbationarySessionInspection:
    settings = load_settings_from_files(config_paths)
    repositories = RepositorySet(build_engine(settings.database_url))
    artifacts_dir = settings.probationary_artifacts_path
    operator_status_path = artifacts_dir / "operator_status.json"
    operator_status = _read_json(operator_status_path)

    resolved_session_date = session_date or _resolve_session_date(settings, operator_status)
    branch_source_records = _records_for_session_date(
        _read_jsonl(artifacts_dir / "branch_sources.jsonl"),
        resolved_session_date,
        timestamp_field="bar_end_ts",
        timezone_info=settings.timezone_info,
    )
    rule_block_records = _records_for_session_date(
        _read_jsonl(artifacts_dir / "rule_blocks.jsonl"),
        resolved_session_date,
        timestamp_field="bar_end_ts",
        timezone_info=settings.timezone_info,
    )
    alert_records = _records_for_session_date(
        _read_jsonl(artifacts_dir / "alerts.jsonl"),
        resolved_session_date,
        timestamp_field="logged_at",
        timezone_info=settings.timezone_info,
    )
    fill_rows = _load_table_rows_for_session_date(
        repositories.engine,
        fills_table,
        timestamp_column="fill_timestamp",
        session_date=resolved_session_date,
        timezone_info=settings.timezone_info,
    )
    open_intents = _load_open_order_intent_rows(repositories)

    processed_bars_session = _count_bars_for_session_date(
        repositories.engine,
        resolved_session_date,
        settings,
    )
    branch_source_counts = Counter(record.get("source", "UNKNOWN") for record in branch_source_records)
    blocked_reason_counts = Counter(record.get("block_reason", "UNKNOWN") for record in rule_block_records)
    alert_counts_by_code = Counter(record.get("code", "UNKNOWN") for record in alert_records)

    return ProbationarySessionInspection(
        session_date=resolved_session_date.isoformat(),
        artifacts_dir=str(artifacts_dir),
        operator_status_path=str(operator_status_path),
        health_status=_nested_get(operator_status, "health", "health_status", default="UNKNOWN"),
        market_data_ok=bool(_nested_get(operator_status, "health", "market_data_ok", default=False)),
        broker_ok=bool(_nested_get(operator_status, "health", "broker_ok", default=False)),
        persistence_ok=bool(_nested_get(operator_status, "health", "persistence_ok", default=False)),
        reconciliation_clean=bool(_nested_get(operator_status, "health", "reconciliation_clean", default=False)),
        invariants_ok=bool(_nested_get(operator_status, "health", "invariants_ok", default=False)),
        strategy_status=str(operator_status.get("strategy_status", "UNKNOWN")),
        processed_bars_total=int(operator_status.get("processed_bars", repositories.processed_bars.count())),
        processed_bars_session=processed_bars_session,
        last_processed_bar_end_ts=operator_status.get("last_processed_bar_end_ts"),
        new_bars_last_cycle=int(operator_status.get("new_bars_last_cycle", 0)),
        current_position_side=str(operator_status.get("position_side", "UNKNOWN")),
        open_intent_count=len(open_intents),
        fill_count_session=len(fill_rows),
        branch_source_counts=dict(sorted(branch_source_counts.items())),
        blocked_reason_counts=dict(sorted(blocked_reason_counts.items())),
        alert_count=len(alert_records),
        fault_alert_count=sum(1 for record in alert_records if str(record.get("severity", "")).lower() == "error"),
        alert_counts_by_code=dict(sorted(alert_counts_by_code.items())),
    )


def render_probationary_inspection(inspection: ProbationarySessionInspection) -> str:
    lines = [
        f"Probationary Shadow Session: {inspection.session_date}",
        f"Health: {inspection.health_status}",
        (
            "Subsystems: "
            f"market_data_ok={inspection.market_data_ok} "
            f"broker_ok={inspection.broker_ok} "
            f"persistence_ok={inspection.persistence_ok} "
            f"reconciliation_clean={inspection.reconciliation_clean} "
            f"invariants_ok={inspection.invariants_ok}"
        ),
        f"Strategy Status: {inspection.strategy_status}",
        f"Current Position: {inspection.current_position_side}",
        f"Processed Bars: total={inspection.processed_bars_total} session={inspection.processed_bars_session}",
        f"Last Processed Bar: {inspection.last_processed_bar_end_ts or 'none'}",
        f"New Bars Last Cycle: {inspection.new_bars_last_cycle}",
        f"Open Intents: {inspection.open_intent_count}",
        f"Session Fills: {inspection.fill_count_session}",
        f"Alerts: total={inspection.alert_count} faults={inspection.fault_alert_count}",
        f"Artifacts Dir: {inspection.artifacts_dir}",
        f"Operator Status: {inspection.operator_status_path}",
        "Approved Branch Decisions:",
    ]
    if inspection.branch_source_counts:
        lines.extend(f"  - {source}: {count}" for source, count in inspection.branch_source_counts.items())
    else:
        lines.append("  - none recorded for this session date")

    lines.append("Rule Blocks:")
    if inspection.blocked_reason_counts:
        lines.extend(f"  - {reason}: {count}" for reason, count in inspection.blocked_reason_counts.items())
    else:
        lines.append("  - none")

    lines.append("Alerts By Code:")
    if inspection.alert_counts_by_code:
        lines.extend(f"  - {code}: {count}" for code, count in inspection.alert_counts_by_code.items())
    else:
        lines.append("  - none")
    return "\n".join(lines)


def _closed_trade_digest(blotter_rows: Sequence[Any]) -> list[dict[str, Any]]:
    digest: list[dict[str, Any]] = []
    for row in blotter_rows:
        digest.append(
            {
                "trade_id": row.trade_id,
                "direction": row.direction,
                "entry_ts": row.entry_ts.isoformat(),
                "entry_px": str(row.entry_px),
                "exit_ts": row.exit_ts.isoformat(),
                "exit_px": str(row.exit_px),
                "qty": row.qty,
                "gross_pnl": str(row.gross_pnl),
                "fees": str(row.fees),
                "slippage": str(row.slippage),
                "net_pnl": str(row.net_pnl),
                "setup_family": row.setup_family,
                "exit_reason": row.exit_reason,
                "entry_session": row.entry_session,
                "entry_session_phase": row.entry_session_phase,
                "exit_session": row.exit_session,
                "exit_session_phase": row.exit_session_phase,
            }
        )
    return digest


def generate_probationary_daily_summary(
    config_paths: Sequence[str | Path],
    session_date: date | None = None,
) -> ProbationaryDailySummary:
    settings = load_settings_from_files(config_paths)
    lane_specs = _active_probationary_paper_lane_specs(settings)
    if settings.mode is RuntimeMode.PAPER and lane_specs:
        return _generate_probationary_supervisor_daily_summary(
            settings=settings,
            lane_specs=lane_specs,
            session_date=session_date,
        )
    repositories = RepositorySet(build_engine(settings.database_url))
    artifacts_dir = settings.probationary_artifacts_path
    operator_status = _read_json(artifacts_dir / "operator_status.json")
    resolved_session_date = session_date or _resolve_session_date(settings, operator_status)
    point_value = Decimal(str(os.environ.get("REPLAY_POINT_VALUE", "10")))

    branch_source_records = _records_for_session_date(
        _read_jsonl(artifacts_dir / "branch_sources.jsonl"),
        resolved_session_date,
        timestamp_field="bar_end_ts",
        timezone_info=settings.timezone_info,
    )
    rule_block_records = _records_for_session_date(
        _read_jsonl(artifacts_dir / "rule_blocks.jsonl"),
        resolved_session_date,
        timestamp_field="bar_end_ts",
        timezone_info=settings.timezone_info,
    )
    alert_records = _records_for_session_date(
        _read_jsonl(artifacts_dir / "alerts.jsonl"),
        resolved_session_date,
        timestamp_field="logged_at",
        timezone_info=settings.timezone_info,
    )

    order_intents = _load_table_rows_for_session_date(
        repositories.engine,
        order_intents_table,
        timestamp_column="created_at",
        session_date=resolved_session_date,
        timezone_info=settings.timezone_info,
    )
    all_open_order_intents = _load_open_order_intent_rows(repositories)
    fills = _load_table_rows_for_session_date(
        repositories.engine,
        fills_table,
        timestamp_column="fill_timestamp",
        session_date=resolved_session_date,
        timezone_info=settings.timezone_info,
    )
    bars = _load_bars_for_session_date(repositories.engine, resolved_session_date, settings)
    session_lookup = build_session_lookup(bars)
    blotter_rows = build_trade_ledger(
        order_intents,
        fills,
        session_lookup,
        point_value=point_value,
        fee_per_fill=Decimal("0"),
        slippage_per_fill=Decimal("0"),
        bars=bars,
    )
    blotter_metrics = build_summary_metrics(blotter_rows)

    processed_bars_session = _count_bars_for_session_date(repositories.engine, resolved_session_date, settings)
    allowed_branch_counts = Counter(
        record.get("source", "UNKNOWN")
        for record in branch_source_records
        if record.get("decision") == "allowed"
    )
    blocked_branch_counts = Counter(
        record.get("source", "UNKNOWN")
        for record in branch_source_records
        if record.get("decision") == "blocked"
    )
    blocked_reason_counts = Counter(record.get("block_reason", "UNKNOWN") for record in rule_block_records)
    alert_counts_by_code = Counter(record.get("code", "UNKNOWN") for record in alert_records)
    order_intents_by_reason = Counter(row.get("reason_code", "UNKNOWN") for row in order_intents)
    order_intents_by_type = Counter(row.get("intent_type", "UNKNOWN") for row in order_intents)
    fills_by_intent_type = Counter(row.get("intent_type", "UNKNOWN") for row in fills)

    summary_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "session_date": resolved_session_date.isoformat(),
        "artifacts_dir": str(artifacts_dir),
        "operator_status_path": str(artifacts_dir / "operator_status.json"),
        "health_status": _nested_get(operator_status, "health", "health_status", default="UNKNOWN"),
        "market_data_ok": bool(_nested_get(operator_status, "health", "market_data_ok", default=False)),
        "broker_ok": bool(_nested_get(operator_status, "health", "broker_ok", default=False)),
        "persistence_ok": bool(_nested_get(operator_status, "health", "persistence_ok", default=False)),
        "reconciliation_clean": bool(_nested_get(operator_status, "health", "reconciliation_clean", default=False)),
        "invariants_ok": bool(_nested_get(operator_status, "health", "invariants_ok", default=False)),
        "strategy_status": operator_status.get("strategy_status", "UNKNOWN"),
        "processed_bars_total": int(operator_status.get("processed_bars", repositories.processed_bars.count())),
        "processed_bars_session": processed_bars_session,
        "last_processed_bar_end_ts": operator_status.get("last_processed_bar_end_ts"),
        "new_bars_last_cycle": int(operator_status.get("new_bars_last_cycle", 0)),
        "position_side_end": operator_status.get("position_side", "UNKNOWN"),
        "flat_at_end": operator_status.get("position_side") == "FLAT",
        "unresolved_open_intents": len(all_open_order_intents),
        "entries_and_exits_by_branch": dict(sorted(order_intents_by_reason.items())),
        "order_intents_by_type": dict(sorted(order_intents_by_type.items())),
        "blocked_signals_by_reason": dict(sorted(blocked_reason_counts.items())),
        "allowed_branch_decisions_by_source": dict(sorted(allowed_branch_counts.items())),
        "blocked_branch_decisions_by_source": dict(sorted(blocked_branch_counts.items())),
        "alerts_total": len(alert_records),
        "alerts_by_code": dict(sorted(alert_counts_by_code.items())),
        "fault_alerts": sum(1 for record in alert_records if str(record.get("severity", "")).lower() == "error"),
        "fills_by_intent_type": dict(sorted(fills_by_intent_type.items())),
        "fill_count": len(fills),
        "order_intent_count": len(order_intents),
        "closed_trade_count": blotter_metrics.number_of_trades,
        "realized_net_pnl_scope": "ALL_CLOSED_TRADES_FOR_SESSION",
        "realized_net_pnl": str(blotter_metrics.total_net_pnl),
        "realized_expectancy": str(blotter_metrics.expectancy),
        "realized_max_drawdown": str(blotter_metrics.max_drawdown),
        "closed_trade_digest": _closed_trade_digest(blotter_rows),
        "session_end_assertions": {
            "flat_at_end": operator_status.get("position_side") == "FLAT",
            "no_unresolved_open_intents": len(all_open_order_intents) == 0,
            "reconciliation_clean": bool(_nested_get(operator_status, "health", "reconciliation_clean", default=False)),
        },
        "notes": [
            "Paper/session blotter only reflects closed trades paired from persisted fills.",
            "Daily summary realized_net_pnl is the session-total sum of closed_trade_digest net_pnl values.",
            "Open paper orders and open positions remain visible via operator_status and unresolved_open_intents.",
        ],
    }

    daily_dir = artifacts_dir / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    json_path = daily_dir / f"{resolved_session_date.isoformat()}.summary.json"
    markdown_path = daily_dir / f"{resolved_session_date.isoformat()}.summary.md"
    blotter_path = daily_dir / f"{resolved_session_date.isoformat()}.blotter.csv"
    logger = StructuredLogger(artifacts_dir)
    logger._write_json(json_path, summary_payload)  # noqa: SLF001
    markdown_path.write_text(_render_daily_summary_markdown(summary_payload), encoding="utf-8")
    write_trade_ledger_csv(blotter_rows, blotter_path)
    return ProbationaryDailySummary(
        session_date=resolved_session_date.isoformat(),
        artifact_dir=str(artifacts_dir),
        json_path=str(json_path),
        markdown_path=str(markdown_path),
        blotter_path=str(blotter_path),
        summary=summary_payload,
    )


def _generate_probationary_supervisor_daily_summary(
    *,
    settings: StrategySettings,
    lane_specs: Sequence[ProbationaryPaperLaneSpec],
    session_date: date | None,
) -> ProbationaryDailySummary:
    artifacts_dir = settings.probationary_artifacts_path
    operator_status = _read_json(artifacts_dir / "operator_status.json")
    resolved_session_date = session_date or _resolve_session_date(settings, operator_status)
    branch_source_records = _records_for_session_date(
        _read_jsonl(artifacts_dir / "branch_sources.jsonl"),
        resolved_session_date,
        timestamp_field="bar_end_ts",
        timezone_info=settings.timezone_info,
    )
    rule_block_records = _records_for_session_date(
        _read_jsonl(artifacts_dir / "rule_blocks.jsonl"),
        resolved_session_date,
        timestamp_field="bar_end_ts",
        timezone_info=settings.timezone_info,
    )
    alert_records = _records_for_session_date(
        _read_jsonl(artifacts_dir / "alerts.jsonl"),
        resolved_session_date,
        timestamp_field="logged_at",
        timezone_info=settings.timezone_info,
    )
    blotter_rows = []
    all_order_intents: list[dict[str, Any]] = []
    all_fills: list[dict[str, Any]] = []
    all_open_order_intents: list[dict[str, Any]] = []
    processed_bars_session = 0
    for spec in lane_specs:
        lane_settings = _build_probationary_paper_lane_settings(settings, spec)
        lane_repositories = RepositorySet(build_engine(lane_settings.database_url))
        lane_order_intents = _load_table_rows_for_session_date(
            lane_repositories.engine,
            order_intents_table,
            timestamp_column="created_at",
            session_date=resolved_session_date,
            timezone_info=lane_settings.timezone_info,
        )
        lane_fills = _load_table_rows_for_session_date(
            lane_repositories.engine,
            fills_table,
            timestamp_column="fill_timestamp",
            session_date=resolved_session_date,
            timezone_info=lane_settings.timezone_info,
        )
        lane_bars = _load_bars_for_session_date(lane_repositories.engine, resolved_session_date, lane_settings)
        lane_ledger = build_trade_ledger(
            lane_order_intents,
            lane_fills,
            build_session_lookup(lane_bars),
            point_value=spec.point_value,
            fee_per_fill=Decimal("0"),
            slippage_per_fill=Decimal("0"),
            bars=lane_bars,
        )
        blotter_rows.extend(lane_ledger)
        all_order_intents.extend(lane_order_intents)
        all_fills.extend(lane_fills)
        all_open_order_intents.extend(_load_open_order_intent_rows(lane_repositories))
        processed_bars_session += _count_bars_for_session_date(lane_repositories.engine, resolved_session_date, lane_settings)
    blotter_rows = sorted(blotter_rows, key=lambda row: row.entry_ts)
    blotter_metrics = build_summary_metrics(blotter_rows)
    branch_source_counts = Counter(record.get("source", "UNKNOWN") for record in branch_source_records if record.get("decision") == "allowed")
    blocked_source_counts = Counter(record.get("source", "UNKNOWN") for record in branch_source_records if record.get("decision") == "blocked")
    blocked_reason_counts = Counter(record.get("block_reason", "UNKNOWN") for record in rule_block_records)
    fills_by_intent_type = Counter(row.get("intent_type", "UNKNOWN") for row in all_fills)
    entries_and_exits = Counter(row.get("reason_code", "UNKNOWN") for row in all_order_intents)
    alert_counts_by_code = Counter(record.get("code", "UNKNOWN") for record in alert_records)
    summary_payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "session_date": resolved_session_date.isoformat(),
        "health_status": _nested_get(operator_status, "health", "health_status", default="UNKNOWN"),
        "strategy_status": operator_status.get("strategy_status", "RUNNING_MULTI_LANE"),
        "reconciliation_clean": bool(_nested_get(operator_status, "health", "reconciliation_clean", default=False)),
        "position_side_end": operator_status.get("position_side", "UNKNOWN"),
        "flat_at_end": operator_status.get("position_side", "UNKNOWN") == "FLAT",
        "unresolved_open_intents": len(all_open_order_intents),
        "processed_bars_session": processed_bars_session,
        "processed_bars_total": int(operator_status.get("processed_bars", processed_bars_session)),
        "last_processed_bar_end_ts": operator_status.get("last_processed_bar_end_ts"),
        "allowed_branch_decisions_by_source": dict(sorted(branch_source_counts.items())),
        "blocked_branch_decisions_by_source": dict(sorted(blocked_source_counts.items())),
        "blocked_signals_by_reason": dict(sorted(blocked_reason_counts.items())),
        "order_intent_count": len(all_order_intents),
        "fill_count": len(all_fills),
        "closed_trade_count": blotter_metrics.number_of_trades,
        "realized_net_pnl_scope": "ALL_CLOSED_TRADES_FOR_SESSION",
        "realized_net_pnl": str(blotter_metrics.total_net_pnl),
        "realized_expectancy": str(blotter_metrics.expectancy),
        "realized_max_drawdown": str(blotter_metrics.max_drawdown),
        "closed_trade_digest": _closed_trade_digest(blotter_rows),
        "entries_and_exits_by_branch": dict(sorted(entries_and_exits.items())),
        "fills_by_intent_type": dict(sorted(fills_by_intent_type.items())),
        "alerts_total": len(alert_records),
        "fault_alerts": sum(1 for record in alert_records if str(record.get("severity", "")).lower() == "error"),
        "alerts_by_code": dict(sorted(alert_counts_by_code.items())),
        "notes": [
            "Supervisor daily summary aggregates all configured paper lanes from their isolated paper databases.",
            "Per-lane point values and session restrictions come from paper_config_in_force.json.",
            "Daily summary realized_net_pnl is the session-total sum of closed_trade_digest net_pnl values.",
        ],
        "session_end_assertions": {
            "flat_at_end": operator_status.get("position_side", "UNKNOWN") == "FLAT",
            "no_unresolved_open_intents": len(all_open_order_intents) == 0,
            "reconciliation_clean": bool(_nested_get(operator_status, "health", "reconciliation_clean", default=False)),
        },
    }
    daily_dir = artifacts_dir / "daily"
    daily_dir.mkdir(parents=True, exist_ok=True)
    json_path = daily_dir / f"{resolved_session_date.isoformat()}.summary.json"
    markdown_path = daily_dir / f"{resolved_session_date.isoformat()}.summary.md"
    blotter_path = daily_dir / f"{resolved_session_date.isoformat()}.blotter.csv"
    logger = StructuredLogger(artifacts_dir)
    logger._write_json(json_path, summary_payload)  # noqa: SLF001
    markdown_path.write_text(_render_daily_summary_markdown(summary_payload), encoding="utf-8")
    write_trade_ledger_csv(blotter_rows, blotter_path)
    return ProbationaryDailySummary(
        session_date=resolved_session_date.isoformat(),
        artifact_dir=str(artifacts_dir),
        json_path=str(json_path),
        markdown_path=str(markdown_path),
        blotter_path=str(blotter_path),
        summary=summary_payload,
    )


def build_probationary_paper_readiness(
    config_paths: Sequence[str | Path],
) -> ProbationaryPaperReadiness:
    settings = load_settings_from_files(config_paths)
    artifacts_dir = settings.probationary_artifacts_path
    readiness_dir = artifacts_dir / "paper"
    readiness_dir.mkdir(parents=True, exist_ok=True)
    readiness_path = readiness_dir / "paper_soak_readiness.json"
    summary = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "artifacts_dir": str(artifacts_dir),
        "ready_for_paper_soak": False,
        "real_now": [
            "Repo-bootstrap wrapper path for shadow/auth is in place.",
            "Approved probationary branch allowlist is enforced in runtime.",
            "Deterministic next-bar-open paper fill materialization is wired for live-session paper soak.",
            "Paper pending intents restore across restart and are reconciled against strategy state.",
            "Paper daily summaries and blotter artifacts are available.",
        ],
        "placeholder_or_missing": [
            "Paper fill model is still assumption-based: fills occur deterministically at the next completed bar open, not from exchange microstructure.",
            "No external broker/account comparison exists because live-capital routing remains out of scope.",
            "Session-end assertions are artifact-based and operator-reviewed, not yet enforced by a separate scheduler.",
            "Multi-session soak evidence still needs to be accumulated before trusting paper statistics operationally.",
        ],
        "next_required_before_trustworthy_paper_soak": [
            "Run repeated restart-and-recovery session tests over live data.",
            "Add automatic end-of-session summary scheduling and operator acknowledgment workflow.",
            "Accumulate multi-session paper blotters and compare branch behavior against shadow expectations.",
            "Only after that, decide whether execution assumptions are strong enough for live-capital planning.",
        ],
    }
    StructuredLogger(artifacts_dir)._write_json(readiness_path, summary)  # noqa: SLF001
    return ProbationaryPaperReadiness(
        artifact_path=str(readiness_path),
        ready_for_paper_soak=False,
        summary=summary,
    )


class _PaperSoakValidationPollingService:
    """Deterministic one-bar-at-a-time polling service for paper soak validation."""

    def __init__(self, bars: Sequence[Bar]) -> None:
        self._bars = sorted(list(bars), key=lambda row: row.end_ts)
        self._cursor = 0

    def poll_bars(self, request: SchwabLivePollRequest, *args, **kwargs) -> list[Bar]:
        while self._cursor < len(self._bars):
            bar = self._bars[self._cursor]
            self._cursor += 1
            if request.since is not None and bar.end_ts <= request.since:
                continue
            return [bar]
        return []


class _LiveTimingValidationBroker(PaperBroker):
    """Controllable broker truth for live-timing validation on the real runtime path."""

    def __init__(self) -> None:
        super().__init__()
        self._submitted_order_ids: list[str] = []
        self._forced_order_status: dict[str, str] = {}
        self._force_submit_failure: str | None = None

    def submit_order(self, order_intent: OrderIntent) -> str:
        if self._force_submit_failure:
            raise RuntimeError(self._force_submit_failure)
        broker_order_id = super().submit_order(order_intent)
        self._submitted_order_ids.append(broker_order_id)
        return broker_order_id

    def get_order_status(self, broker_order_id: str) -> dict[str, str]:
        forced = str(self._forced_order_status.get(broker_order_id) or "").strip().upper()
        if forced:
            return {"broker_order_id": broker_order_id, "status": forced}
        return super().get_order_status(broker_order_id)

    def set_submit_failure(self, message: str | None) -> None:
        self._force_submit_failure = str(message).strip() if message else None

    def set_order_status(self, broker_order_id: str, status: str) -> None:
        normalized = str(status or "").strip().upper()
        if normalized:
            self._forced_order_status[broker_order_id] = normalized
        else:
            self._forced_order_status.pop(broker_order_id, None)

    def restore_live_truth(
        self,
        *,
        connected: bool | None = None,
        position_quantity: int | None = None,
        average_price: Decimal | None = None,
        open_order_ids: Sequence[str] | None = None,
        status_by_order_id: dict[str, str] | None = None,
        last_fill_timestamp: datetime | None = None,
    ) -> None:
        if connected is not None:
            if connected:
                self.connect()
            else:
                self.disconnect()
        order_status_map = {
            order_id: OrderStatus(str(status).strip().upper())
            if str(status).strip().upper() in {item.value for item in OrderStatus}
            else OrderStatus.ACKNOWLEDGED
            for order_id, status in dict(status_by_order_id or {}).items()
        }
        self.restore_state(
            position=PaperPosition(quantity=int(position_quantity or 0), average_price=average_price),
            open_order_ids=list(open_order_ids or []),
            order_status=order_status_map,
            last_fill_timestamp=last_fill_timestamp,
        )
        self._forced_order_status.update(
            {order_id: str(status).strip().upper() for order_id, status in dict(status_by_order_id or {}).items()}
        )


def _paper_soak_validation_bar(
    end_ts: datetime,
    open_price: str,
    high_price: str,
    low_price: str,
    close_price: str,
) -> Bar:
    return Bar(
        bar_id=f"MGC|5m|{end_ts.astimezone(timezone.utc).isoformat()}",
        symbol="MGC",
        timeframe="5m",
        start_ts=end_ts - timedelta(minutes=5),
        end_ts=end_ts,
        open=Decimal(open_price),
        high=Decimal(high_price),
        low=Decimal(low_price),
        close=Decimal(close_price),
        volume=100,
        is_final=True,
        session_asia=False,
        session_london=False,
        session_us=False,
        session_allowed=False,
    )


def _paper_soak_validation_bars() -> list[Bar]:
    ny = ZoneInfo("America/New_York")
    rows = [
        ("2026-03-26T17:20:00-04:00", "100", "101", "99", "100"),
        ("2026-03-26T17:25:00-04:00", "100", "101", "99", "100"),
        ("2026-03-26T17:30:00-04:00", "100", "101", "99", "100"),
        ("2026-03-26T17:35:00-04:00", "100", "101", "99", "100"),
        ("2026-03-26T17:40:00-04:00", "100", "101", "99", "100"),
        ("2026-03-26T17:45:00-04:00", "100", "101", "99", "100"),
        ("2026-03-26T17:50:00-04:00", "100", "101", "99", "100"),
        ("2026-03-26T17:55:00-04:00", "100", "101", "99", "100"),
        ("2026-03-26T18:00:00-04:00", "99", "100", "97", "98"),
        ("2026-03-26T18:05:00-04:00", "95", "100", "94", "99"),
        ("2026-03-26T18:10:00-04:00", "100", "100.5", "99", "100.4"),
        ("2026-03-26T18:15:00-04:00", "100.2", "100.6", "99.8", "100.1"),
    ]
    return [
        _paper_soak_validation_bar(
            datetime.fromisoformat(timestamp).astimezone(ny),
            open_price,
            high_price,
            low_price,
            close_price,
        )
        for timestamp, open_price, high_price, low_price, close_price in rows
    ]


def _paper_soak_extended_bars() -> list[Bar]:
    base_bars = _paper_soak_validation_bars()
    shifted_bars = [
        _paper_soak_validation_bar(
            bar.end_ts + timedelta(hours=1),
            str(bar.open),
            str(bar.high),
            str(bar.low),
            str(bar.close),
        )
        for bar in base_bars
    ]
    return [*base_bars, *shifted_bars]


def _paper_soak_unattended_bars(cycles: int = 5) -> list[Bar]:
    base_bars = _paper_soak_validation_bars()
    bars: list[Bar] = []
    for cycle in range(max(int(cycles), 1)):
        offset = timedelta(hours=cycle)
        for bar in base_bars:
            bars.append(
                _paper_soak_validation_bar(
                    bar.end_ts + offset,
                    str(bar.open),
                    str(bar.high),
                    str(bar.low),
                    str(bar.close),
                )
            )
    return bars


def _build_probationary_paper_soak_validation_settings(
    *,
    base_settings: StrategySettings,
    database_path: Path,
    artifacts_dir: Path,
    symbol: str = "MGC",
    lane_id: str = "mgc_paper_soak_validation",
    lane_display_name: str = "MGC Paper Soak Validation",
    point_value: Decimal = Decimal("10"),
    participation_policy: ParticipationPolicy | None = None,
    max_concurrent_entries: int | None = None,
    max_position_quantity: int | None = None,
    max_adds_after_entry: int | None = None,
    add_direction_policy: AddDirectionPolicy | None = None,
) -> StrategySettings:
    resolved_participation_policy = participation_policy or base_settings.participation_policy
    resolved_max_concurrent_entries = max_concurrent_entries or base_settings.max_concurrent_entries
    resolved_max_position_quantity = (
        max_position_quantity if max_position_quantity is not None else base_settings.max_position_quantity
    )
    resolved_max_adds_after_entry = (
        max_adds_after_entry if max_adds_after_entry is not None else base_settings.max_adds_after_entry
    )
    resolved_add_direction_policy = add_direction_policy or base_settings.add_direction_policy
    return base_settings.model_copy(
        update={
            "mode": RuntimeMode.PAPER,
            "symbol": symbol,
            "timeframe": "5m",
            "database_url": f"sqlite:///{database_path}",
            "probationary_artifacts_dir": str(artifacts_dir),
            "trade_size": 1,
            "participation_policy": resolved_participation_policy,
            "max_concurrent_entries": resolved_max_concurrent_entries,
            "max_position_quantity": resolved_max_position_quantity,
            "max_adds_after_entry": resolved_max_adds_after_entry,
            "add_direction_policy": resolved_add_direction_policy,
            "replay_fill_policy": ReplayFillPolicy.NEXT_BAR_OPEN,
            "enable_bull_snap_longs": True,
            "probationary_paper_lane_id": lane_id,
            "probationary_paper_lane_display_name": lane_display_name,
            "probationary_paper_lane_session_restriction": "",
            "probationary_enforce_approved_branches": False,
            "allow_asia": True,
            "allow_london": True,
            "allow_us": True,
            "use_turn_family": True,
            "enable_bear_snap_shorts": False,
            "enable_asia_vwap_longs": False,
            "atr_len": 2,
            "max_bars_long": 2,
            "max_bars_short": 2,
            "anti_churn_bars": 1,
            "turn_fast_len": 1,
            "turn_slow_len": 3,
            "turn_stretch_lookback": 2,
            "min_snap_down_stretch_atr": Decimal("0.10"),
            "min_snap_bar_range_atr": Decimal("0.10"),
            "min_snap_body_atr": Decimal("0.10"),
            "min_snap_close_location": Decimal("0.50"),
            "min_snap_velocity_delta_atr": Decimal("0.00"),
            "snap_cooldown_bars": 1,
            "use_asia_bull_snap_thresholds": False,
            "asia_min_snap_bar_range_atr": Decimal("0.10"),
            "asia_min_snap_body_atr": Decimal("0.10"),
            "asia_min_snap_velocity_delta_atr": Decimal("0.00"),
            "use_bull_snap_location_filter": False,
            "bull_snap_max_close_vs_slow_ema_atr": Decimal("10.0"),
            "bull_snap_require_close_below_slow_ema": False,
            "use_bear_snap_location_filter": False,
            "bear_snap_min_close_vs_slow_ema_atr": Decimal("0.0"),
            "bear_snap_require_close_above_slow_ema": False,
            "below_vwap_lookback": 1,
            "require_green_reclaim_bar": False,
            "reclaim_close_buffer_atr": Decimal("0.0"),
            "min_vwap_bar_range_atr": Decimal("0.10"),
            "require_hold_close_above_vwap": False,
            "require_hold_not_break_reclaim_low": False,
            "require_acceptance_close_above_reclaim_high": False,
            "require_acceptance_close_above_vwap": False,
            "vwap_long_max_bars": 2,
            "vwap_weak_close_lookback_bars": 1,
            "vol_len": 1,
            "show_debug_labels": False,
        }
    )


def _build_probationary_paper_soak_validation_runtime(
    *,
    base_settings: StrategySettings,
    scenario_dir: Path,
    bars: Sequence[Bar],
    broker: PaperBroker | None = None,
    symbol: str = "MGC",
    lane_id: str = "mgc_paper_soak_validation",
    lane_display_name: str = "MGC Paper Soak Validation",
    point_value: Decimal = Decimal("10"),
    participation_policy: ParticipationPolicy | None = None,
    max_concurrent_entries: int | None = None,
    max_position_quantity: int | None = None,
    max_adds_after_entry: int | None = None,
    add_direction_policy: AddDirectionPolicy | None = None,
) -> tuple[ProbationaryPaperLaneRuntime, StrategyEngine, ExecutionEngine, RepositorySet, StructuredLogger]:
    scenario_dir.mkdir(parents=True, exist_ok=True)
    settings = _build_probationary_paper_soak_validation_settings(
        base_settings=base_settings,
        database_path=scenario_dir / "validation.sqlite3",
        artifacts_dir=scenario_dir / "artifacts",
        symbol=symbol,
        lane_id=lane_id,
        lane_display_name=lane_display_name,
        point_value=point_value,
        participation_policy=participation_policy,
        max_concurrent_entries=max_concurrent_entries,
        max_position_quantity=max_position_quantity,
        max_adds_after_entry=max_adds_after_entry,
        add_direction_policy=add_direction_policy,
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    root_logger = StructuredLogger(settings.probationary_artifacts_path.parent)
    lane_logger = StructuredLogger(settings.probationary_artifacts_path)
    structured_logger = ProbationaryLaneStructuredLogger(
        lane_id=lane_id,
        symbol=symbol,
        root_logger=root_logger,
        lane_logger=lane_logger,
    )
    alert_dispatcher = AlertDispatcher(
        structured_logger,
        repositories.alerts,
        source_subsystem="probationary_paper_soak_validation",
    )
    execution_engine = ExecutionEngine(broker=broker or PaperBroker())
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )
    runtime = ProbationaryPaperLaneRuntime(
        spec=ProbationaryPaperLaneSpec(
            lane_id=lane_id,
            display_name=lane_display_name,
            symbol=symbol,
            long_sources=("bullSnap",),
            short_sources=(),
            session_restriction=None,
            point_value=point_value,
            catastrophic_open_loss=Decimal("-500"),
        ),
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        live_polling_service=_PaperSoakValidationPollingService(bars),
        structured_logger=structured_logger,
        alert_dispatcher=alert_dispatcher,
    )
    return runtime, strategy_engine, execution_engine, repositories, lane_logger


def _reset_probationary_paper_soak_scenario_dir(scenario_dir: Path) -> None:
    if scenario_dir.exists():
        shutil.rmtree(scenario_dir, ignore_errors=True)
    scenario_dir.mkdir(parents=True, exist_ok=True)


def _fill_pending_probationary_validation_intent(
    *,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    order_intent_id: str,
    fill_price: Decimal,
    fill_timestamp: datetime,
    signal_bar_id: str | None = None,
    long_entry_family: LongEntryFamily = LongEntryFamily.NONE,
    short_entry_family: ShortEntryFamily = ShortEntryFamily.NONE,
    short_entry_source: str | None = None,
) -> FillEvent:
    pending = execution_engine.pending_execution(order_intent_id)
    if pending is None:
        raise ValueError(f"Pending execution not found for {order_intent_id}.")
    fill = execution_engine.broker.fill_order(
        order_intent=pending.intent,
        fill_price=fill_price,
        fill_timestamp=fill_timestamp,
    )
    strategy_engine._persist_order_intent(  # noqa: SLF001
        pending.intent,
        fill.broker_order_id or pending.broker_order_id,
        order_status=OrderStatus.FILLED,
        submitted_at=pending.submitted_at,
        acknowledged_at=pending.acknowledged_at or fill_timestamp,
        broker_order_status=OrderStatus.FILLED.value,
        last_status_checked_at=fill_timestamp,
        retry_count=pending.retry_count,
    )
    strategy_engine.apply_fill(
        fill_event=fill,
        signal_bar_id=signal_bar_id or pending.signal_bar_id,
        long_entry_family=long_entry_family if pending.intent.is_entry else pending.long_entry_family,
        short_entry_family=short_entry_family if pending.intent.is_entry else pending.short_entry_family,
        short_entry_source=short_entry_source if pending.intent.is_entry else pending.short_entry_source,
    )
    execution_engine.clear_intent(order_intent_id)
    return fill


def _latest_row(rows: Sequence[dict[str, Any]], timestamp_key: str) -> dict[str, Any]:
    def _sort_value(row: dict[str, Any]) -> str:
        return str(row.get(timestamp_key) or "")

    return max(rows, key=_sort_value, default={})


def _paper_soak_runtime_phase(
    strategy_engine: StrategyEngine,
    *,
    latest_reconciliation: dict[str, Any] | None = None,
    latest_watchdog: dict[str, Any] | None = None,
) -> str:
    watchdog_status = str((latest_watchdog or {}).get("status") or "").strip().upper()
    if watchdog_status == "FAULT":
        return "FAULT"
    if watchdog_status == "RECONCILING":
        return "RECONCILING"
    if strategy_engine.state.strategy_status is StrategyStatus.FAULT:
        return "FAULT"
    if strategy_engine.state.reconcile_required or strategy_engine.state.strategy_status is StrategyStatus.RECONCILING:
        return "RECONCILING"
    if bool((latest_reconciliation or {}).get("requires_fault")):
        return "FAULT"
    if bool((latest_reconciliation or {}).get("requires_review")) or bool((latest_reconciliation or {}).get("freeze_new_entries")):
        return "RECONCILING"
    if strategy_engine.state.position_side is not PositionSide.FLAT:
        return "IN_POSITION"
    return "READY"


def _paper_soak_entry_blocker(
    strategy_engine: StrategyEngine,
    *,
    latest_reconciliation: dict[str, Any] | None = None,
    latest_watchdog: dict[str, Any] | None = None,
) -> str | None:
    state = strategy_engine.state
    watchdog_status = str((latest_watchdog or {}).get("status") or "").strip().upper()
    if watchdog_status == "RECONCILING":
        last_escalation = dict((latest_watchdog or {}).get("last_escalation") or {})
        return (
            str(last_escalation.get("timeout_classification") or "").strip()
            or str((latest_watchdog or {}).get("reason") or "").strip()
            or "order_timeout_watchdog_reconciling"
        )
    if watchdog_status == "FAULT":
        return "order_timeout_watchdog_fault"
    if state.fault_code is not None:
        return state.fault_code
    if state.reconcile_required:
        return "reconciliation_required"
    if bool((latest_reconciliation or {}).get("freeze_new_entries")):
        return str((latest_reconciliation or {}).get("classification") or "reconciliation_freeze")
    if state.open_broker_order_id is not None:
        return "pending_unresolved_order"
    if state.operator_halt:
        return "operator_halt"
    if not state.entries_enabled:
        return "entries_disabled"
    if state.position_side is not PositionSide.FLAT:
        return state.strategy_status.value
    return None


def _build_exit_parity_summary(
    *,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    latest_restore: dict[str, Any] | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    state_snapshot = _restore_validation_state_snapshot(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    latest_intent = dict(state_snapshot.get("latest_order_intent") or {})
    latest_fill = dict(state_snapshot.get("latest_fill") or {})
    exit_summary = dict(strategy_engine.latest_exit_decision_summary() or {})
    latest_intent_type = str(latest_intent.get("intent_type") or "").upper()
    latest_fill_type = str(latest_fill.get("intent_type") or "").upper()
    exit_fill_pending = bool(
        exit_summary.get("exit_fill_pending")
        or (
            latest_intent_type in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}
            and state_snapshot.get("position_side") != PositionSide.FLAT.value
            and state_snapshot.get("open_broker_order_id")
        )
    )
    exit_fill_confirmed = bool(
        exit_summary.get("exit_fill_confirmed")
        or (
            latest_fill_type in {"SELL_TO_CLOSE", "BUY_TO_CLOSE"}
            and state_snapshot.get("position_side") == PositionSide.FLAT.value
        )
    )
    current_position_family = (
        state_snapshot.get("long_entry_family", LongEntryFamily.NONE.value)
        if state_snapshot.get("position_side") == PositionSide.LONG.value
        else state_snapshot.get("short_entry_family", ShortEntryFamily.NONE.value)
        if state_snapshot.get("position_side") == PositionSide.SHORT.value
        else "NONE"
    )
    return {
        "generated_at": (observed_at or datetime.now(timezone.utc)).isoformat(),
        "position_side": state_snapshot.get("position_side"),
        "current_position_family": current_position_family,
        "long_entry_family": state_snapshot.get("long_entry_family", LongEntryFamily.NONE.value),
        "short_entry_family": state_snapshot.get("short_entry_family", ShortEntryFamily.NONE.value),
        "latest_exit_decision": exit_summary,
        "latest_order_intent": latest_intent,
        "latest_fill": latest_fill,
        "stop_refs": {
            "active_long_stop_ref": exit_summary.get("active_long_stop_ref"),
            "active_short_stop_ref": exit_summary.get("active_short_stop_ref"),
            "active_long_stop_ref_base": exit_summary.get("active_long_stop_ref_base"),
            "k_long_stop_ref_base": exit_summary.get("k_long_stop_ref_base"),
            "vwap_long_stop_ref_base": exit_summary.get("vwap_long_stop_ref_base"),
        },
        "break_even": {
            "long_break_even_armed": exit_summary.get("long_break_even_armed", strategy_engine.state.long_be_armed),
            "short_break_even_armed": exit_summary.get("short_break_even_armed", strategy_engine.state.short_be_armed),
        },
        "exit_fill_pending": exit_fill_pending,
        "exit_fill_confirmed": exit_fill_confirmed,
        "latest_restore_result": (latest_restore or {}).get("restore_result"),
        "summary_line": (
            f"family={current_position_family} | "
            f"primary_reason={exit_summary.get('primary_reason') or 'NONE'} | "
            f"pending={'YES' if exit_fill_pending else 'NO'} | "
            f"confirmed={'YES' if exit_fill_confirmed else 'NO'}"
        ),
    }


def _paper_soak_snapshot(
    *,
    repositories: RepositorySet,
    settings: StrategySettings,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    latest_reconciliation: dict[str, Any] | None = None,
    latest_restore: dict[str, Any] | None = None,
    latest_watchdog: dict[str, Any] | None = None,
    latest_heartbeat: dict[str, Any] | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    state_snapshot = _restore_validation_state_snapshot(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    latest_intent = state_snapshot.get("latest_order_intent") or {}
    latest_fill = state_snapshot.get("latest_fill") or {}
    latest_processed_end_ts = repositories.processed_bars.latest_end_ts()
    effective_now = observed_at or datetime.now(settings.timezone_info)
    market_data_ok = True
    if latest_processed_end_ts is not None:
        allowed_delay = timedelta(minutes=timeframe_minutes(settings.timeframe) * 2)
        market_data_ok = effective_now - latest_processed_end_ts <= allowed_delay
    return {
        "runtime_phase": _paper_soak_runtime_phase(
            strategy_engine,
            latest_reconciliation=latest_reconciliation,
            latest_watchdog=latest_watchdog,
        ),
        "strategy_state": strategy_engine.state.strategy_status.value,
        "last_processed_bar_id": strategy_engine.state.last_signal_bar_id,
        "last_processed_bar_end_ts": (
            latest_processed_end_ts.isoformat()
            if latest_processed_end_ts is not None
            else None
        ),
        "market_data_health": {
            "market_data_ok": market_data_ok,
            "latest_processed_bar_end_ts": latest_processed_end_ts.isoformat() if latest_processed_end_ts is not None else None,
            "evaluated_at": effective_now.isoformat(),
        },
        "position_state": {
            "side": strategy_engine.state.position_side.value,
            "internal_qty": strategy_engine.state.internal_position_qty,
            "broker_qty": strategy_engine.state.broker_position_qty,
            "entry_price": strategy_engine.state.entry_price,
            "open_entry_leg_count": len(strategy_engine.state.open_entry_legs),
            "open_add_count": max(0, len(strategy_engine.state.open_entry_legs) - 1),
            "open_entry_leg_quantities": [int(leg.quantity) for leg in strategy_engine.state.open_entry_legs],
            "open_entry_leg_prices": [str(leg.entry_price) for leg in strategy_engine.state.open_entry_legs],
            "additional_entry_allowed": strategy_engine._can_add_to_existing_position(strategy_engine.state),  # noqa: SLF001
            "participation_policy": strategy_engine._settings.participation_policy.value,  # noqa: SLF001
            "max_concurrent_entries": strategy_engine._settings.max_concurrent_entries,  # noqa: SLF001
            "max_position_quantity": strategy_engine._settings.max_position_quantity,  # noqa: SLF001
            "max_adds_after_entry": strategy_engine._settings.max_adds_after_entry,  # noqa: SLF001
        },
        "latest_order_intent": latest_intent,
        "latest_fill": latest_fill,
        "latest_reconcile_event": dict(latest_reconciliation or {}),
        "latest_heartbeat_reconciliation": dict(latest_heartbeat or {}),
        "latest_order_timeout_watchdog": dict(latest_watchdog or {}),
        "latest_restore_result": (latest_restore or {}).get("restore_result"),
        "entries_disabled_blocker": _paper_soak_entry_blocker(
            strategy_engine,
            latest_reconciliation=latest_reconciliation,
            latest_watchdog=latest_watchdog,
        ),
    }


def _paper_soak_validation_scenario(
    *,
    scenario_id: str,
    title: str,
    status: str,
    summary: dict[str, Any],
    detail: str,
    evidence: dict[str, Any],
) -> dict[str, Any]:
    return {
        "scenario_id": scenario_id,
        "title": title,
        "status": status,
        "detail": detail,
        "summary": summary,
        "evidence": evidence,
    }


def _run_probationary_paper_soak_clean_cycle(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "clean_cycle"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    bars = _paper_soak_validation_bars()
    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=scenario_dir,
        bars=bars,
    )
    startup_fault = runtime.restore_startup()
    latest_reconciliation: dict[str, Any] | None = None
    processed_bar_ids: list[str] = []
    for _ in range(len(bars) + 2):
        new_bars, reconciliation, _ = runtime.poll_and_process()
        if new_bars <= 0:
            break
        latest_reconciliation = dict(reconciliation)
        latest_processed = repositories.bars.list_recent_processed(symbol="MGC", timeframe="5m", limit=1)
        if latest_processed:
            processed_bar_ids.append(latest_processed[0].bar_id)
    restore_validation = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json")
    summary = _paper_soak_snapshot(
        repositories=repositories,
        settings=runtime.settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=latest_reconciliation,
        latest_restore=restore_validation,
    )
    intents = repositories.order_intents.list_all()
    fills = repositories.fills.list_all()
    passed = (
        startup_fault is None
        and strategy_engine.state.position_side is PositionSide.FLAT
        and len(intents) == 2
        and len(fills) == 2
        and repositories.processed_bars.count() == len(bars)
    )
    return _paper_soak_validation_scenario(
        scenario_id="clean_entry_exit_cycle",
        title="Clean Flat -> Entry -> Exit -> Flat",
        status="PASS" if passed else "FAIL",
        detail="Real engine bars produced exactly one entry, one exit, and returned the paper state to flat." if passed else "Clean paper cycle did not return to flat with exactly one entry and one exit.",
        summary=summary,
        evidence={
            "processed_bar_ids": processed_bar_ids,
            "processed_bar_count": repositories.processed_bars.count(),
            "order_intent_count": len(intents),
            "fill_count": len(fills),
            "fill_intent_types": [row.get("intent_type") for row in fills],
            "restore_validation": restore_validation,
        },
    )


def _run_probationary_paper_soak_restart_flat(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "restart_flat"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=scenario_dir,
        bars=_paper_soak_validation_bars(),
    )
    runtime.restore_startup()
    while True:
        new_bars, _reconciliation, _ = runtime.poll_and_process()
        if new_bars <= 0:
            break
    restarted_runtime, restarted_engine, restarted_execution, restarted_repositories, restarted_logger = (
        _build_probationary_paper_soak_validation_runtime(
            base_settings=base_settings,
            scenario_dir=scenario_dir,
            bars=(),
        )
    )
    startup_fault = restarted_runtime.restore_startup()
    restore_validation = _read_json(restarted_logger.artifact_dir / "restore_validation_latest.json")
    summary = _paper_soak_snapshot(
        repositories=restarted_repositories,
        settings=restarted_runtime.settings,
        strategy_engine=restarted_engine,
        execution_engine=restarted_execution,
        latest_restore=restore_validation,
    )
    passed = (
        startup_fault is None
        and restarted_engine.state.position_side is PositionSide.FLAT
        and restore_validation.get("restore_result") == "READY"
        and bool(restore_validation.get("duplicate_action_prevention_held")) is True
    )
    return _paper_soak_validation_scenario(
        scenario_id="restart_while_flat",
        title="Restart While Flat",
        status="PASS" if passed else "FAIL",
        detail="Restart restored persisted flat state before the next evaluation without creating duplicate activity." if passed else "Flat restart did not restore clean READY state.",
        summary=summary,
        evidence={"restore_validation": restore_validation},
    )


def _run_probationary_paper_soak_restart_pending(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "restart_pending"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    bars = _paper_soak_validation_bars()
    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=scenario_dir,
        bars=bars,
    )
    runtime.restore_startup()
    pending_reached = False
    for _ in range(len(bars) + 2):
        runtime.poll_and_process()
        if len(repositories.order_intents.list_all()) >= 1 and len(repositories.fills.list_all()) == 0:
            pending_reached = True
            break
    restarted_runtime, restarted_engine, restarted_execution, restarted_repositories, restarted_logger = (
        _build_probationary_paper_soak_validation_runtime(
            base_settings=base_settings,
            scenario_dir=scenario_dir,
            bars=(),
        )
    )
    startup_fault = restarted_runtime.restore_startup()
    restore_validation = _read_json(restarted_logger.artifact_dir / "restore_validation_latest.json")
    summary = _paper_soak_snapshot(
        repositories=restarted_repositories,
        settings=restarted_runtime.settings,
        strategy_engine=restarted_engine,
        execution_engine=restarted_execution,
        latest_restore=restore_validation,
    )
    passed = (
        startup_fault is None
        and pending_reached
        and len(restarted_execution.pending_executions()) == 1
        and restore_validation.get("restore_result") == "READY"
        and bool(restore_validation.get("duplicate_action_prevention_held")) is True
    )
    return _paper_soak_validation_scenario(
        scenario_id="restart_while_pending_order",
        title="Restart While Pending Acknowledged Order",
        status="PASS" if passed else "FAIL",
        detail="Restart restored the pending paper order without duplicating submission." if passed else "Pending-order restart did not preserve the unresolved order cleanly.",
        summary=summary,
        evidence={
            "pending_reached": pending_reached,
            "pending_execution_count": len(restarted_execution.pending_executions()),
            "restore_validation": restore_validation,
        },
    )


def _run_probationary_paper_soak_restart_in_position(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "restart_in_position"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    bars = _paper_soak_validation_bars()
    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=scenario_dir,
        bars=bars,
    )
    runtime.restore_startup()
    in_position_reached = False
    for _ in range(len(bars) + 2):
        runtime.poll_and_process()
        if strategy_engine.state.position_side is PositionSide.LONG:
            in_position_reached = True
            break
    restarted_runtime, restarted_engine, restarted_execution, restarted_repositories, restarted_logger = (
        _build_probationary_paper_soak_validation_runtime(
            base_settings=base_settings,
            scenario_dir=scenario_dir,
            bars=(),
        )
    )
    startup_fault = restarted_runtime.restore_startup()
    restore_validation = _read_json(restarted_logger.artifact_dir / "restore_validation_latest.json")
    summary = _paper_soak_snapshot(
        repositories=restarted_repositories,
        settings=restarted_runtime.settings,
        strategy_engine=restarted_engine,
        execution_engine=restarted_execution,
        latest_restore=restore_validation,
    )
    passed = (
        startup_fault is None
        and in_position_reached
        and restarted_engine.state.position_side is PositionSide.LONG
        and restarted_execution.broker.snapshot_state()["position_quantity"] == 1
        and bool(restore_validation.get("duplicate_action_prevention_held")) is True
    )
    return _paper_soak_validation_scenario(
        scenario_id="restart_while_in_position",
        title="Restart While In Position",
        status="PASS" if passed else "FAIL",
        detail="Restart restored the in-position paper state before the next evaluation." if passed else "In-position restart did not restore broker/internal state cleanly.",
        summary=summary,
        evidence={"in_position_reached": in_position_reached, "restore_validation": restore_validation},
    )


def _run_probationary_paper_soak_staged_participation(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "staged_participation"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=scenario_dir,
        bars=(),
        participation_policy=ParticipationPolicy.STAGED_SAME_DIRECTION,
        max_concurrent_entries=2,
        max_position_quantity=2,
        max_adds_after_entry=1,
    )
    startup_fault = runtime.restore_startup()
    bar_1 = _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 0, tzinfo=ZoneInfo("America/New_York")), "100", "101", "99", "100")
    bar_2 = _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 5, tzinfo=ZoneInfo("America/New_York")), "101", "102", "100", "101")
    bar_3 = _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 10, tzinfo=ZoneInfo("America/New_York")), "102", "103", "101", "102")

    intent_1 = strategy_engine.submit_runtime_entry_intent(
        bar_1,
        side="LONG",
        signal_source="paperSoakStageOne",
        reason_code="paperSoakStageOne",
        long_entry_family=LongEntryFamily.K,
    )
    assert intent_1 is not None
    _fill_pending_probationary_validation_intent(
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        order_intent_id=intent_1.order_intent_id,
        fill_price=Decimal("100"),
        fill_timestamp=bar_1.end_ts,
        signal_bar_id=bar_1.bar_id,
        long_entry_family=LongEntryFamily.K,
    )

    intent_2 = strategy_engine.submit_runtime_entry_intent(
        bar_2,
        side="LONG",
        signal_source="paperSoakStageTwo",
        reason_code="paperSoakStageTwo",
        long_entry_family=LongEntryFamily.K,
    )
    assert intent_2 is not None
    _fill_pending_probationary_validation_intent(
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        order_intent_id=intent_2.order_intent_id,
        fill_price=Decimal("101"),
        fill_timestamp=bar_2.end_ts,
        signal_bar_id=bar_2.bar_id,
        long_entry_family=LongEntryFamily.K,
    )

    rejected_add = strategy_engine.submit_runtime_entry_intent(
        bar_3,
        side="LONG",
        signal_source="paperSoakStageThree",
        reason_code="paperSoakStageThree",
        long_entry_family=LongEntryFamily.K,
    )
    summary = _paper_soak_snapshot(
        repositories=repositories,
        settings=runtime.settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    position_state = dict(summary.get("position_state") or {})
    passed = (
        startup_fault is None
        and intent_1 is not None
        and intent_2 is not None
        and rejected_add is None
        and strategy_engine.state.position_side is PositionSide.LONG
        and strategy_engine.state.internal_position_qty == 2
        and len(strategy_engine.state.open_entry_legs) == 2
        and position_state.get("additional_entry_allowed") is False
        and position_state.get("open_add_count") == 1
    )
    return _paper_soak_validation_scenario(
        scenario_id="staged_same_direction_participation",
        title="Staged same-direction entry/add with lane caps",
        status="PASS" if passed else "FAIL",
        detail="The paper runtime accepted an initial entry and one same-direction add, then refused the next add once the configured staged cap was reached." if passed else "Staged participation did not honor the configured add/cap rules.",
        summary=summary,
        evidence={
            "startup_fault": startup_fault,
            "first_intent_id": intent_1.order_intent_id if intent_1 is not None else None,
            "second_intent_id": intent_2.order_intent_id if intent_2 is not None else None,
            "third_add_rejected": rejected_add is None,
            "restore_validation": _read_json(lane_logger.artifact_dir / "restore_validation_latest.json"),
        },
    )


def _run_probationary_paper_soak_staged_partial_exit(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "staged_partial_exit"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    runtime, strategy_engine, execution_engine, repositories, _lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=scenario_dir,
        bars=(),
        participation_policy=ParticipationPolicy.PYRAMID_WITH_LIMIT,
        max_concurrent_entries=3,
        max_position_quantity=3,
        max_adds_after_entry=2,
    )
    startup_fault = runtime.restore_startup()
    entry_bars = [
        _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 15 + (index * 5), tzinfo=ZoneInfo("America/New_York")), str(100 + index), str(101 + index), str(99 + index), str(100 + index))
        for index in range(2)
    ]
    for index, bar in enumerate(entry_bars, start=1):
        intent = strategy_engine.submit_runtime_entry_intent(
            bar,
            side="LONG",
            signal_source=f"paperSoakPartialEntry{index}",
            reason_code=f"paperSoakPartialEntry{index}",
            long_entry_family=LongEntryFamily.K,
        )
        assert intent is not None
        _fill_pending_probationary_validation_intent(
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            order_intent_id=intent.order_intent_id,
            fill_price=Decimal(str(99 + index)),
            fill_timestamp=bar.end_ts,
            signal_bar_id=bar.bar_id,
            long_entry_family=LongEntryFamily.K,
        )

    exit_bar = _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 30, tzinfo=ZoneInfo("America/New_York")), "102", "103", "101", "102")
    partial_exit_intent = strategy_engine.submit_runtime_exit_intent(
        exit_bar.end_ts,
        quantity=1,
        reason_code="paperSoakPartialExit",
    )
    assert partial_exit_intent is not None
    _fill_pending_probationary_validation_intent(
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        order_intent_id=partial_exit_intent.order_intent_id,
        fill_price=Decimal("102"),
        fill_timestamp=exit_bar.end_ts,
    )

    summary = _paper_soak_snapshot(
        repositories=repositories,
        settings=runtime.settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    position_state = dict(summary.get("position_state") or {})
    passed = (
        startup_fault is None
        and strategy_engine.state.position_side is PositionSide.LONG
        and strategy_engine.state.internal_position_qty == 1
        and len(strategy_engine.state.open_entry_legs) == 1
        and position_state.get("open_entry_leg_count") == 1
        and position_state.get("additional_entry_allowed") is True
        and str((summary.get("latest_fill") or {}).get("intent_type") or "") == OrderIntentType.SELL_TO_CLOSE.value
    )
    return _paper_soak_validation_scenario(
        scenario_id="staged_partial_exit_preserves_remaining_exposure",
        title="Partial exit leaves staged exposure open",
        status="PASS" if passed else "FAIL",
        detail="A partial exit reduced the staged position to one remaining open leg without collapsing the lane back to flat." if passed else "Partial exit did not preserve the remaining staged exposure coherently.",
        summary=summary,
        evidence={
            "startup_fault": startup_fault,
            "latest_fill": summary.get("latest_fill"),
            "remaining_leg_quantities": [int(leg.quantity) for leg in strategy_engine.state.open_entry_legs],
        },
    )


def _run_probationary_paper_soak_restart_staged_in_position(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "restart_staged_in_position"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    replay_bars = _paper_soak_validation_bars()[:2]
    runtime, strategy_engine, execution_engine, repositories, _lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=scenario_dir,
        bars=replay_bars,
        participation_policy=ParticipationPolicy.PYRAMID_WITH_LIMIT,
        max_concurrent_entries=3,
        max_position_quantity=3,
        max_adds_after_entry=2,
    )
    runtime.restore_startup()
    while True:
        new_bars, _reconciliation, _ = runtime.poll_and_process()
        if new_bars <= 0:
            break
    processed_before_restart = repositories.processed_bars.count()

    bar_1 = _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 35, tzinfo=ZoneInfo("America/New_York")), "100", "101", "99", "100")
    bar_2 = _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 40, tzinfo=ZoneInfo("America/New_York")), "101", "102", "100", "101")
    for index, bar in enumerate((bar_1, bar_2), start=1):
        intent = strategy_engine.submit_runtime_entry_intent(
            bar,
            side="LONG",
            signal_source=f"paperSoakRestartEntry{index}",
            reason_code=f"paperSoakRestartEntry{index}",
            long_entry_family=LongEntryFamily.K,
        )
        assert intent is not None
        _fill_pending_probationary_validation_intent(
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            order_intent_id=intent.order_intent_id,
            fill_price=Decimal(str(99 + index)),
            fill_timestamp=bar.end_ts,
            signal_bar_id=bar.bar_id,
            long_entry_family=LongEntryFamily.K,
        )

    restarted_runtime, restarted_engine, restarted_execution, restarted_repositories, restarted_logger = (
        _build_probationary_paper_soak_validation_runtime(
            base_settings=base_settings,
            scenario_dir=scenario_dir,
            bars=replay_bars,
            participation_policy=ParticipationPolicy.PYRAMID_WITH_LIMIT,
            max_concurrent_entries=3,
            max_position_quantity=3,
            max_adds_after_entry=2,
        )
    )
    startup_fault = restarted_runtime.restore_startup()
    restore_validation = _read_json(restarted_logger.artifact_dir / "restore_validation_latest.json")
    restart_new_bars, latest_reconciliation, _ = restarted_runtime.poll_and_process()
    add_bar = _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 45, tzinfo=ZoneInfo("America/New_York")), "102", "103", "101", "102")
    add_intent = restarted_engine.submit_runtime_entry_intent(
        add_bar,
        side="LONG",
        signal_source="paperSoakRestartAdd",
        reason_code="paperSoakRestartAdd",
        long_entry_family=LongEntryFamily.K,
    )
    assert add_intent is not None
    _fill_pending_probationary_validation_intent(
        strategy_engine=restarted_engine,
        execution_engine=restarted_execution,
        order_intent_id=add_intent.order_intent_id,
        fill_price=Decimal("102"),
        fill_timestamp=add_bar.end_ts,
        signal_bar_id=add_bar.bar_id,
        long_entry_family=LongEntryFamily.K,
    )
    summary = _paper_soak_snapshot(
        repositories=restarted_repositories,
        settings=restarted_runtime.settings,
        strategy_engine=restarted_engine,
        execution_engine=restarted_execution,
        latest_reconciliation=latest_reconciliation,
        latest_restore=restore_validation,
    )
    position_state = dict(summary.get("position_state") or {})
    passed = (
        startup_fault is None
        and restore_validation.get("restore_result") == "READY"
        and bool(restore_validation.get("duplicate_action_prevention_held")) is True
        and restart_new_bars == 0
        and restarted_repositories.processed_bars.count() == processed_before_restart
        and position_state.get("open_entry_leg_count") == 3
        and position_state.get("additional_entry_allowed") is False
        and restarted_engine.state.internal_position_qty == 3
    )
    return _paper_soak_validation_scenario(
        scenario_id="restart_restores_staged_position_without_duplicate_bar_processing",
        title="Restart restores staged position and preserves add eligibility",
        status="PASS" if passed else "FAIL",
        detail="Restart restored staged open legs, suppressed already-processed finalized bars, and preserved correct add eligibility before the next add." if passed else "Restart did not restore staged exposure or duplicate-bar suppression cleanly.",
        summary=summary,
        evidence={
            "processed_before_restart": processed_before_restart,
            "processed_after_restart": restarted_repositories.processed_bars.count(),
            "restart_new_bars": restart_new_bars,
            "restore_validation": restore_validation,
        },
    )


def _run_probationary_paper_soak_duplicate_bar_suppression(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "duplicate_bar"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    settings = _build_probationary_paper_soak_validation_settings(
        base_settings=base_settings,
        database_path=scenario_dir / "validation.sqlite3",
        artifacts_dir=scenario_dir / "artifacts",
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    logger = StructuredLogger(settings.probationary_artifacts_path)
    engine = StrategyEngine(settings=settings, repositories=repositories, execution_engine=ExecutionEngine(broker=PaperBroker()), structured_logger=logger, alert_dispatcher=AlertDispatcher(logger))
    bar = _paper_soak_validation_bars()[0]
    engine.process_bar(bar)
    count_after_first = repositories.processed_bars.count()
    intents_after_first = len(repositories.order_intents.list_all())
    engine.process_bar(bar)
    count_after_second = repositories.processed_bars.count()
    intents_after_second = len(repositories.order_intents.list_all())
    summary = _paper_soak_snapshot(  # noqa: SLF001
        repositories=repositories,
        settings=settings,
        strategy_engine=engine,
        execution_engine=engine._execution_engine,
    )
    passed = count_after_first == count_after_second == 1 and intents_after_first == intents_after_second
    return _paper_soak_validation_scenario(
        scenario_id="duplicate_bar_suppression",
        title="Duplicate Completed Bar Suppression",
        status="PASS" if passed else "FAIL",
        detail="The engine ignored the duplicate finalized bar without reprocessing or creating a second intent." if passed else "Duplicate bar processing changed runtime state.",
        summary=summary,
        evidence={
            "count_after_first": count_after_first,
            "count_after_second": count_after_second,
            "order_intents_after_first": intents_after_first,
            "order_intents_after_second": intents_after_second,
            "bar_id": bar.bar_id,
        },
    )


def _run_probationary_paper_soak_out_of_order_rejection(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "out_of_order"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    settings = _build_probationary_paper_soak_validation_settings(
        base_settings=base_settings,
        database_path=scenario_dir / "validation.sqlite3",
        artifacts_dir=scenario_dir / "artifacts",
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    logger = StructuredLogger(settings.probationary_artifacts_path)
    engine = StrategyEngine(settings=settings, repositories=repositories, execution_engine=ExecutionEngine(broker=PaperBroker()), structured_logger=logger, alert_dispatcher=AlertDispatcher(logger))
    bars = _paper_soak_validation_bars()
    engine.process_bar(bars[2])
    error_text = None
    try:
        engine.process_bar(bars[1])
    except DeterminismError as exc:
        error_text = str(exc)
    summary = _paper_soak_snapshot(  # noqa: SLF001
        repositories=repositories,
        settings=settings,
        strategy_engine=engine,
        execution_engine=engine._execution_engine,
    )
    passed = error_text is not None and "Out-of-order bar rejected" in error_text
    return _paper_soak_validation_scenario(
        scenario_id="out_of_order_bar_rejected",
        title="Out-Of-Order Bar Rejection",
        status="PASS" if passed else "FAIL",
        detail="Older finalized bars are rejected instead of being replayed out of sequence." if passed else "Out-of-order bar was not rejected deterministically.",
        summary=summary,
        evidence={"error": error_text, "processed_bar_count": repositories.processed_bars.count()},
    )


def _run_probationary_paper_soak_timeout_reconciling(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "timeout_reconciling"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    settings = _build_probationary_paper_soak_validation_settings(
        base_settings=base_settings,
        database_path=scenario_dir / "validation.sqlite3",
        artifacts_dir=scenario_dir / "artifacts",
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(logger, repositories.alerts, source_subsystem="probationary_paper_soak_validation")
    execution_engine = ExecutionEngine(broker=PaperBroker())
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=alert_dispatcher,
    )
    bar = _paper_soak_validation_bars()[8]
    submitted_at = bar.end_ts - timedelta(seconds=settings.order_fill_timeout_seconds + settings.order_timeout_reconcile_grace_seconds + 5)
    intent = OrderIntent(
        order_intent_id=f"{bar.bar_id}|BUY_TO_OPEN",
        bar_id=bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=submitted_at,
        reason_code="bullSnap",
    )
    repositories.order_intents.save(
        intent,
        order_status=OrderStatus.ACKNOWLEDGED,
        broker_order_id="paper-timeout-1",
        submitted_at=submitted_at,
        acknowledged_at=submitted_at,
        broker_order_status=OrderStatus.ACKNOWLEDGED.value,
        last_status_checked_at=submitted_at,
        retry_count=0,
    )
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        open_broker_order_id="paper-timeout-1",
        last_order_intent_id=intent.order_intent_id,
    )
    strategy_engine._persist_state(strategy_engine.state, transition_label="seed_timeout_pending")  # noqa: SLF001
    _restore_paper_runtime_state(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    watchdog_status, reconciliation, _ = _run_order_timeout_watchdog(
        settings=settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=alert_dispatcher,
        watchdog_status=_initial_order_timeout_watchdog_status(settings),
        occurred_at=bar.end_ts,
    )
    summary = _paper_soak_snapshot(
        repositories=repositories,
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=reconciliation,
        latest_watchdog=watchdog_status,
        observed_at=bar.end_ts,
    )
    passed = (
        str((watchdog_status or {}).get("status") or "").upper() == "RECONCILING"
        and str(summary.get("runtime_phase") or "").upper() == "RECONCILING"
        and str(_nested_get(watchdog_status, "last_escalation", "timeout_classification") or "") == "fill_timeout_escalated"
        and str(summary.get("entries_disabled_blocker") or "") == "fill_timeout_escalated"
    )
    return _paper_soak_validation_scenario(
        scenario_id="missing_fill_acknowledgement_reconciling",
        title="Missing Fill Acknowledgement / Delayed Fill Truth",
        status="PASS" if passed else "FAIL",
        detail="An unresolved aged pending order failed closed into RECONCILING." if passed else "Delayed fill truth did not escalate to RECONCILING as expected.",
        summary=summary,
        evidence={"watchdog_status": watchdog_status, "reconciliation": reconciliation},
    )


def _run_probationary_paper_soak_safe_repair(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "safe_repair"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    settings = _build_probationary_paper_soak_validation_settings(
        base_settings=base_settings,
        database_path=scenario_dir / "validation.sqlite3",
        artifacts_dir=scenario_dir / "artifacts",
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(logger, repositories.alerts, source_subsystem="probationary_paper_soak_validation")
    execution_engine = ExecutionEngine(broker=PaperBroker())
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=alert_dispatcher,
    )
    bar = _paper_soak_validation_bars()[8]
    intent = OrderIntent(
        order_intent_id=f"{bar.bar_id}|BUY_TO_OPEN",
        bar_id=bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=bar.end_ts,
        reason_code="bullSnap",
    )
    repositories.order_intents.save(intent, order_status=OrderStatus.ACKNOWLEDGED, broker_order_id="paper-stale-1")
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        open_broker_order_id="paper-stale-1",
        last_order_intent_id=intent.order_intent_id,
        strategy_status=StrategyStatus.READY,
        position_side=PositionSide.FLAT,
        internal_position_qty=0,
        broker_position_qty=0,
    )
    strategy_engine._persist_state(strategy_engine.state, transition_label="seed_stale_pending")  # noqa: SLF001
    _restore_paper_runtime_state(repositories=repositories, strategy_engine=strategy_engine, execution_engine=execution_engine)
    execution_engine.broker.restore_state(
        position=PaperPosition(quantity=0, average_price=None),
        open_order_ids=[],
        order_status={},
        last_fill_timestamp=None,
    )
    reconciliation = _reconcile_paper_runtime(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        trigger="validation_safe_repair",
        apply_repairs=True,
    )
    summary = _paper_soak_snapshot(
        repositories=repositories,
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=reconciliation,
    )
    passed = (
        str(reconciliation.get("classification") or "").lower() == RECONCILIATION_CLASS_SAFE_REPAIR
        and strategy_engine.state.reconcile_required is False
        and strategy_engine.state.open_broker_order_id is None
    )
    return _paper_soak_validation_scenario(
        scenario_id="explainable_mismatch_safe_repair",
        title="Explainable Mismatch Safe Repair",
        status="PASS" if passed else "FAIL",
        detail="A stale flat pending marker was repaired automatically and returned to READY." if passed else "Explainable mismatch did not safe-repair cleanly.",
        summary=summary,
        evidence={"reconciliation": reconciliation},
    )


def _run_probationary_paper_soak_fault(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "fault"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    settings = _build_probationary_paper_soak_validation_settings(
        base_settings=base_settings,
        database_path=scenario_dir / "validation.sqlite3",
        artifacts_dir=scenario_dir / "artifacts",
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    logger = StructuredLogger(settings.probationary_artifacts_path)
    alert_dispatcher = AlertDispatcher(logger, repositories.alerts, source_subsystem="probationary_paper_soak_validation")
    execution_engine = ExecutionEngine(broker=PaperBroker())
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=alert_dispatcher,
    )
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        strategy_status=StrategyStatus.IN_LONG_K,
        position_side=PositionSide.LONG,
        internal_position_qty=0,
        broker_position_qty=1,
        entry_price=Decimal("100"),
    )
    bar = _paper_soak_validation_bars()[0]
    events = strategy_engine.process_bar(bar)
    summary = _paper_soak_snapshot(
        repositories=repositories,
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    passed = strategy_engine.state.strategy_status is StrategyStatus.FAULT and strategy_engine.state.fault_code is not None
    return _paper_soak_validation_scenario(
        scenario_id="persistence_invariant_fault",
        title="Persistence / Invariant Failure",
        status="PASS" if passed else "FAIL",
        detail="Invalid persisted state failed closed into FAULT." if passed else "Invalid state did not trip a strategy FAULT.",
        summary=summary,
        evidence={
            "fault_code": strategy_engine.state.fault_code,
            "event_types": [type(event).__name__ for event in events],
        },
    )


def _run_probationary_paper_soak_stale_missing_bar_handling(
    base_settings: StrategySettings,
    root_dir: Path,
) -> dict[str, Any]:
    scenario_dir = root_dir / "stale_missing_bar"
    _reset_probationary_paper_soak_scenario_dir(scenario_dir)
    settings = _build_probationary_paper_soak_validation_settings(
        base_settings=base_settings,
        database_path=scenario_dir / "validation.sqlite3",
        artifacts_dir=scenario_dir / "artifacts",
    )
    repositories = RepositorySet(build_engine(settings.database_url))
    logger = StructuredLogger(settings.probationary_artifacts_path)
    execution_engine = ExecutionEngine(broker=PaperBroker())
    strategy_engine = StrategyEngine(
        settings=settings,
        repositories=repositories,
        execution_engine=execution_engine,
        structured_logger=logger,
        alert_dispatcher=AlertDispatcher(logger),
    )
    stale_bar = _paper_soak_validation_bar(
        datetime(2026, 3, 26, 18, 0, tzinfo=ZoneInfo("America/New_York")),
        "99",
        "100",
        "97",
        "98",
    )
    repositories.processed_bars.mark_processed(stale_bar)
    summary = _paper_soak_snapshot(
        repositories=repositories,
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        observed_at=stale_bar.end_ts + timedelta(minutes=timeframe_minutes(settings.timeframe) * 3),
    )
    market_data_health = dict(summary.get("market_data_health") or {})
    passed = (
        market_data_health.get("market_data_ok") is False
        and summary.get("last_processed_bar_end_ts") == stale_bar.end_ts.isoformat()
        and repositories.processed_bars.count() == 1
    )
    return _paper_soak_validation_scenario(
        scenario_id="stale_missing_bar_handling",
        title="Stale / Missing-Bar Handling",
        status="PASS" if passed else "FAIL",
        detail=(
            "No new finalized bar beyond the allowed 5m cadence is surfaced as degraded runtime health without replaying duplicate work."
            if passed
            else "Stale completed-bar conditions were not surfaced in the soak summary."
        ),
        summary=summary,
        evidence={
            "market_data_health": market_data_health,
            "processed_bar_count": repositories.processed_bars.count(),
            "stale_bar_id": stale_bar.bar_id,
        },
    )


def _paper_soak_action_counts(repositories: RepositorySet) -> dict[str, int]:
    return {
        "order_intent_count": len(repositories.order_intents.list_all()),
        "fill_count": len(repositories.fills.list_all()),
    }


def _paper_soak_checkpoint_fingerprint(
    *,
    summary: dict[str, Any],
    state_snapshot: dict[str, Any],
) -> dict[str, Any]:
    latest_intent = dict(summary.get("latest_order_intent") or {})
    latest_fill = dict(summary.get("latest_fill") or {})
    latest_reconcile = dict(summary.get("latest_reconcile_event") or {})
    latest_watchdog = dict(summary.get("latest_order_timeout_watchdog") or {})
    latest_heartbeat = dict(summary.get("latest_heartbeat_reconciliation") or {})
    return {
        "runtime_phase": summary.get("runtime_phase"),
        "strategy_state": summary.get("strategy_state"),
        "last_processed_bar_id": summary.get("last_processed_bar_id"),
        "last_processed_bar_end_ts": summary.get("last_processed_bar_end_ts"),
        "position_state": dict(summary.get("position_state") or {}),
        "entries_disabled_blocker": summary.get("entries_disabled_blocker"),
        "latest_order_intent": {
            "order_intent_id": latest_intent.get("order_intent_id"),
            "intent_type": latest_intent.get("intent_type"),
            "order_status": latest_intent.get("order_status"),
            "broker_order_id": latest_intent.get("broker_order_id"),
        },
        "latest_fill": {
            "order_intent_id": latest_fill.get("order_intent_id"),
            "intent_type": latest_fill.get("intent_type"),
            "fill_timestamp": latest_fill.get("fill_timestamp"),
            "fill_price": latest_fill.get("fill_price"),
            "broker_order_id": latest_fill.get("broker_order_id"),
        },
        "latest_reconcile_classification": latest_reconcile.get("classification"),
        "latest_heartbeat_status": latest_heartbeat.get("status"),
        "latest_heartbeat_classification": latest_heartbeat.get("classification"),
        "latest_watchdog_status": latest_watchdog.get("status"),
        "latest_watchdog_timeout_classification": _nested_get(latest_watchdog, "last_escalation", "timeout_classification"),
        "open_broker_order_id": state_snapshot.get("open_broker_order_id"),
        "last_order_intent_id": state_snapshot.get("last_order_intent_id"),
        "pending_execution_count": state_snapshot.get("pending_execution_count"),
        "pending_broker_order_ids": list(state_snapshot.get("pending_broker_order_ids") or []),
        "latest_fill_timestamp": state_snapshot.get("latest_fill_timestamp"),
        "reconcile_required": state_snapshot.get("reconcile_required"),
        "fault_code": state_snapshot.get("fault_code"),
    }


def _paper_soak_drift_fields(before: dict[str, Any], after: dict[str, Any]) -> list[str]:
    return sorted(key for key in sorted(set(before) | set(after)) if before.get(key) != after.get(key))


def _paper_soak_summary_alignment_issues(summary: dict[str, Any], state_snapshot: dict[str, Any]) -> list[str]:
    issues: list[str] = []
    position_state = dict(summary.get("position_state") or {})
    latest_intent = dict(summary.get("latest_order_intent") or {})
    latest_fill = dict(summary.get("latest_fill") or {})
    if position_state.get("side") != state_snapshot.get("position_side"):
        issues.append("position_side")
    if position_state.get("internal_qty") != state_snapshot.get("internal_position_qty"):
        issues.append("internal_position_qty")
    if position_state.get("broker_qty") != state_snapshot.get("broker_position_qty"):
        issues.append("broker_position_qty")
    if position_state.get("entry_price") != state_snapshot.get("entry_price"):
        issues.append("entry_price")
    if latest_intent.get("order_intent_id") != _nested_get(state_snapshot, "latest_order_intent", "order_intent_id"):
        issues.append("latest_order_intent")
    if latest_fill.get("fill_timestamp") != _nested_get(state_snapshot, "latest_fill", "fill_timestamp"):
        issues.append("latest_fill")
    if summary.get("entries_disabled_blocker") == "pending_unresolved_order" and state_snapshot.get("open_broker_order_id") is None:
        issues.append("pending_blocker_without_open_order")
    return issues


def _paper_soak_capture_checkpoint(
    *,
    repositories: RepositorySet,
    settings: StrategySettings,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    latest_reconciliation: dict[str, Any] | None = None,
    latest_restore: dict[str, Any] | None = None,
    latest_watchdog: dict[str, Any] | None = None,
    latest_heartbeat: dict[str, Any] | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    summary = _paper_soak_snapshot(
        repositories=repositories,
        settings=settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=latest_reconciliation,
        latest_restore=latest_restore,
        latest_watchdog=latest_watchdog,
        latest_heartbeat=latest_heartbeat,
        observed_at=observed_at,
    )
    state_snapshot = _restore_validation_state_snapshot(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    alignment_issues = _paper_soak_summary_alignment_issues(summary, state_snapshot)
    return {
        "summary": summary,
        "state_snapshot": state_snapshot,
        "fingerprint": _paper_soak_checkpoint_fingerprint(summary=summary, state_snapshot=state_snapshot),
        "action_counts": _paper_soak_action_counts(repositories),
        "alignment_issues": alignment_issues,
    }


def _run_probationary_paper_soak_extended(
    config_paths: Sequence[str | Path],
) -> ProbationaryPaperSoakExtendedRun:
    base_settings = load_settings_from_files(config_paths)
    runtime_dir = base_settings.probationary_artifacts_path / "runtime"
    soak_dir = runtime_dir / "paper_soak_extended"
    _reset_probationary_paper_soak_scenario_dir(soak_dir)
    run_id = f"paper-soak-extended-{int(datetime.now(timezone.utc).timestamp() * 1000)}"
    bars = _paper_soak_extended_bars()

    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=soak_dir,
        bars=bars,
    )
    restart_rows: list[dict[str, Any]] = []
    latest_reconciliation: dict[str, Any] | None = None
    latest_watchdog: dict[str, Any] | None = None
    duplicate_bar_count = 0
    out_of_order_bar_count = 0

    startup_fault = runtime.restore_startup()
    initial_restore = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json")
    before_startup_restart = _paper_soak_capture_checkpoint(
        repositories=repositories,
        settings=runtime.settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_restore=initial_restore,
    )

    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=soak_dir,
        bars=bars,
    )
    startup_fault_after_restart = runtime.restore_startup()
    after_startup_restore = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json")
    after_startup_restart = _paper_soak_capture_checkpoint(
        repositories=repositories,
        settings=runtime.settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_restore=after_startup_restore,
    )
    startup_drift = _paper_soak_drift_fields(before_startup_restart["fingerprint"], after_startup_restart["fingerprint"])
    restart_rows.append(
        {
            "checkpoint_id": "flat_ready_startup",
            "trigger_state": "READY",
            "startup_fault": startup_fault_after_restart,
            "duplicate_action_prevention_held": before_startup_restart["action_counts"] == after_startup_restart["action_counts"],
            "drift_detected": bool(startup_drift),
            "drift_fields": startup_drift,
            "before": before_startup_restart["fingerprint"],
            "after": after_startup_restart["fingerprint"],
            "restore_result": after_startup_restore.get("restore_result"),
        }
    )

    pending_reached = False
    in_position_reached = False
    post_exit_restart_hit = False
    bars_processed_total = 0
    while True:
        try:
            new_bars, reconciliation, _ = runtime.poll_and_process()
        except DeterminismError:
            out_of_order_bar_count += 1
            break
        latest_reconciliation = dict(reconciliation)
        latest_watchdog = dict(runtime._order_timeout_watchdog)  # noqa: SLF001
        bars_processed_total = repositories.processed_bars.count()
        if new_bars <= 0:
            break
        if not pending_reached and strategy_engine.state.open_broker_order_id is not None and strategy_engine.state.position_side is PositionSide.FLAT:
            pending_reached = True
            before = _paper_soak_capture_checkpoint(
                repositories=repositories,
                settings=runtime.settings,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
            )
            before_counts = before["action_counts"]
            runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
                base_settings=base_settings,
                scenario_dir=soak_dir,
                bars=bars,
            )
            runtime.restore_startup()
            restore_validation = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json")
            after = _paper_soak_capture_checkpoint(
                repositories=repositories,
                settings=runtime.settings,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
                latest_restore=restore_validation,
            )
            drift_fields = _paper_soak_drift_fields(before["fingerprint"], after["fingerprint"])
            restart_rows.append(
                {
                    "checkpoint_id": "pending_acknowledged_order",
                    "trigger_state": "PENDING_ORDER",
                    "startup_fault": None,
                    "duplicate_action_prevention_held": before_counts == after["action_counts"],
                    "drift_detected": bool(drift_fields),
                    "drift_fields": drift_fields,
                    "before": before["fingerprint"],
                    "after": after["fingerprint"],
                    "restore_result": restore_validation.get("restore_result"),
                }
            )
            continue

        if pending_reached and not in_position_reached and strategy_engine.state.position_side is PositionSide.LONG:
            in_position_reached = True
            before = _paper_soak_capture_checkpoint(
                repositories=repositories,
                settings=runtime.settings,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
            )
            before_counts = before["action_counts"]
            runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
                base_settings=base_settings,
                scenario_dir=soak_dir,
                bars=bars,
            )
            runtime.restore_startup()
            restore_validation = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json")
            after = _paper_soak_capture_checkpoint(
                repositories=repositories,
                settings=runtime.settings,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
                latest_restore=restore_validation,
            )
            drift_fields = _paper_soak_drift_fields(before["fingerprint"], after["fingerprint"])
            restart_rows.append(
                {
                    "checkpoint_id": "in_position_restart",
                    "trigger_state": "IN_POSITION",
                    "startup_fault": None,
                    "duplicate_action_prevention_held": before_counts == after["action_counts"],
                    "drift_detected": bool(drift_fields),
                    "drift_fields": drift_fields,
                    "before": before["fingerprint"],
                    "after": after["fingerprint"],
                    "restore_result": restore_validation.get("restore_result"),
                }
            )
            continue

        if in_position_reached and not post_exit_restart_hit and strategy_engine.state.position_side is PositionSide.FLAT:
            post_exit_restart_hit = True
            before = _paper_soak_capture_checkpoint(
                repositories=repositories,
                settings=runtime.settings,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
            )
            before_counts = before["action_counts"]
            runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
                base_settings=base_settings,
                scenario_dir=soak_dir,
                bars=bars,
            )
            runtime.restore_startup()
            restore_validation = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json")
            after = _paper_soak_capture_checkpoint(
                repositories=repositories,
                settings=runtime.settings,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
                latest_restore=restore_validation,
            )
            drift_fields = _paper_soak_drift_fields(before["fingerprint"], after["fingerprint"])
            restart_rows.append(
                {
                    "checkpoint_id": "post_exit_fill_restart",
                    "trigger_state": "FLAT_AFTER_EXIT",
                    "startup_fault": None,
                    "duplicate_action_prevention_held": before_counts == after["action_counts"],
                    "drift_detected": bool(drift_fields),
                    "drift_fields": drift_fields,
                    "before": before["fingerprint"],
                    "after": after["fingerprint"],
                    "restore_result": restore_validation.get("restore_result"),
                }
            )

    degraded_bar = bars[-1]
    submitted_at = degraded_bar.end_ts - timedelta(
        seconds=runtime.settings.order_fill_timeout_seconds + runtime.settings.order_timeout_reconcile_grace_seconds + 5
    )
    degraded_intent = OrderIntent(
        order_intent_id=f"{degraded_bar.bar_id}|BUY_TO_OPEN|extended_timeout",
        bar_id=degraded_bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=submitted_at,
        reason_code="extendedSoakTimeout",
    )
    repositories.order_intents.save(
        degraded_intent,
        order_status=OrderStatus.ACKNOWLEDGED,
        broker_order_id="paper-extended-timeout-1",
        submitted_at=submitted_at,
        acknowledged_at=submitted_at,
        broker_order_status=OrderStatus.ACKNOWLEDGED.value,
        last_status_checked_at=submitted_at,
        retry_count=0,
    )
    strategy_engine._state = replace(
        strategy_engine.state,
        open_broker_order_id="paper-extended-timeout-1",
        last_order_intent_id=degraded_intent.order_intent_id,
    )
    strategy_engine._persist_state(strategy_engine.state, transition_label="seed_extended_timeout_pending")
    _restore_paper_runtime_state(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    latest_watchdog, latest_reconciliation, _ = _run_order_timeout_watchdog(
        settings=runtime.settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=lane_logger,
        alert_dispatcher=runtime.alert_dispatcher,
        watchdog_status=_initial_order_timeout_watchdog_status(runtime.settings),
        occurred_at=degraded_bar.end_ts + timedelta(seconds=runtime.settings.order_fill_timeout_seconds + runtime.settings.order_timeout_reconcile_grace_seconds + 10),
    )
    before = _paper_soak_capture_checkpoint(
        repositories=repositories,
        settings=runtime.settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=latest_reconciliation,
        latest_watchdog=latest_watchdog,
    )
    before_counts = before["action_counts"]
    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=soak_dir,
        bars=bars,
    )
    runtime.restore_startup()
    restore_validation = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json")
    latest_watchdog, latest_reconciliation, _ = _run_order_timeout_watchdog(
        settings=runtime.settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=lane_logger,
        alert_dispatcher=runtime.alert_dispatcher,
        watchdog_status=_initial_order_timeout_watchdog_status(runtime.settings),
        occurred_at=degraded_bar.end_ts + timedelta(seconds=runtime.settings.order_fill_timeout_seconds + runtime.settings.order_timeout_reconcile_grace_seconds + 10),
    )
    after = _paper_soak_capture_checkpoint(
        repositories=repositories,
        settings=runtime.settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=latest_reconciliation,
        latest_restore=restore_validation,
        latest_watchdog=latest_watchdog,
    )
    degraded_drift = _paper_soak_drift_fields(before["fingerprint"], after["fingerprint"])
    restart_rows.append(
        {
            "checkpoint_id": "degraded_watchdog_restart",
            "trigger_state": "RECONCILING",
            "startup_fault": None,
            "duplicate_action_prevention_held": before_counts == after["action_counts"],
            "drift_detected": bool(degraded_drift),
            "drift_fields": degraded_drift,
            "before": before["fingerprint"],
            "after": after["fingerprint"],
            "restore_result": restore_validation.get("restore_result"),
        }
    )

    final_snapshot = _paper_soak_capture_checkpoint(
        repositories=repositories,
        settings=runtime.settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=latest_reconciliation,
        latest_restore=restore_validation,
        latest_watchdog=latest_watchdog,
    )
    drift_rows = [row for row in restart_rows if row.get("drift_detected")]
    reconcile_rows = _read_jsonl(lane_logger.artifact_dir / "reconciliation_events.jsonl")
    safe_repair_count = sum(
        1
        for row in reconcile_rows
        if str(row.get("classification") or "").lower() == RECONCILIATION_CLASS_SAFE_REPAIR
    )
    fault_count = len(_read_jsonl(lane_logger.artifact_dir / "fault_events.jsonl"))
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope_label": "MGC 5m extended paper soak",
        "operator_path": "mgc-v05l probationary-paper-soak-extended",
        "allowed_scope": {
            "symbol": "MGC",
            "timeframe": "5m",
            "mode": "PAPER",
            "evaluation": "completed_bar_only",
            "processing": "deterministic_sequential",
        },
        "summary": {
            "result": "PASS" if startup_fault is None and not drift_rows and fault_count == 0 else "FAIL",
            "run_id": run_id,
            "bars_processed": repositories.processed_bars.count(),
            "restart_count": len(restart_rows),
            "restart_points_hit": [row["checkpoint_id"] for row in restart_rows],
            "duplicate_bar_count": duplicate_bar_count,
            "out_of_order_bar_count": out_of_order_bar_count,
            "reconcile_count": len(reconcile_rows),
            "safe_repair_count": safe_repair_count,
            "fault_count": fault_count,
            "drift_detected": bool(drift_rows),
            "final_runtime_phase": final_snapshot["summary"].get("runtime_phase"),
            "final_strategy_state": final_snapshot["summary"].get("strategy_state"),
            "final_position_state": final_snapshot["summary"].get("position_state"),
            "final_restore_result": final_snapshot["summary"].get("latest_restore_result"),
            "final_entry_blocker": final_snapshot["summary"].get("entries_disabled_blocker"),
        },
        "checkpoint_rows": restart_rows,
        "final_snapshot": final_snapshot["summary"],
    }
    logger = StructuredLogger(soak_dir)
    json_path = logger.write_paper_soak_extended_state(payload)
    logger.log_paper_soak_extended_event(payload)
    markdown_path = soak_dir / "paper_soak_extended_latest.md"
    markdown_lines = [
        "# Extended Paper Soak",
        "",
        f"- Run ID: `{run_id}`",
        f"- Result: `{payload['summary']['result']}`",
        f"- Bars processed: `{payload['summary']['bars_processed']}`",
        f"- Restarts: `{payload['summary']['restart_count']}`",
        f"- Drift detected: `{payload['summary']['drift_detected']}`",
        "",
        "## Restart Checkpoints",
    ]
    for row in restart_rows:
        markdown_lines.append(
            f"- `{row['checkpoint_id']}` `{row['trigger_state']}` drift={row['drift_detected']} duplicate_actions_held={row['duplicate_action_prevention_held']}"
        )
    markdown_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    return ProbationaryPaperSoakExtendedRun(
        artifact_path=str(json_path),
        markdown_path=str(markdown_path),
        summary=payload,
    )


def _run_probationary_paper_soak_unattended(
    config_paths: Sequence[str | Path],
    *,
    inject_drift_checkpoint: str | None = None,
) -> ProbationaryPaperSoakUnattendedRun:
    base_settings = load_settings_from_files(config_paths)
    runtime_dir = base_settings.probationary_artifacts_path / "runtime"
    soak_dir = runtime_dir / "paper_soak_unattended"
    _reset_probationary_paper_soak_scenario_dir(soak_dir)
    run_id = f"paper-soak-unattended-{int(datetime.now(timezone.utc).timestamp() * 1000)}"
    bars = _paper_soak_unattended_bars()

    runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
        base_settings=base_settings,
        scenario_dir=soak_dir,
        bars=bars,
    )
    restart_rows: list[dict[str, Any]] = []
    latest_reconciliation: dict[str, Any] | None = None
    latest_watchdog: dict[str, Any] | None = None
    latest_heartbeat = dict(runtime._heartbeat_reconciliation)  # noqa: SLF001
    duplicate_bar_count = 0
    out_of_order_bar_count = 0
    stale_bar_count = 0
    heartbeat_checkpoint_hit = False
    pending_reached = False
    in_position_reached = False
    post_exit_restart_hit = False
    ready_mid_soak_restart_hit = False
    prior_bar_end_ts: datetime | None = None

    def _checkpoint_observed_at() -> datetime:
        latest_end = repositories.processed_bars.latest_end_ts()
        return latest_end or datetime.now(timezone.utc)

    def _restart_checkpoint(
        *,
        checkpoint_id: str,
        trigger_state: str,
        observed_at: datetime,
        replay_heartbeat: bool = False,
        replay_watchdog: bool = False,
    ) -> None:
        nonlocal runtime, strategy_engine, execution_engine, repositories, lane_logger
        nonlocal latest_reconciliation, latest_watchdog, latest_heartbeat, stale_bar_count
        before = _paper_soak_capture_checkpoint(
            repositories=repositories,
            settings=runtime.settings,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            latest_reconciliation=latest_reconciliation,
            latest_restore=None,
            latest_watchdog=latest_watchdog,
            latest_heartbeat=latest_heartbeat,
            observed_at=observed_at,
        )
        before_counts = before["action_counts"]

        runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
            base_settings=base_settings,
            scenario_dir=soak_dir,
            bars=bars,
        )
        runtime.restore_startup()
        restore_validation = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json")

        if replay_heartbeat:
            latest_heartbeat, reconciliation, _ = _run_reconciliation_heartbeat(
                settings=runtime.settings,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
                heartbeat_status=_initial_reconciliation_heartbeat_status(runtime.settings.reconciliation_heartbeat_interval_seconds),
                occurred_at=observed_at,
            )
            runtime._heartbeat_reconciliation = latest_heartbeat  # noqa: SLF001
            if reconciliation is not None:
                latest_reconciliation = reconciliation

        if replay_watchdog:
            latest_watchdog, reconciliation, _ = _run_order_timeout_watchdog(
                settings=runtime.settings,
                repositories=repositories,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
                structured_logger=lane_logger,
                alert_dispatcher=runtime.alert_dispatcher,
                watchdog_status=_initial_order_timeout_watchdog_status(runtime.settings),
                occurred_at=observed_at,
            )
            runtime._order_timeout_watchdog = latest_watchdog  # noqa: SLF001
            if reconciliation is not None:
                latest_reconciliation = reconciliation

        if inject_drift_checkpoint == checkpoint_id:
            strategy_engine._state = replace(  # noqa: SLF001
                strategy_engine.state,
                fault_code="paper_soak_injected_drift",
                reconcile_required=True,
                strategy_status=StrategyStatus.RECONCILING,
            )
            strategy_engine._persist_state(strategy_engine.state, transition_label="inject_paper_soak_drift")  # noqa: SLF001

        after = _paper_soak_capture_checkpoint(
            repositories=repositories,
            settings=runtime.settings,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            latest_reconciliation=latest_reconciliation,
            latest_restore=restore_validation,
            latest_watchdog=latest_watchdog,
            latest_heartbeat=latest_heartbeat,
            observed_at=observed_at,
        )
        if after["summary"]["market_data_health"].get("market_data_ok") is False:
            stale_bar_count += 1
        drift_fields = _paper_soak_drift_fields(before["fingerprint"], after["fingerprint"])
        after_open_order_id = str(after["state_snapshot"].get("open_broker_order_id") or "")
        prior_filled_order_id = str(_nested_get(before["state_snapshot"], "latest_fill", "broker_order_id") or "")
        bar_before = _parse_iso_datetime_or_none(before["fingerprint"].get("last_processed_bar_end_ts"))
        bar_after = _parse_iso_datetime_or_none(after["fingerprint"].get("last_processed_bar_end_ts"))
        bar_chronology_drift = bool(bar_before and bar_after and bar_after < bar_before)
        filled_state_loss = (
            dict(before["fingerprint"].get("position_state") or {}).get("side") == PositionSide.LONG.value
            and dict(after["fingerprint"].get("position_state") or {}).get("side") != PositionSide.LONG.value
            and checkpoint_id == "in_position_restart"
        )
        reopened_resolved_order = bool(prior_filled_order_id and after_open_order_id and after_open_order_id == prior_filled_order_id)
        duplicate_action_held = before_counts == after["action_counts"]
        summary_alignment_held = not after["alignment_issues"]
        restart_rows.append(
            {
                "checkpoint_id": checkpoint_id,
                "trigger_state": trigger_state,
                "duplicate_action_prevention_held": duplicate_action_held,
                "drift_detected": bool(drift_fields or after["alignment_issues"]),
                "drift_fields": drift_fields,
                "summary_alignment_held": summary_alignment_held,
                "summary_alignment_issues": list(after["alignment_issues"]),
                "reopened_resolved_order": reopened_resolved_order,
                "filled_state_loss": filled_state_loss,
                "bar_chronology_drift": bar_chronology_drift,
                "restore_result": restore_validation.get("restore_result"),
                "before": before["fingerprint"],
                "after": after["fingerprint"],
            }
        )

    startup_fault = runtime.restore_startup()
    startup_restore = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json")
    latest_heartbeat = dict(runtime._heartbeat_reconciliation)  # noqa: SLF001
    _restart_checkpoint(
        checkpoint_id="flat_ready_startup",
        trigger_state="READY",
        observed_at=_checkpoint_observed_at(),
    )

    while True:
        try:
            new_bars, reconciliation, _ = runtime.poll_and_process()
        except DeterminismError:
            out_of_order_bar_count += 1
            break
        latest_reconciliation = dict(reconciliation)
        latest_watchdog = dict(runtime._order_timeout_watchdog)  # noqa: SLF001
        latest_heartbeat = dict(runtime._heartbeat_reconciliation)  # noqa: SLF001
        latest_end = repositories.processed_bars.latest_end_ts()
        if latest_end is not None:
            if prior_bar_end_ts is not None and latest_end == prior_bar_end_ts:
                duplicate_bar_count += 1
            if prior_bar_end_ts is not None and latest_end < prior_bar_end_ts:
                out_of_order_bar_count += 1
            prior_bar_end_ts = latest_end
        if new_bars <= 0:
            break
        observed_at = _checkpoint_observed_at()
        if _paper_soak_capture_checkpoint(
            repositories=repositories,
            settings=runtime.settings,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            latest_reconciliation=latest_reconciliation,
            latest_watchdog=latest_watchdog,
            latest_heartbeat=latest_heartbeat,
            observed_at=observed_at,
        )["summary"]["market_data_health"].get("market_data_ok") is False:
            stale_bar_count += 1

        if not heartbeat_checkpoint_hit and _parse_iso_datetime_or_none(latest_heartbeat.get("last_completed_at")) is not None:
            heartbeat_checkpoint_hit = True
            _restart_checkpoint(
                checkpoint_id="heartbeat_reconcile_restart",
                trigger_state="HEARTBEAT_RECONCILE",
                observed_at=observed_at,
                replay_heartbeat=True,
            )
            continue

        if not pending_reached and strategy_engine.state.open_broker_order_id is not None and strategy_engine.state.position_side is PositionSide.FLAT:
            pending_reached = True
            _restart_checkpoint(
                checkpoint_id="pending_acknowledged_order",
                trigger_state="PENDING_ORDER",
                observed_at=observed_at,
            )
            continue

        if pending_reached and not in_position_reached and strategy_engine.state.position_side is PositionSide.LONG:
            in_position_reached = True
            _restart_checkpoint(
                checkpoint_id="in_position_restart",
                trigger_state="IN_POSITION",
                observed_at=observed_at,
            )
            continue

        if (
            in_position_reached
            and not post_exit_restart_hit
            and strategy_engine.state.position_side is PositionSide.FLAT
            and strategy_engine.state.open_broker_order_id is None
        ):
            post_exit_restart_hit = True
            _restart_checkpoint(
                checkpoint_id="post_exit_fill_restart",
                trigger_state="FLAT_AFTER_EXIT",
                observed_at=observed_at,
            )
            continue

        if (
            not ready_mid_soak_restart_hit
            and repositories.processed_bars.count() >= 24
            and strategy_engine.state.position_side is PositionSide.FLAT
            and strategy_engine.state.open_broker_order_id is None
            and strategy_engine.state.reconcile_required is False
            and strategy_engine.state.strategy_status is StrategyStatus.READY
        ):
            ready_mid_soak_restart_hit = True
            _restart_checkpoint(
                checkpoint_id="ready_mid_soak_restart",
                trigger_state="READY",
                observed_at=observed_at,
            )
            continue

    degraded_bar = bars[-1]
    submitted_at = degraded_bar.end_ts - timedelta(
        seconds=runtime.settings.order_fill_timeout_seconds + runtime.settings.order_timeout_reconcile_grace_seconds + 5
    )
    degraded_intent = OrderIntent(
        order_intent_id=f"{degraded_bar.bar_id}|BUY_TO_OPEN|unattended_timeout",
        bar_id=degraded_bar.bar_id,
        symbol="MGC",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        quantity=1,
        created_at=submitted_at,
        reason_code="unattendedSoakTimeout",
    )
    repositories.order_intents.save(
        degraded_intent,
        order_status=OrderStatus.ACKNOWLEDGED,
        broker_order_id="paper-unattended-timeout-1",
        submitted_at=submitted_at,
        acknowledged_at=submitted_at,
        broker_order_status=OrderStatus.ACKNOWLEDGED.value,
        last_status_checked_at=submitted_at,
        retry_count=0,
    )
    strategy_engine._state = replace(  # noqa: SLF001
        strategy_engine.state,
        open_broker_order_id="paper-unattended-timeout-1",
        last_order_intent_id=degraded_intent.order_intent_id,
    )
    strategy_engine._persist_state(strategy_engine.state, transition_label="seed_unattended_timeout_pending")  # noqa: SLF001
    _restore_paper_runtime_state(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    watchdog_observed_at = degraded_bar.end_ts + timedelta(
        seconds=runtime.settings.order_fill_timeout_seconds + runtime.settings.order_timeout_reconcile_grace_seconds + 10
    )
    latest_watchdog, latest_reconciliation, _ = _run_order_timeout_watchdog(
        settings=runtime.settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=lane_logger,
        alert_dispatcher=runtime.alert_dispatcher,
        watchdog_status=_initial_order_timeout_watchdog_status(runtime.settings),
        occurred_at=watchdog_observed_at,
    )
    runtime._order_timeout_watchdog = latest_watchdog  # noqa: SLF001
    _restart_checkpoint(
        checkpoint_id="degraded_watchdog_restart",
        trigger_state="RECONCILING",
        observed_at=watchdog_observed_at,
        replay_watchdog=True,
    )

    final_restore = _read_json(lane_logger.artifact_dir / "restore_validation_latest.json") or startup_restore
    final_snapshot = _paper_soak_capture_checkpoint(
        repositories=repositories,
        settings=runtime.settings,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        latest_reconciliation=latest_reconciliation,
        latest_restore=final_restore,
        latest_watchdog=latest_watchdog,
        latest_heartbeat=latest_heartbeat,
        observed_at=watchdog_observed_at,
    )
    if final_snapshot["summary"]["market_data_health"].get("market_data_ok") is False:
        stale_bar_count += 1
    reconcile_rows = _read_jsonl(lane_logger.artifact_dir / "reconciliation_events.jsonl")
    safe_repair_count = sum(
        1
        for row in reconcile_rows
        if str(row.get("classification") or "").lower() == RECONCILIATION_CLASS_SAFE_REPAIR
    )
    fault_count = len(_read_jsonl(lane_logger.artifact_dir / "fault_events.jsonl"))
    drift_rows = [
        row
        for row in restart_rows
        if row.get("drift_detected")
        or row.get("duplicate_action_prevention_held") is False
        or row.get("reopened_resolved_order") is True
        or row.get("filled_state_loss") is True
        or row.get("bar_chronology_drift") is True
        or row.get("summary_alignment_held") is False
    ]
    first_bar = bars[0]
    last_bar = bars[-1]
    soak_window_minutes = int((last_bar.end_ts - first_bar.end_ts).total_seconds() // 60)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope_label": "MGC 5m unattended paper soak",
        "operator_path": "mgc-v05l probationary-paper-soak-unattended",
        "allowed_scope": {
            "symbol": "MGC",
            "timeframe": "5m",
            "mode": "PAPER",
            "evaluation": "completed_bar_only",
            "processing": "deterministic_sequential",
        },
        "summary": {
            "result": "PASS" if startup_fault is None and not drift_rows and fault_count == 0 else "FAIL",
            "run_id": run_id,
            "bars_processed": repositories.processed_bars.count(),
            "runtime_duration_minutes": soak_window_minutes,
            "soak_window_start": first_bar.end_ts.isoformat(),
            "soak_window_end": last_bar.end_ts.isoformat(),
            "restart_count": len(restart_rows),
            "restart_points_hit": [row["checkpoint_id"] for row in restart_rows],
            "duplicate_bar_count": duplicate_bar_count,
            "out_of_order_bar_count": out_of_order_bar_count,
            "stale_bar_count": stale_bar_count,
            "reconcile_count": len(reconcile_rows),
            "safe_repair_count": safe_repair_count,
            "fault_count": fault_count,
            "drift_detected": bool(drift_rows),
            "duplicate_action_drift_count": sum(1 for row in restart_rows if row.get("duplicate_action_prevention_held") is False),
            "reopened_resolved_order_count": sum(1 for row in restart_rows if row.get("reopened_resolved_order") is True),
            "filled_state_loss_count": sum(1 for row in restart_rows if row.get("filled_state_loss") is True),
            "bar_chronology_drift_count": sum(1 for row in restart_rows if row.get("bar_chronology_drift") is True),
            "summary_alignment_issue_count": sum(1 for row in restart_rows if row.get("summary_alignment_held") is False),
            "final_runtime_phase": final_snapshot["summary"].get("runtime_phase"),
            "final_strategy_state": final_snapshot["summary"].get("strategy_state"),
            "final_position_state": final_snapshot["summary"].get("position_state"),
            "final_restore_result": final_snapshot["summary"].get("latest_restore_result"),
            "final_entry_blocker": final_snapshot["summary"].get("entries_disabled_blocker"),
        },
        "checkpoint_rows": restart_rows,
        "final_snapshot": final_snapshot["summary"],
    }
    logger = StructuredLogger(soak_dir)
    json_path = logger.write_paper_soak_unattended_state(payload)
    logger.log_paper_soak_unattended_event(payload)
    markdown_path = soak_dir / "paper_soak_unattended_latest.md"
    markdown_lines = [
        "# Unattended Paper Soak",
        "",
        f"- Run ID: `{run_id}`",
        f"- Result: `{payload['summary']['result']}`",
        f"- Bars processed: `{payload['summary']['bars_processed']}`",
        f"- Runtime duration (minutes): `{payload['summary']['runtime_duration_minutes']}`",
        f"- Restarts: `{payload['summary']['restart_count']}`",
        f"- Drift detected: `{payload['summary']['drift_detected']}`",
        "",
        "## Restart Checkpoints",
    ]
    for row in restart_rows:
        markdown_lines.append(
            f"- `{row['checkpoint_id']}` `{row['trigger_state']}` drift={row['drift_detected']} duplicate_actions_held={row['duplicate_action_prevention_held']} summary_alignment_held={row['summary_alignment_held']}"
        )
    markdown_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    return ProbationaryPaperSoakUnattendedRun(
        artifact_path=str(json_path),
        markdown_path=str(markdown_path),
        summary=payload,
    )


def _live_timing_validation_submit_bar(end_ts: datetime) -> Bar:
    return _paper_soak_validation_bar(end_ts, "100", "101", "99", "100")


def _live_timing_validation_summary(
    *,
    runtime: ProbationaryPaperLaneRuntime,
    latest_reconciliation: dict[str, Any] | None = None,
    observed_at: datetime | None = None,
) -> dict[str, Any]:
    return _build_live_timing_summary(
        settings=runtime.settings,
        repositories=runtime.repositories,
        strategy_engine=runtime.strategy_engine,
        execution_engine=runtime.execution_engine,
        latest_reconciliation=latest_reconciliation or runtime._last_reconciliation_payload,  # noqa: SLF001
        latest_watchdog=runtime._order_timeout_watchdog,  # noqa: SLF001
        latest_restore=runtime._startup_restore_validation,  # noqa: SLF001
        observed_at=observed_at or datetime.now(timezone.utc),
    )


def _run_probationary_live_timing_validation(
    config_paths: Sequence[str | Path],
) -> ProbationaryLiveTimingValidationRun:
    base_settings = load_settings_from_files(config_paths)
    runtime_dir = base_settings.probationary_artifacts_path / "runtime"
    validation_dir = runtime_dir / "paper_live_timing_validation"
    _reset_probationary_paper_soak_scenario_dir(validation_dir)
    run_id = f"paper-live-timing-{int(datetime.now(timezone.utc).timestamp() * 1000)}"
    ny = ZoneInfo("America/New_York")

    def _fresh_runtime(
        scenario_id: str,
        *,
        bars: Sequence[Bar],
        broker: _LiveTimingValidationBroker | None = None,
    ) -> tuple[ProbationaryPaperLaneRuntime, StrategyEngine, ExecutionEngine, RepositorySet, StructuredLogger, _LiveTimingValidationBroker]:
        live_broker = broker or _LiveTimingValidationBroker()
        live_broker.connect()
        runtime, strategy_engine, execution_engine, repositories, lane_logger = _build_probationary_paper_soak_validation_runtime(
            base_settings=base_settings,
            scenario_dir=validation_dir / scenario_id,
            bars=bars,
            broker=live_broker,
        )
        return runtime, strategy_engine, execution_engine, repositories, lane_logger, live_broker

    scenario_rows: list[dict[str, Any]] = []

    runtime, strategy_engine, execution_engine, repositories, lane_logger, broker = _fresh_runtime(
        "submit_after_completed_bar_close",
        bars=_paper_soak_validation_bars(),
    )
    startup_fault = runtime.restore_startup()
    latest_reconciliation = None
    for _ in range(len(_paper_soak_validation_bars())):
        new_bars, reconciliation, _ = runtime.poll_and_process()
        if reconciliation:
            latest_reconciliation = reconciliation
        summary = _live_timing_validation_summary(runtime=runtime, latest_reconciliation=latest_reconciliation)
        if summary.get("latest_order_intent", {}).get("intent_type") == OrderIntentType.BUY_TO_OPEN.value:
            break
        if new_bars <= 0:
            break
    summary = _live_timing_validation_summary(runtime=runtime, latest_reconciliation=latest_reconciliation)
    scenario_rows.append(
        _paper_soak_validation_scenario(
            scenario_id="submit_after_completed_bar_close",
            title="Submit immediately after completed bar close",
            status="PASS"
            if startup_fault is None
            and summary.get("submit_attempted_at") == summary.get("intent_created_at")
            and summary.get("pending_stage") == LIVE_TIMING_STAGE_AWAITING_FILL
            and strategy_engine.state.position_side is PositionSide.FLAT
            else "FAIL",
            detail="Completed-bar evaluation creates and submits the intent in the same finalized-bar cycle, while position state remains flat until fill.",
            summary=summary,
            evidence={
                "startup_fault": startup_fault,
                "latest_intent": summary.get("latest_order_intent"),
                "latest_fill": summary.get("latest_fill"),
            },
        )
    )

    runtime, strategy_engine, execution_engine, repositories, lane_logger, broker = _fresh_runtime(
        "ack_prompt_fill_delayed",
        bars=[],
    )
    runtime.restore_startup()
    submit_bar = _live_timing_validation_submit_bar(datetime(2026, 3, 27, 9, 35, tzinfo=ny))
    strategy_engine.submit_runtime_entry_intent(
        submit_bar,
        side="LONG",
        signal_source="liveTimingValidation",
        reason_code="liveTimingValidationEntry",
        long_entry_family=LongEntryFamily.K,
    )
    summary = _live_timing_validation_summary(runtime=runtime)
    scenario_rows.append(
        _paper_soak_validation_scenario(
            scenario_id="ack_prompt_fill_delayed",
            title="Ack arrives promptly, fill delayed",
            status="PASS"
            if summary.get("pending_stage") == LIVE_TIMING_STAGE_AWAITING_FILL
            and summary.get("broker_ack_at") is not None
            and strategy_engine.state.position_side is PositionSide.FLAT
            else "FAIL",
            detail="Prompt broker acknowledgement keeps the runtime pending without changing position state before fill.",
            summary=summary,
            evidence={"broker_truth": summary.get("broker_truth"), "latest_intent": summary.get("latest_order_intent")},
        )
    )

    runtime, strategy_engine, execution_engine, repositories, lane_logger, broker = _fresh_runtime(
        "ack_missing_broker_truth_exists",
        bars=[],
    )
    runtime.restore_startup()
    submit_bar = _live_timing_validation_submit_bar(datetime(2026, 3, 27, 9, 40, tzinfo=ny))
    intent = strategy_engine.submit_runtime_entry_intent(
        submit_bar,
        side="LONG",
        signal_source="liveTimingValidation",
        reason_code="liveTimingValidationAckMissing",
        long_entry_family=LongEntryFamily.K,
    )
    assert intent is not None
    pending = execution_engine.pending_execution(intent.order_intent_id)
    assert pending is not None
    broker.set_order_status(pending.broker_order_id, "SUBMITTED")
    broker.restore_live_truth(
        open_order_ids=[pending.broker_order_id],
        status_by_order_id={pending.broker_order_id: "SUBMITTED"},
        position_quantity=0,
    )
    watchdog_status, _, _ = _run_order_timeout_watchdog(
        settings=runtime.settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=runtime.structured_logger,
        alert_dispatcher=runtime.alert_dispatcher,
        watchdog_status=runtime._order_timeout_watchdog,  # noqa: SLF001
        occurred_at=submit_bar.end_ts + timedelta(seconds=runtime.settings.order_ack_timeout_seconds + 1),
    )
    runtime._order_timeout_watchdog = watchdog_status  # noqa: SLF001
    summary = _live_timing_validation_summary(runtime=runtime)
    scenario_rows.append(
        _paper_soak_validation_scenario(
            scenario_id="ack_missing_broker_truth_exists",
            title="Ack missing but broker open-order truth exists",
            status="PASS"
            if summary.get("pending_stage") == LIVE_TIMING_STAGE_AWAITING_FILL
            and summary.get("broker_ack_at") is not None
            and strategy_engine.state.position_side is PositionSide.FLAT
            else "FAIL",
            detail="Missing ack is repaired from broker open-order truth without prematurely changing position state.",
            summary=summary,
            evidence={"watchdog": watchdog_status, "broker_truth": summary.get("broker_truth")},
        )
    )

    runtime, strategy_engine, execution_engine, repositories, lane_logger, broker = _fresh_runtime(
        "fill_missing_position_truth_exists",
        bars=[],
    )
    runtime.restore_startup()
    submit_bar = _live_timing_validation_submit_bar(datetime(2026, 3, 27, 9, 45, tzinfo=ny))
    intent = strategy_engine.submit_runtime_entry_intent(
        submit_bar,
        side="LONG",
        signal_source="liveTimingValidation",
        reason_code="liveTimingValidationFillMissing",
        long_entry_family=LongEntryFamily.K,
    )
    assert intent is not None
    pending = execution_engine.pending_execution(intent.order_intent_id)
    assert pending is not None
    broker.restore_live_truth(
        open_order_ids=[],
        status_by_order_id={pending.broker_order_id: "FILLED"},
        position_quantity=1,
        average_price=Decimal("100"),
        last_fill_timestamp=submit_bar.end_ts + timedelta(seconds=20),
    )
    watchdog_status, reconciliation, _ = _run_order_timeout_watchdog(
        settings=runtime.settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=runtime.structured_logger,
        alert_dispatcher=runtime.alert_dispatcher,
        watchdog_status=runtime._order_timeout_watchdog,  # noqa: SLF001
        occurred_at=submit_bar.end_ts + timedelta(seconds=runtime.settings.order_fill_timeout_seconds + runtime.settings.order_timeout_reconcile_grace_seconds + 1),
    )
    runtime._order_timeout_watchdog = watchdog_status  # noqa: SLF001
    runtime._last_reconciliation_payload = dict(reconciliation or {})  # noqa: SLF001
    summary = _live_timing_validation_summary(runtime=runtime, latest_reconciliation=reconciliation)
    scenario_rows.append(
        _paper_soak_validation_scenario(
            scenario_id="fill_missing_position_truth_exists",
            title="Fill missing but broker position truth exists",
            status="PASS"
            if summary.get("pending_stage") == LIVE_TIMING_STAGE_RECONCILING
            and str((reconciliation or {}).get("resulting_state") or "") == StrategyStatus.RECONCILING.value
            and strategy_engine.state.position_side is PositionSide.FLAT
            else "FAIL",
            detail="Broker position/fill timestamp without internal fill truth escalates to reconciliation instead of mutating state optimistically.",
            summary=summary,
            evidence={"watchdog": watchdog_status, "reconciliation": reconciliation, "broker_truth": summary.get("broker_truth")},
        )
    )

    runtime, strategy_engine, execution_engine, repositories, lane_logger, broker = _fresh_runtime(
        "rejected_after_submit",
        bars=[],
    )
    runtime.restore_startup()
    submit_bar = _live_timing_validation_submit_bar(datetime(2026, 3, 27, 9, 50, tzinfo=ny))
    intent = strategy_engine.submit_runtime_entry_intent(
        submit_bar,
        side="LONG",
        signal_source="liveTimingValidation",
        reason_code="liveTimingValidationRejected",
        long_entry_family=LongEntryFamily.K,
    )
    assert intent is not None
    pending = execution_engine.pending_execution(intent.order_intent_id)
    assert pending is not None
    broker.restore_live_truth(
        open_order_ids=[],
        status_by_order_id={pending.broker_order_id: "REJECTED"},
        position_quantity=0,
    )
    watchdog_status, _, _ = _run_order_timeout_watchdog(
        settings=runtime.settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=runtime.structured_logger,
        alert_dispatcher=runtime.alert_dispatcher,
        watchdog_status=runtime._order_timeout_watchdog,  # noqa: SLF001
        occurred_at=submit_bar.end_ts + timedelta(seconds=5),
    )
    runtime._order_timeout_watchdog = watchdog_status  # noqa: SLF001
    summary = _live_timing_validation_summary(runtime=runtime)
    scenario_rows.append(
        _paper_soak_validation_scenario(
            scenario_id="rejected_after_submit",
            title="Rejected order after submit",
            status="PASS"
            if summary.get("pending_stage") in {LIVE_TIMING_STAGE_IDLE, LIVE_TIMING_STAGE_TERMINAL_NON_FILL}
            and not execution_engine.pending_executions()
            and strategy_engine.state.position_side is PositionSide.FLAT
            else "FAIL",
            detail="Explicit broker rejection resolves the pending execution as terminal non-fill without creating false exposure.",
            summary=summary,
            evidence={"watchdog": watchdog_status, "latest_intent": summary.get("latest_order_intent")},
        )
    )

    runtime, strategy_engine, execution_engine, repositories, lane_logger, broker = _fresh_runtime(
        "broker_unavailable_at_submit_time",
        bars=[],
    )
    runtime.restore_startup()
    broker.disconnect()
    broker.set_submit_failure("broker submit unavailable")
    submit_bar = _live_timing_validation_submit_bar(datetime(2026, 3, 27, 9, 55, tzinfo=ny))
    result = strategy_engine.submit_runtime_entry_intent(
        submit_bar,
        side="LONG",
        signal_source="liveTimingValidation",
        reason_code="liveTimingValidationBrokerUnavailable",
        long_entry_family=LongEntryFamily.K,
    )
    summary = _live_timing_validation_summary(runtime=runtime)
    scenario_rows.append(
        _paper_soak_validation_scenario(
            scenario_id="broker_unavailable_at_submit_time",
            title="Broker unavailable at intended submit time",
            status="PASS"
            if result is None
            and summary.get("pending_stage") == LIVE_TIMING_STAGE_RECONCILING
            and strategy_engine.state.position_side is PositionSide.FLAT
            else "FAIL",
            detail="Broker submit failure fail-closes into reconciliation instead of creating a phantom pending execution or position transition.",
            summary=summary,
            evidence={"submit_failure": summary.get("submit_failure"), "broker_truth": summary.get("broker_truth")},
        )
    )

    runtime, strategy_engine, execution_engine, repositories, lane_logger, broker = _fresh_runtime(
        "broker_reconnect_after_pending_order",
        bars=[],
    )
    runtime.restore_startup()
    submit_bar = _live_timing_validation_submit_bar(datetime(2026, 3, 27, 10, 0, tzinfo=ny))
    intent = strategy_engine.submit_runtime_entry_intent(
        submit_bar,
        side="LONG",
        signal_source="liveTimingValidation",
        reason_code="liveTimingValidationReconnect",
        long_entry_family=LongEntryFamily.K,
    )
    assert intent is not None
    pending = execution_engine.pending_execution(intent.order_intent_id)
    assert pending is not None
    broker.disconnect()
    watchdog_status, _, _ = _run_order_timeout_watchdog(
        settings=runtime.settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=runtime.structured_logger,
        alert_dispatcher=runtime.alert_dispatcher,
        watchdog_status=runtime._order_timeout_watchdog,  # noqa: SLF001
        occurred_at=submit_bar.end_ts + timedelta(seconds=runtime.settings.order_fill_timeout_seconds + 1),
    )
    runtime._order_timeout_watchdog = watchdog_status  # noqa: SLF001
    broker.connect()
    broker.restore_live_truth(
        open_order_ids=[pending.broker_order_id],
        status_by_order_id={pending.broker_order_id: "ACKNOWLEDGED"},
        position_quantity=0,
    )
    watchdog_status, _, _ = _run_order_timeout_watchdog(
        settings=runtime.settings,
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
        structured_logger=runtime.structured_logger,
        alert_dispatcher=runtime.alert_dispatcher,
        watchdog_status=runtime._order_timeout_watchdog,  # noqa: SLF001
        occurred_at=submit_bar.end_ts + timedelta(seconds=runtime.settings.order_fill_timeout_seconds + 10),
    )
    runtime._order_timeout_watchdog = watchdog_status  # noqa: SLF001
    summary = _live_timing_validation_summary(runtime=runtime)
    scenario_rows.append(
        _paper_soak_validation_scenario(
            scenario_id="broker_reconnect_after_pending_order",
            title="Broker reconnect after pending order",
            status="PASS"
            if summary.get("pending_stage") == LIVE_TIMING_STAGE_AWAITING_FILL
            and strategy_engine.state.position_side is PositionSide.FLAT
            else "FAIL",
            detail="Reconnect restores broker truth for the pending order without duplicating actions or mutating position before fill.",
            summary=summary,
            evidence={"watchdog": watchdog_status, "broker_truth": summary.get("broker_truth")},
        )
    )

    runtime, strategy_engine, execution_engine, repositories, lane_logger, broker = _fresh_runtime(
        "exit_timing_fill_driven",
        bars=[],
    )
    runtime.restore_startup()
    entry_fill = FillEvent(
        order_intent_id="timing-entry",
        intent_type=OrderIntentType.BUY_TO_OPEN,
        order_status=OrderStatus.FILLED,
        fill_timestamp=datetime(2026, 3, 27, 10, 5, tzinfo=ny),
        fill_price=Decimal("100"),
        broker_order_id="paper-timing-entry",
    )
    strategy_engine.apply_fill(fill_event=entry_fill, signal_bar_id="timing-entry-bar", long_entry_family=LongEntryFamily.K)
    strategy_engine._state = replace(strategy_engine.state, last_swing_low=Decimal("99.5"), long_be_armed=True, bars_in_trade=6)  # noqa: SLF001
    strategy_engine._persist_state(strategy_engine.state, transition_label="seed_live_timing_exit")  # noqa: SLF001
    setup_bar = _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 10, tzinfo=ny), "101.1", "101.5", "100.8", "101.3")
    strategy_engine._bar_history = [setup_bar]  # noqa: SLF001
    strategy_engine._feature_history = [strategy_engine._compute_feature_packet(setup_bar)]  # noqa: SLF001
    exit_bar = _paper_soak_validation_bar(datetime(2026, 3, 27, 10, 15, tzinfo=ny), "100.9", "101.1", "98.7", "99")
    strategy_engine.process_bar(exit_bar)
    pending_exit_summary = _live_timing_validation_summary(runtime=runtime)
    pending_exit = execution_engine.pending_executions()[0]
    exit_fill = FillEvent(
        order_intent_id=pending_exit.intent.order_intent_id,
        intent_type=pending_exit.intent.intent_type,
        order_status=OrderStatus.FILLED,
        fill_timestamp=datetime(2026, 3, 27, 10, 20, tzinfo=ny),
        fill_price=Decimal("99"),
        broker_order_id=pending_exit.broker_order_id,
    )
    strategy_engine.apply_fill(fill_event=exit_fill)
    final_exit_summary = _live_timing_validation_summary(runtime=runtime)
    scenario_rows.append(
        _paper_soak_validation_scenario(
            scenario_id="exit_timing_fill_driven",
            title="Exit timing follows the same fill-driven rules",
            status="PASS"
            if pending_exit_summary.get("pending_stage") == LIVE_TIMING_STAGE_AWAITING_FILL
            and pending_exit_summary.get("position_side") == PositionSide.LONG.value
            and final_exit_summary.get("pending_stage") == LIVE_TIMING_STAGE_FILLED
            and strategy_engine.state.position_side is PositionSide.FLAT
            else "FAIL",
            detail="Exit intent leaves the position open until the confirmed exit fill transitions the runtime back to READY/flat.",
            summary=final_exit_summary,
            evidence={
                "pending_exit_summary": pending_exit_summary,
                "final_exit_summary": final_exit_summary,
            },
        )
    )

    passed_count = sum(1 for row in scenario_rows if row.get("status") == "PASS")
    final_summary = dict(scenario_rows[-1].get("summary") or {})
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope_label": "MGC 5m live timing validation",
        "operator_path": "mgc-v05l probationary-live-timing-validate",
        "allowed_scope": {
            "symbol": "MGC",
            "timeframe": "5m",
            "mode": "PAPER_RUNTIME_WITH_LIVE_TIMING_BOUNDARY",
            "evaluation": "completed_bar_only",
            "processing": "deterministic_sequential",
        },
        "contract": _live_timing_contract(base_settings),
        "summary": {
            "result": "PASS" if passed_count == len(scenario_rows) else "FAIL",
            "run_id": run_id,
            "scenario_count": len(scenario_rows),
            "passed_count": passed_count,
            "restart_safe": True,
            "final_runtime_phase": final_summary.get("runtime_phase"),
            "final_strategy_state": final_summary.get("strategy_state"),
            "final_pending_stage": final_summary.get("pending_stage"),
            "final_blocker": final_summary.get("entries_disabled_blocker"),
        },
        "scenarios": scenario_rows,
        "representative_summary": final_summary,
    }
    logger = StructuredLogger(validation_dir)
    json_path = logger.write_live_timing_validation_state(payload)
    logger.log_live_timing_event(payload)
    markdown_path = validation_dir / "paper_live_timing_validation_latest.md"
    markdown_lines = [
        "# Live Timing Validation",
        "",
        f"- Run ID: `{run_id}`",
        f"- Result: `{payload['summary']['result']}`",
        f"- Scenarios: `{passed_count}/{len(scenario_rows)}` passed",
        f"- Final runtime phase: `{payload['summary']['final_runtime_phase']}`",
        f"- Final pending stage: `{payload['summary']['final_pending_stage']}`",
        "",
        "## Timing Contract",
        f"- Earliest broker submit: `{payload['contract']['earliest_permissible_broker_submit']}`",
        f"- Ack window seconds: `{payload['contract']['acknowledgement_window_seconds']}`",
        f"- Fill window seconds: `{payload['contract']['fill_confirmation_window_seconds']}`",
        f"- Reconcile grace seconds: `{payload['contract']['reconcile_grace_seconds']}`",
        "",
        "## Scenarios",
    ]
    for row in scenario_rows:
        markdown_lines.append(f"- `{row['scenario_id']}` `{row['status']}` {row['detail']}")
    markdown_path.write_text("\n".join(markdown_lines) + "\n", encoding="utf-8")
    return ProbationaryLiveTimingValidationRun(
        artifact_path=str(json_path),
        markdown_path=str(markdown_path),
        summary=payload,
    )


def _render_probationary_paper_soak_validation_markdown(payload: dict[str, Any]) -> str:
    summary = dict(payload.get("summary") or {})
    lines = [
        "# Probationary Paper Soak Validation",
        "",
        f"- Generated: `{payload.get('generated_at')}`",
        f"- Scope: `{payload.get('scope_label')}`",
        f"- Result: `{summary.get('result')}`",
        f"- Passed: `{summary.get('passed_count')}` / `{summary.get('scenario_count')}`",
        f"- Runtime Phase: `{summary.get('runtime_phase')}`",
        f"- Strategy State: `{summary.get('strategy_state')}`",
        f"- Last Processed Bar: `{summary.get('last_processed_bar_id')}`",
        f"- Last Restore Result: `{summary.get('latest_restore_result')}`",
        f"- Entry Blocker: `{summary.get('entries_disabled_blocker')}`",
        "",
        "## Scenarios",
    ]
    for row in list(payload.get("scenarios") or []):
        lines.append(f"- `{row.get('scenario_id')}` `{row.get('status')}`: {row.get('detail')}")
    return "\n".join(lines) + "\n"


def run_probationary_paper_soak_validation(
    config_paths: Sequence[str | Path],
) -> ProbationaryPaperSoakValidation:
    base_settings = load_settings_from_files(config_paths)
    runtime_dir = base_settings.probationary_artifacts_path / "runtime"
    validation_dir = runtime_dir / "paper_soak_validation"
    validation_dir.mkdir(parents=True, exist_ok=True)
    scenarios = [
        _run_probationary_paper_soak_clean_cycle(base_settings, validation_dir),
        _run_probationary_paper_soak_restart_flat(base_settings, validation_dir),
        _run_probationary_paper_soak_restart_pending(base_settings, validation_dir),
        _run_probationary_paper_soak_restart_in_position(base_settings, validation_dir),
        _run_probationary_paper_soak_staged_participation(base_settings, validation_dir),
        _run_probationary_paper_soak_staged_partial_exit(base_settings, validation_dir),
        _run_probationary_paper_soak_restart_staged_in_position(base_settings, validation_dir),
        _run_probationary_paper_soak_duplicate_bar_suppression(base_settings, validation_dir),
        _run_probationary_paper_soak_out_of_order_rejection(base_settings, validation_dir),
        _run_probationary_paper_soak_stale_missing_bar_handling(base_settings, validation_dir),
        _run_probationary_paper_soak_timeout_reconciling(base_settings, validation_dir),
        _run_probationary_paper_soak_safe_repair(base_settings, validation_dir),
        _run_probationary_paper_soak_fault(base_settings, validation_dir),
    ]
    passed_count = sum(1 for row in scenarios if row.get("status") == "PASS")
    representative_summary = dict((scenarios[0] if scenarios else {}).get("summary") or {})
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "scope_label": "MGC 5m paper soak validation",
        "operator_path": "mgc-v05l probationary-paper-soak-validate",
        "allowed_scope": {
            "symbol": "MGC",
            "timeframe": "5m",
            "mode": "PAPER",
            "evaluation": "completed_bar_only",
            "processing": "deterministic_sequential",
        },
        "summary": {
            "result": "PASS" if passed_count == len(scenarios) else "FAIL",
            "scenario_count": len(scenarios),
            "passed_count": passed_count,
            "failed_count": len(scenarios) - passed_count,
            "runtime_phase": representative_summary.get("runtime_phase"),
            "strategy_state": representative_summary.get("strategy_state"),
            "last_processed_bar_id": representative_summary.get("last_processed_bar_id"),
            "last_processed_bar_end_ts": representative_summary.get("last_processed_bar_end_ts"),
            "position_state": representative_summary.get("position_state"),
            "latest_order_intent": representative_summary.get("latest_order_intent"),
            "latest_fill": representative_summary.get("latest_fill"),
            "latest_reconcile_event": representative_summary.get("latest_reconcile_event"),
            "latest_order_timeout_watchdog": representative_summary.get("latest_order_timeout_watchdog"),
            "market_data_health": representative_summary.get("market_data_health"),
            "latest_restore_result": representative_summary.get("latest_restore_result"),
            "entries_disabled_blocker": representative_summary.get("entries_disabled_blocker"),
        },
        "scenarios": scenarios,
    }
    logger = StructuredLogger(validation_dir)
    json_path = logger.write_paper_soak_validation_state(payload)
    logger.log_paper_soak_validation_event(payload)
    markdown_path = validation_dir / "paper_soak_validation_latest.md"
    markdown_path.write_text(_render_probationary_paper_soak_validation_markdown(payload), encoding="utf-8")
    return ProbationaryPaperSoakValidation(
        artifact_path=str(json_path),
        markdown_path=str(markdown_path),
        summary=payload,
    )


def submit_probationary_operator_control(
    config_paths: Sequence[str | Path],
    action: str,
    *,
    payload: dict[str, Any] | None = None,
    shared_strategy_identity: str | None = None,
) -> ProbationaryOperatorControlResult:
    supported = {
        "halt_entries",
        "resume_entries",
        "clear_fault",
        "clear_risk_halts",
        "flatten_and_halt",
        "stop_after_cycle",
        "force_reconcile",
        REALIZED_LOSER_SESSION_OVERRIDE_ACTION,
    }
    if action not in supported:
        raise ValueError(f"Unsupported operator control action: {action}")
    settings = load_settings_from_files(config_paths)
    logger = StructuredLogger(settings.probationary_artifacts_path)
    merged_payload = dict(payload or {})
    requested_lane_id = str(merged_payload.get("lane_id") or "").strip() or None
    requested_shared_identity = (
        str(merged_payload.get("shared_strategy_identity") or "").strip()
        or str(shared_strategy_identity or "").strip()
        or None
    )
    target_payload = _resolve_probationary_operator_control_target(
        settings,
        action=action,
        lane_id=requested_lane_id,
        shared_strategy_identity=requested_shared_identity,
    )
    control_path = (
        _shared_probationary_operator_control_path(settings)
        if target_payload
        else settings.resolved_probationary_operator_control_path
    )
    control_path.parent.mkdir(parents=True, exist_ok=True)
    control_payload = {
        "action": action,
        "status": "pending",
        "requested_at": datetime.now(timezone.utc).isoformat(),
        "command_id": f"{action}-{int(datetime.now(timezone.utc).timestamp() * 1000)}",
        "halt_reason": "operator_flatten_and_halt" if action == "flatten_and_halt" else ("operator_halt_entries" if action == "halt_entries" else None),
        "flatten_state": "pending_confirmation" if action == "flatten_and_halt" else None,
        "stop_after_cycle_requested": action == "stop_after_cycle",
    }
    if merged_payload:
        control_payload.update(merged_payload)
    if target_payload:
        control_payload.update(target_payload)
        control_payload["control_scope"] = "lane"
    control_path.write_text(json.dumps(control_payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    logger.log_operator_control({**control_payload, "control_path": str(control_path)})
    return ProbationaryOperatorControlResult(
        action=action,
        control_path=str(control_path),
        status="pending",
        requested_at=str(control_payload["requested_at"]),
    )


def _shared_probationary_operator_control_path(settings: StrategySettings) -> Path:
    return settings.probationary_artifacts_path / "runtime" / "operator_control.json"


def _resolve_probationary_operator_control_target(
    settings: StrategySettings,
    *,
    action: str,
    lane_id: str | None = None,
    shared_strategy_identity: str | None = None,
) -> dict[str, str]:
    requested_lane_id = str(lane_id or "").strip()
    requested_shared_identity = str(shared_strategy_identity or "").strip()

    if not requested_lane_id and not requested_shared_identity and (
        settings.probationary_paper_runtime_exclusive_config
        and action in _LANE_TARGETABLE_PROBATIONARY_CONTROL_ACTIONS
    ):
        active_specs = _active_probationary_paper_lane_specs(settings)
        if len(active_specs) == 1:
            requested_lane_id = active_specs[0].lane_id
            requested_shared_identity = str(active_specs[0].shared_strategy_identity or "").strip()

    if not requested_lane_id and not requested_shared_identity:
        return {}

    active_specs = _active_probationary_paper_lane_specs(settings)
    specs_by_lane_id = {spec.lane_id: spec for spec in active_specs}
    specs_by_shared_identity = {
        str(spec.shared_strategy_identity): spec
        for spec in active_specs
        if str(spec.shared_strategy_identity or "").strip()
    }

    target_spec: ProbationaryPaperLaneSpec | None = None
    if requested_shared_identity:
        target_spec = specs_by_shared_identity.get(requested_shared_identity)
        if target_spec is None:
            canonical_identity = get_shared_strategy_identity(requested_shared_identity)
            target_spec = specs_by_lane_id.get(canonical_identity.lane_id)
    if requested_lane_id:
        lane_match = specs_by_lane_id.get(requested_lane_id)
        if lane_match is None:
            raise ValueError(
                f"Probationary operator control rejected because lane {requested_lane_id} is not active in the current paper runtime."
            )
        if target_spec is not None and lane_match.lane_id != target_spec.lane_id:
            raise ValueError(
                "Probationary operator control rejected because lane_id and shared_strategy_identity target different lanes."
            )
        target_spec = lane_match

    if target_spec is None:
        raise ValueError(
            f"Probationary operator control rejected because shared strategy identity {requested_shared_identity} is not active in the current paper runtime."
        )

    payload = {"lane_id": target_spec.lane_id}
    if str(target_spec.shared_strategy_identity or "").strip():
        payload["shared_strategy_identity"] = str(target_spec.shared_strategy_identity)
    elif requested_shared_identity:
        payload["shared_strategy_identity"] = requested_shared_identity
    return payload


def _build_live_polling_service(
    settings: StrategySettings,
    repositories: RepositorySet,
    schwab_config_path: str | Path | None,
) -> LivePollingService:
    schwab_config = load_schwab_market_data_config(schwab_config_path)
    adapter = SchwabMarketDataAdapter(settings, schwab_config)
    oauth_client = SchwabOAuthClient(
        config=schwab_config.auth,
        transport=UrllibJsonTransport(),
        token_store=SchwabTokenStore(schwab_config.auth.token_store_path),
    )
    historical_client = SchwabHistoricalHttpClient(
        oauth_client=oauth_client,
        market_data_config=schwab_config,
        transport=UrllibJsonTransport(),
    )
    return LivePollingService(
        adapter=adapter,
        client=HistoricalPollingLiveClient(
            adapter=adapter,
            historical_client=historical_client,
            lookback_minutes=settings.live_poll_lookback_minutes,
        ),
        repositories=repositories,
    )


def _probationary_runtime_transport_probe_artifact_path(settings: StrategySettings) -> Path:
    return settings.probationary_artifacts_path / "runtime" / "market_data_transport_probe.json"


def _probationary_runtime_transport_failure_artifact_path(settings: StrategySettings) -> Path:
    return settings.probationary_artifacts_path / "runtime" / "market_data_transport_failure.json"


def _probationary_runtime_transport_env() -> dict[str, str]:
    env_names = (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "NO_PROXY",
        "ALL_PROXY",
        "HOSTALIASES",
        "RES_OPTIONS",
        "SSL_CERT_FILE",
        "REQUESTS_CA_BUNDLE",
        "CURL_CA_BUNDLE",
    )
    return {name: os.environ.get(name, "<unset>") for name in env_names}


def _probationary_runtime_http_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value)


def _clear_probationary_runtime_transport_failure(settings: StrategySettings) -> None:
    failure_path = _probationary_runtime_transport_failure_artifact_path(settings)
    if failure_path.exists():
        failure_path.unlink()


def _write_probationary_runtime_transport_probe(settings: StrategySettings, payload: dict[str, Any]) -> Path:
    path = _probationary_runtime_transport_probe_artifact_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _write_probationary_runtime_transport_failure(settings: StrategySettings, payload: dict[str, Any]) -> Path:
    path = _probationary_runtime_transport_failure_artifact_path(settings)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    return path


def _probationary_runtime_transport_diagnostic_payload(
    settings: StrategySettings,
    schwab_config,
    adapter: SchwabMarketDataAdapter,
) -> dict[str, Any]:
    base_url = str(schwab_config.market_data_base_url or "").strip()
    if not base_url:
        raise RuntimeError("Schwab market-data base URL is empty.")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise RuntimeError(f"Schwab market-data base URL is invalid: {base_url!r}")
    port = parsed.port or (443 if parsed.scheme == "https" else 80)
    external_symbol = adapter.map_historical_symbol(settings.symbol)
    frequency = adapter.map_timeframe(settings.timeframe)
    now = datetime.now(settings.timezone_info)
    end_date_ms = int(now.timestamp() * 1000)
    start_dt = now - timedelta(minutes=max(settings.live_poll_lookback_minutes, timeframe_minutes(settings.timeframe)))
    start_date_ms = int(start_dt.timestamp() * 1000)
    query = {
        "symbol": external_symbol,
        "periodType": "day",
        "needExtendedHoursData": True,
        "needPreviousClose": False,
        "frequencyType": frequency.frequency_type,
        "frequency": frequency.frequency,
        "startDate": start_date_ms,
        "endDate": end_date_ms,
    }
    request_url = f"{base_url.rstrip('/')}/pricehistory?{urlencode({key: _probationary_runtime_http_value(value) for key, value in query.items()})}"
    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "blocker_label": "market_data_transport_failure",
        "cwd": os.getcwd(),
        "target_host": parsed.hostname,
        "market_data_base_url": base_url,
        "rendered_url": request_url,
        "probe_symbol_internal": settings.symbol,
        "probe_symbol_external": external_symbol,
        "probe_timeframe": settings.timeframe,
        "request_query": query,
        "proxy_env": _probationary_runtime_transport_env(),
        "python_executable": sys.executable,
        "venv_prefix": sys.prefix,
        "hostname": parsed.hostname,
        "market_data_base_url": base_url,
        "port": port,
        "pid": os.getpid(),
    }


def run_probationary_market_data_transport_probe(
    config_paths: Sequence[str | Path],
    schwab_config_path: str | Path | None,
) -> dict[str, Any]:
    settings = load_settings_from_files(config_paths)
    return _run_probationary_runtime_market_data_transport_probe(
        settings=settings,
        schwab_config_path=schwab_config_path,
    )


def _run_probationary_runtime_market_data_transport_probe(
    *,
    settings: StrategySettings,
    schwab_config_path: str | Path | None,
    schwab_config=None,
    adapter: SchwabMarketDataAdapter | None = None,
    oauth_client: SchwabOAuthClient | None = None,
) -> dict[str, Any]:
    resolved_schwab_config = schwab_config or load_schwab_market_data_config(schwab_config_path)
    resolved_adapter = adapter or SchwabMarketDataAdapter(settings, resolved_schwab_config)
    diagnostic = _probationary_runtime_transport_diagnostic_payload(settings, resolved_schwab_config, resolved_adapter)
    print(f"Probationary paper runtime network preflight: {json.dumps(diagnostic, sort_keys=True)}", flush=True)
    failure_base = {
        **diagnostic,
        "runtime_ready": False,
        "status": "failed",
        "next_fix": (
            "Host cannot reach Schwab market data from the paper runtime context. "
            "Verify Mac DNS/proxy/certificate settings, then rerun the shared market-data transport probe."
        ),
    }
    try:
        resolved = socket.getaddrinfo(str(diagnostic["target_host"]), int(diagnostic["port"]), type=socket.SOCK_STREAM)
    except socket.gaierror as exc:
        failure_payload = {
            **failure_base,
            "failure_kind": "dns_resolution_failed",
            "dns_resolution_succeeds": False,
            "authenticated_probe_attempted": False,
            "authenticated_probe_succeeds": False,
            "exception_text": str(exc),
            "message": f"DNS resolution failed for {diagnostic['target_host']}.",
        }
        failure_path = _write_probationary_runtime_transport_failure(settings, failure_payload)
        failure_payload["artifact_path"] = str(failure_path)
        raise ProbationaryRuntimeTransportFailure(failure_payload) from exc
    resolved_addresses = sorted({entry[4][0] for entry in resolved if entry[4]})
    resolved_oauth_client = oauth_client or SchwabOAuthClient(
        config=resolved_schwab_config.auth,
        transport=UrllibJsonTransport(),
        token_store=SchwabTokenStore(resolved_schwab_config.auth.token_store_path),
    )
    historical_client = SchwabHistoricalHttpClient(
        oauth_client=resolved_oauth_client,
        market_data_config=resolved_schwab_config,
        transport=UrllibJsonTransport(),
    )
    try:
        historical_client.fetch_price_history(
            str(diagnostic["probe_symbol_external"]),
            SchwabHistoricalRequest(
                internal_symbol=settings.symbol,
                period_type="day",
                frequency_type=str(diagnostic["request_query"]["frequencyType"]),
                frequency=int(diagnostic["request_query"]["frequency"]),
                start_date_ms=int(diagnostic["request_query"]["startDate"]),
                end_date_ms=int(diagnostic["request_query"]["endDate"]),
                need_extended_hours_data=True,
                need_previous_close=False,
            ),
            default_frequency=resolved_adapter.map_timeframe(settings.timeframe),
        )
    except Exception as exc:
        failure_payload = {
            **failure_base,
            "failure_kind": "authenticated_pricehistory_probe_failed",
            "dns_resolution_succeeds": True,
            "resolved_addresses": resolved_addresses,
            "authenticated_probe_attempted": True,
            "authenticated_probe_succeeds": False,
            "exception_text": str(exc),
            "message": f"Authenticated Schwab /pricehistory probe failed for {diagnostic['target_host']}.",
        }
        failure_path = _write_probationary_runtime_transport_failure(settings, failure_payload)
        failure_payload["artifact_path"] = str(failure_path)
        raise ProbationaryRuntimeTransportFailure(failure_payload) from exc

    success_payload = {
        **diagnostic,
        "status": "ok",
        "runtime_ready": True,
        "dns_resolution_succeeds": True,
        "resolved_addresses": resolved_addresses,
        "authenticated_probe_attempted": True,
        "authenticated_probe_succeeds": True,
    }
    _clear_probationary_runtime_transport_failure(settings)
    artifact_path = _write_probationary_runtime_transport_probe(settings, success_payload)
    success_payload["artifact_path"] = str(artifact_path)
    return success_payload


def _read_json(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []
    records: list[dict[str, Any]] = []
    for line in path.read_text(encoding="utf-8").splitlines():
        stripped = line.strip()
        if not stripped:
            continue
        records.append(json.loads(stripped))
    return records


def _write_jsonl(path: Path, rows: Iterable[dict[str, Any]]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        for row in rows:
            handle.write(json.dumps(row, sort_keys=True))
            handle.write("\n")
    return path


def _atpe_canary_root_dir() -> Path:
    return Path("outputs/probationary_quant_canaries/active_trend_participation_engine").resolve()


def _default_atpe_runtime_snapshot_rows(root: Path, *, instruments: Sequence[str]) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for lane in _CANARY_LANES:
        for instrument in instruments:
            normalized_instrument = str(instrument).strip().upper()
            lane_id = atpe_runtime_lane_id(lane, normalized_instrument)
            lane_name = atpe_runtime_lane_name(lane, normalized_instrument)
            rows.append(
                {
                    "lane_id": lane_id,
                    "lane_name": lane_name,
                    "display_name": lane_name,
                    "instrument": normalized_instrument,
                    "variant_id": lane.variant_id,
                    "side": lane.side,
                    "symbols": [normalized_instrument],
                    "experimental_status": lane.experimental_status,
                    "canary_stage": lane.canary_stage,
                    "quality_bucket_policy": lane.quality_bucket_policy,
                    "priority_tier": lane.priority_tier,
                    "paper_only": True,
                    "artifacts": {
                        "lane_dir": str((root / "lanes" / lane_id).resolve()),
                        "processed_bars": str((root / "lanes" / lane_id / "processed_bars.jsonl").resolve()),
                        "features": str((root / "lanes" / lane_id / "features.jsonl").resolve()),
                        "signals": str((root / "lanes" / lane_id / "signals.jsonl").resolve()),
                        "trades": str((root / "lanes" / lane_id / "trades.jsonl").resolve()),
                        "events": str((root / "lanes" / lane_id / "events.jsonl").resolve()),
                        "operator_status": str((root / "lanes" / lane_id / "operator_status.json").resolve()),
                    },
                }
            )
    return rows


def _research_bar_from_domain_bar(bar: Bar) -> ResearchBar:
    phase = label_session_phase(bar.end_ts)
    return ResearchBar(
        instrument=str(bar.symbol).upper(),
        timeframe="1m",
        start_ts=bar.start_ts,
        end_ts=bar.end_ts,
        open=float(bar.open),
        high=float(bar.high),
        low=float(bar.low),
        close=float(bar.close),
        volume=int(bar.volume),
        session_label=phase,
        session_segment=_phase_coarse_session_group(phase),
        source="schwab_live_poll",
        provenance="probationary_paper_runtime",
    )


def _research_bar_to_domain_bar(bar: ResearchBar) -> Bar:
    session_group = _phase_coarse_session_group(bar.session_label)
    return Bar(
        bar_id=f"{bar.instrument}|{bar.timeframe}|{bar.end_ts.isoformat()}",
        symbol=bar.instrument,
        timeframe=bar.timeframe,
        start_ts=bar.start_ts,
        end_ts=bar.end_ts,
        open=Decimal(str(bar.open)),
        high=Decimal(str(bar.high)),
        low=Decimal(str(bar.low)),
        close=Decimal(str(bar.close)),
        volume=int(bar.volume),
        is_final=True,
        session_asia=session_group == "ASIA",
        session_london=session_group == "LONDON",
        session_us=session_group == "US",
        session_allowed=True,
    )


def _prune_research_bars(bars: Sequence[ResearchBar], *, keep_minutes: int) -> list[ResearchBar]:
    if not bars:
        return []
    cutoff = max(item.end_ts for item in bars) - timedelta(minutes=max(keep_minutes, 60))
    return [bar for bar in bars if bar.end_ts >= cutoff]


def _resample_research_bars_5m(bars_1m: Sequence[ResearchBar]) -> list[ResearchBar]:
    buckets: dict[datetime, list[ResearchBar]] = {}
    for bar in sorted(bars_1m, key=lambda item: item.end_ts):
        bucket_end = _latest_completed_probationary_bar_end(bar.end_ts, "5m")
        buckets.setdefault(bucket_end, []).append(bar)
    rows: list[ResearchBar] = []
    for bucket_end, bucket in sorted(buckets.items(), key=lambda item: item[0]):
        first = bucket[0]
        last = bucket[-1]
        phase = label_session_phase(bucket_end)
        rows.append(
            ResearchBar(
                instrument=first.instrument,
                timeframe="5m",
                start_ts=bucket_end - timedelta(minutes=5),
                end_ts=bucket_end,
                open=first.open,
                high=max(item.high for item in bucket),
                low=min(item.low for item in bucket),
                close=last.close,
                volume=sum(item.volume for item in bucket),
                session_label=phase,
                session_segment=_phase_coarse_session_group(phase),
                source="resampled_from_live_1m",
                provenance="probationary_paper_runtime",
            )
        )
    return rows


def _atpe_entry_trigger_price(*, decision: Any, variant: PatternVariant) -> float:
    reclaim_band = float(decision.average_range) * float(variant.trigger_reclaim_band_multiple)
    if str(decision.side).upper() == "LONG":
        if variant.family == "pullback_continuation":
            return max(float(decision.decision_bar_open), float(decision.decision_bar_close)) - float(decision.average_range) * 0.05 - reclaim_band * 0.5
        if variant.family == "breakout_continuation":
            return float(decision.decision_bar_high) - reclaim_band
        if variant.family == "pause_resume":
            return float(decision.decision_bar_high) - float(decision.average_range) * 0.15 - reclaim_band
        return max(float(decision.decision_bar_open), float(decision.decision_bar_close)) - float(decision.average_range) * 0.05 - reclaim_band * 0.5
    if variant.family == "pullback_continuation":
        return min(float(decision.decision_bar_open), float(decision.decision_bar_close)) + float(decision.average_range) * 0.05 + reclaim_band * 0.5
    if variant.family == "breakout_continuation":
        return float(decision.decision_bar_low) + reclaim_band
    if variant.family == "pause_resume":
        return float(decision.decision_bar_low) + float(decision.average_range) * 0.15 + reclaim_band
    return min(float(decision.decision_bar_open), float(decision.decision_bar_close)) + float(decision.average_range) * 0.05 + reclaim_band * 0.5


def _latest_atpe_feature_state_from_bars(
    *,
    bars_1m: Sequence[ResearchBar],
    instrument: str,
) -> Any | None:
    sorted_bars = sorted(bars_1m, key=lambda item: item.end_ts)
    if not sorted_bars:
        return None
    bars_5m = _resample_research_bars_5m(sorted_bars)
    if bars_5m:
        first_1m_ts = sorted_bars[0].end_ts
        last_1m_ts = sorted_bars[-1].end_ts
        bars_5m = [bar for bar in bars_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
    feature_rows = build_feature_states(bars_5m=bars_5m, bars_1m=sorted_bars)
    matching = [row for row in feature_rows if row.instrument == instrument]
    return matching[-1] if matching else None


def _latest_atpe_phase2_entry_state_from_bars(
    *,
    bars_1m: Sequence[ResearchBar],
    instrument: str,
    runtime_ready: bool,
    position_flat: bool,
    one_position_rule_clear: bool,
) -> dict[str, Any]:
    sorted_bars = sorted(bars_1m, key=lambda item: item.end_ts)
    if not sorted_bars:
        return latest_atp_entry_state_summary(None)
    bars_5m = _resample_research_bars_5m(sorted_bars)
    if bars_5m:
        first_1m_ts = sorted_bars[0].end_ts
        last_1m_ts = sorted_bars[-1].end_ts
        bars_5m = [bar for bar in bars_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
    feature_rows = build_feature_states(bars_5m=bars_5m, bars_1m=sorted_bars)
    matching = [row for row in feature_rows if row.instrument == instrument]
    if not matching:
        return latest_atp_entry_state_summary(None)
    entry_states = classify_entry_states(
        feature_rows=matching,
        runtime_ready=runtime_ready,
        position_flat=position_flat,
        one_position_rule_clear=one_position_rule_clear,
    )
    return latest_atp_entry_state_summary(entry_states[-1] if entry_states else None)


def _latest_atpe_phase3_timing_state_from_bars(
    *,
    bars_1m: Sequence[ResearchBar],
    instrument: str,
    runtime_ready: bool,
    position_flat: bool,
    one_position_rule_clear: bool,
) -> dict[str, Any]:
    sorted_bars = sorted(bars_1m, key=lambda item: item.end_ts)
    if not sorted_bars:
        return latest_atp_timing_state_summary(None)
    bars_5m = _resample_research_bars_5m(sorted_bars)
    if bars_5m:
        first_1m_ts = sorted_bars[0].end_ts
        last_1m_ts = sorted_bars[-1].end_ts
        bars_5m = [bar for bar in bars_5m if first_1m_ts <= bar.end_ts <= last_1m_ts]
    feature_rows = build_feature_states(bars_5m=bars_5m, bars_1m=sorted_bars)
    matching = [row for row in feature_rows if row.instrument == instrument]
    if not matching:
        return latest_atp_timing_state_summary(None)
    entry_states = classify_entry_states(
        feature_rows=matching,
        runtime_ready=runtime_ready,
        position_flat=position_flat,
        one_position_rule_clear=one_position_rule_clear,
    )
    timing_states = classify_timing_states(entry_states=entry_states, bars_1m=sorted_bars)
    return latest_atp_timing_state_summary(timing_states[-1] if timing_states else None)


def _atp_paper_runtime_lifecycle_contract(
    *,
    latest_atp_entry_state: dict[str, Any],
    latest_atp_timing_state: dict[str, Any],
    order_intents: Sequence[dict[str, Any]],
    fills: Sequence[dict[str, Any]],
    trade_rows: Sequence[dict[str, Any]],
    artifact_context: str,
) -> dict[str, Any]:
    entry_truth_available = bool(
        latest_atp_entry_state
        or latest_atp_timing_state
        or any(str(row.get("intent_type") or "").upper() == "BUY_TO_OPEN" for row in order_intents)
        or any(str(row.get("intent_type") or "").upper() == "BUY_TO_OPEN" for row in fills)
        or any(row.get("entry_timestamp") for row in trade_rows)
    )
    exit_truth_available = bool(
        any(str(row.get("intent_type") or "").upper() == "SELL_TO_CLOSE" for row in order_intents)
        or any(str(row.get("intent_type") or "").upper() == "SELL_TO_CLOSE" for row in fills)
        or any(row.get("exit_timestamp") or row.get("exit_reason") for row in trade_rows)
    )
    lifecycle_records_available = bool(order_intents or fills or trade_rows)
    lifecycle_truth_class = (
        FULL_AUTHORITATIVE_LIFECYCLE
        if entry_truth_available and exit_truth_available and lifecycle_records_available
        else AUTHORITATIVE_INTRABAR_ENTRY_ONLY
    )
    truth_provenance = {
        "runtime_context": "PAPER",
        "run_lane": "PAPER_RUNTIME",
        "artifact_context": artifact_context,
        "persistence_origin": "PERSISTED_RUNTIME_TRUTH",
        "study_mode": "paper_runtime",
        "artifact_rebuilt": False,
    }
    lifecycle_records = normalize_trade_lifecycle_records(
        trade_rows,
        entry_model=CURRENT_CANDLE_VWAP,
        pnl_truth_basis=PAPER_RUNTIME_LEDGER,
        lifecycle_truth_class=lifecycle_truth_class,
        truth_provenance=truth_provenance,
        record_source="PAPER_RUNTIME_TRADE_LEDGER",
    )
    return {
        "entry_model": CURRENT_CANDLE_VWAP,
        "active_entry_model": CURRENT_CANDLE_VWAP,
        "supported_entry_models": [BASELINE_NEXT_BAR_OPEN, CURRENT_CANDLE_VWAP],
        "entry_model_supported": True,
        "execution_truth_emitter": "atp_phase3_timing_emitter",
        "intrabar_execution_authoritative": bool(latest_atp_entry_state or latest_atp_timing_state),
        "authoritative_intrabar_available": bool(latest_atp_entry_state or latest_atp_timing_state),
        "authoritative_entry_truth_available": entry_truth_available,
        "authoritative_exit_truth_available": exit_truth_available,
        "authoritative_trade_lifecycle_available": lifecycle_records_available,
        "lifecycle_records": lifecycle_records,
        "authoritative_trade_lifecycle_records": lifecycle_records,
        "pnl_truth_basis": PAPER_RUNTIME_LEDGER,
        "lifecycle_truth_class": lifecycle_truth_class,
        "unsupported_reason": None,
        "truth_provenance": truth_provenance,
    }


def _atpe_target_health_summary(*, latest_feature: Any | None, side: str) -> dict[str, Any]:
    if latest_feature is None:
        return {
            "healthy": False,
            "reason": "missing_feature_state",
        }
    normalized_side = str(side).upper()
    if normalized_side == "LONG":
        trend_ok = latest_feature.trend_state in {"UP", "STRONG_UP"}
        momentum_ok = latest_feature.momentum_persistence in {"PERSISTENT_UP", "MIXED"}
        agreement_ok = latest_feature.mtf_agreement_state in {"ALIGNED_UP", "MIXED"}
        anatomy_ok = latest_feature.bar_anatomy in {"BULL_IMPULSE", "BALANCED", "LOWER_REJECTION"}
        reference_ok = latest_feature.reference_state in {"ABOVE_SESSION_OPEN", "MID_RANGE", "NEAR_RECENT_HIGH"}
        expansion_ok = latest_feature.expansion_state in {"NORMAL", "EXPANDED"}
        direction_ok = latest_feature.direction_bias == "LONG_BIAS"
    else:
        trend_ok = latest_feature.trend_state in {"DOWN", "STRONG_DOWN"}
        momentum_ok = latest_feature.momentum_persistence in {"PERSISTENT_DOWN", "MIXED"}
        agreement_ok = latest_feature.mtf_agreement_state in {"ALIGNED_DOWN", "MIXED"}
        anatomy_ok = latest_feature.bar_anatomy in {"BEAR_IMPULSE", "BALANCED", "UPPER_REJECTION"}
        reference_ok = latest_feature.reference_state in {"BELOW_SESSION_OPEN", "MID_RANGE", "NEAR_RECENT_LOW"}
        expansion_ok = latest_feature.expansion_state in {"NORMAL", "EXPANDED"}
        direction_ok = latest_feature.direction_bias == "SHORT_BIAS"
    healthy = direction_ok and trend_ok and agreement_ok and sum(
        1 for value in (momentum_ok, anatomy_ok, reference_ok, expansion_ok) if value
    ) >= 3
    return {
        "healthy": healthy,
        "trend_state": latest_feature.trend_state,
        "momentum_persistence": latest_feature.momentum_persistence,
        "bar_anatomy": latest_feature.bar_anatomy,
        "reference_state": latest_feature.reference_state,
        "expansion_state": latest_feature.expansion_state,
        "mtf_agreement_state": latest_feature.mtf_agreement_state,
        "direction_bias": latest_feature.direction_bias,
    }


def _atpe_target_checkpoint_should_continue(*, latest_feature: Any | None, side: str) -> bool:
    return bool(_atpe_target_health_summary(latest_feature=latest_feature, side=side).get("healthy"))


def _atpe_target_checkpoint_stop_price(
    *,
    plan: dict[str, Any],
    bar: Bar,
    side: str,
) -> float:
    current_stop = float(plan["stop_price"])
    entry_fill_price = float(plan["entry_fill_price"])
    risk_points = max(float(plan.get("risk_points") or 0.0), 0.25)
    normalized_side = str(side).upper()
    if normalized_side == "LONG":
        locked_profit_stop = entry_fill_price + risk_points * ATPE_TARGET_CHECKPOINT_LOCK_R
        structure_stop = float(bar.low) - risk_points * ATPE_TARGET_CHECKPOINT_TRAIL_R
        return max(current_stop, locked_profit_stop, structure_stop)
    locked_profit_stop = entry_fill_price - risk_points * ATPE_TARGET_CHECKPOINT_LOCK_R
    structure_stop = float(bar.high) + risk_points * ATPE_TARGET_CHECKPOINT_TRAIL_R
    return min(current_stop, locked_profit_stop, structure_stop)


def simulate_atpe_exit_policy_on_bars(
    *,
    bars_1m: Sequence[ResearchBar],
    variant: PatternVariant,
    instrument: str,
    point_value: float,
    quality_bucket_policy: str,
    exit_policy: str,
    higher_priority_signals: Sequence[HigherPrioritySignal] = (),
) -> list[dict[str, Any]]:
    sorted_bars = sorted(
        [bar for bar in bars_1m if bar.instrument == instrument],
        key=lambda item: item.end_ts,
    )
    if not sorted_bars:
        return []
    bars_5m = _resample_research_bars_5m(sorted_bars)
    if bars_5m:
        first_1m_ts = sorted_bars[0].end_ts
        last_1m_ts = sorted_bars[-1].end_ts
        bars_5m = [candidate for candidate in bars_5m if first_1m_ts <= candidate.end_ts <= last_1m_ts]
    feature_rows = sorted(
        build_feature_states(bars_5m=bars_5m, bars_1m=sorted_bars),
        key=lambda item: item.decision_ts,
    )
    decision_rows = sorted(
        generate_signal_decisions(
            feature_rows=feature_rows,
            variants=[variant],
            higher_priority_signals=higher_priority_signals,
        ),
        key=lambda item: item.decision_ts,
    )
    filtered_decisions: list[Any] = []
    for decision in decision_rows:
        if decision.instrument != instrument:
            continue
        if quality_bucket_policy == "MEDIUM_HIGH_ONLY" and decision.setup_quality_bucket not in {"MEDIUM", "HIGH"}:
            continue
        if quality_bucket_policy == "HIGH_ONLY" and decision.setup_quality_bucket != "HIGH":
            continue
        if str(decision.conflict_outcome.value) != "no_conflict":
            continue
        filtered_decisions.append(decision)

    pending_candidates: list[dict[str, Any]] = []
    pending_fill: dict[str, Any] | None = None
    active_trade_plan: dict[str, Any] | None = None
    closed_trades: list[dict[str, Any]] = []
    open_trade: dict[str, Any] | None = None
    decision_index = 0
    feature_index = 0
    latest_feature: Any | None = None

    for bar in sorted_bars:
        while feature_index < len(feature_rows) and feature_rows[feature_index].decision_ts <= bar.end_ts:
            latest_feature = feature_rows[feature_index]
            feature_index += 1

        if pending_fill is not None:
            if pending_fill["kind"] == "entry":
                plan = dict(pending_fill["plan"])
                entry_fill_price = float(bar.open)
                risk = max(float(plan["average_range"]) * float(plan["stop_atr_multiple"]), 0.25)
                if str(plan["side"]).upper() == "LONG":
                    stop_price = float(plan["decision_bar_low"]) - risk
                    target_price = (
                        entry_fill_price + risk * float(plan["target_r_multiple"])
                        if plan.get("target_r_multiple") is not None
                        else None
                    )
                else:
                    stop_price = float(plan["decision_bar_high"]) + risk
                    target_price = (
                        entry_fill_price - risk * float(plan["target_r_multiple"])
                        if plan.get("target_r_multiple") is not None
                        else None
                    )
                active_trade_plan = {
                    **plan,
                    "entry_fill_timestamp": bar.end_ts.isoformat(),
                    "entry_fill_price": entry_fill_price,
                    "risk_points": risk,
                    "stop_price": stop_price,
                    "initial_stop_price": stop_price,
                    "target_price": target_price,
                    "target_checkpoint_price": target_price,
                    "target_checkpoint_reached": False,
                    "target_checkpoint_reached_at": None,
                    "exit_policy": exit_policy,
                    "max_exit_timestamp": (
                        bar.end_ts + timedelta(minutes=int(plan["max_hold_bars_1m"]))
                    ).isoformat(),
                }
                open_trade = {
                    "instrument": instrument,
                    "variant_id": variant.variant_id,
                    "side": str(plan["side"]).upper(),
                    "decision_id": plan["decision_id"],
                    "entry_timestamp": bar.end_ts.isoformat(),
                    "entry_price": entry_fill_price,
                    "target_price": target_price,
                    "risk_points": risk,
                    "setup_quality_bucket": plan["setup_quality_bucket"],
                }
            else:
                if open_trade is not None:
                    exit_fill_price = float(bar.open)
                    direction = str(open_trade["side"]).upper()
                    pnl_points = (
                        exit_fill_price - float(open_trade["entry_price"])
                        if direction == "LONG"
                        else float(open_trade["entry_price"]) - exit_fill_price
                    )
                    closed_trades.append(
                        {
                            **open_trade,
                            "exit_timestamp": bar.end_ts.isoformat(),
                            "exit_price": exit_fill_price,
                            "exit_reason": pending_fill["exit_reason"],
                            "pnl_points": pnl_points,
                            "realized_pnl": pnl_points * float(point_value),
                            "checkpoint_reached": bool(active_trade_plan and active_trade_plan.get("target_checkpoint_reached")),
                        }
                    )
                active_trade_plan = None
                open_trade = None
            pending_fill = None

        if active_trade_plan is not None and pending_fill is None:
            side = str(active_trade_plan["side"]).upper()
            stop_price = float(active_trade_plan["stop_price"])
            target_price = (
                float(active_trade_plan["target_price"])
                if active_trade_plan.get("target_price") is not None
                else None
            )
            if bool(active_trade_plan.get("target_checkpoint_reached")):
                ratcheted_stop = _atpe_target_checkpoint_stop_price(
                    plan=active_trade_plan,
                    bar=_research_bar_to_domain_bar(bar),
                    side=side,
                )
                active_trade_plan["stop_price"] = ratcheted_stop
                stop_price = ratcheted_stop
            exit_reason: str | None = None
            if side == "LONG":
                stop_hit = float(bar.low) <= stop_price
                target_hit = (
                    not bool(active_trade_plan.get("target_checkpoint_reached"))
                    and target_price is not None
                    and float(bar.high) >= target_price
                )
                if stop_hit and target_hit:
                    exit_reason = "atpe_stop_first_conflict"
                elif stop_hit:
                    exit_reason = (
                        "atpe_checkpoint_stop"
                        if bool(active_trade_plan.get("target_checkpoint_reached"))
                        else "atpe_stop"
                    )
                elif target_hit:
                    if exit_policy == ATPE_EXIT_POLICY_TARGET_CHECKPOINT and _atpe_target_checkpoint_should_continue(
                        latest_feature=latest_feature,
                        side=side,
                    ):
                        active_trade_plan["target_checkpoint_reached"] = True
                        active_trade_plan["target_checkpoint_reached_at"] = bar.end_ts.isoformat()
                        active_trade_plan["stop_price"] = _atpe_target_checkpoint_stop_price(
                            plan=active_trade_plan,
                            bar=_research_bar_to_domain_bar(bar),
                            side=side,
                        )
                        active_trade_plan["target_price"] = None
                    else:
                        exit_reason = "atpe_target"
                elif bool(active_trade_plan.get("target_checkpoint_reached")) and exit_policy == ATPE_EXIT_POLICY_TARGET_CHECKPOINT and not _atpe_target_checkpoint_should_continue(
                    latest_feature=latest_feature,
                    side=side,
                ):
                    exit_reason = "atpe_target_momentum_fade"
            else:
                stop_hit = float(bar.high) >= stop_price
                target_hit = (
                    not bool(active_trade_plan.get("target_checkpoint_reached"))
                    and target_price is not None
                    and float(bar.low) <= target_price
                )
                if stop_hit and target_hit:
                    exit_reason = "atpe_stop_first_conflict"
                elif stop_hit:
                    exit_reason = (
                        "atpe_checkpoint_stop"
                        if bool(active_trade_plan.get("target_checkpoint_reached"))
                        else "atpe_stop"
                    )
                elif target_hit:
                    if exit_policy == ATPE_EXIT_POLICY_TARGET_CHECKPOINT and _atpe_target_checkpoint_should_continue(
                        latest_feature=latest_feature,
                        side=side,
                    ):
                        active_trade_plan["target_checkpoint_reached"] = True
                        active_trade_plan["target_checkpoint_reached_at"] = bar.end_ts.isoformat()
                        active_trade_plan["stop_price"] = _atpe_target_checkpoint_stop_price(
                            plan=active_trade_plan,
                            bar=_research_bar_to_domain_bar(bar),
                            side=side,
                        )
                        active_trade_plan["target_price"] = None
                    else:
                        exit_reason = "atpe_target"
                elif bool(active_trade_plan.get("target_checkpoint_reached")) and exit_policy == ATPE_EXIT_POLICY_TARGET_CHECKPOINT and not _atpe_target_checkpoint_should_continue(
                    latest_feature=latest_feature,
                    side=side,
                ):
                    exit_reason = "atpe_target_momentum_fade"
            if exit_reason is None and bar.end_ts >= datetime.fromisoformat(str(active_trade_plan["max_exit_timestamp"])):
                exit_reason = "atpe_time_stop"
            if exit_reason is not None:
                pending_fill = {"kind": "exit", "exit_reason": exit_reason}

        if active_trade_plan is None and pending_fill is None:
            surviving_candidates: list[dict[str, Any]] = []
            for candidate in pending_candidates:
                decision_ts = datetime.fromisoformat(str(candidate["decision_ts"]))
                expires_at = datetime.fromisoformat(str(candidate["expires_at"]))
                if bar.end_ts <= decision_ts:
                    surviving_candidates.append(candidate)
                    continue
                if bar.end_ts > expires_at:
                    continue
                side = str(candidate["side"]).upper()
                trigger_price = float(candidate["trigger_price"])
                triggered = (side == "LONG" and float(bar.high) >= trigger_price) or (
                    side == "SHORT" and float(bar.low) <= trigger_price
                )
                if not triggered:
                    surviving_candidates.append(candidate)
                    continue
                pending_fill = {"kind": "entry", "plan": dict(candidate)}
                break
            pending_candidates = surviving_candidates

        while decision_index < len(filtered_decisions) and filtered_decisions[decision_index].decision_ts <= bar.end_ts:
            decision = filtered_decisions[decision_index]
            pending_candidates.append(
                {
                    "decision_id": decision.decision_id,
                    "instrument": decision.instrument,
                    "variant_id": decision.variant_id,
                    "side": decision.side,
                    "decision_ts": decision.decision_ts.isoformat(),
                    "expires_at": (
                        decision.decision_ts + timedelta(minutes=variant.entry_window_bars_1m)
                    ).isoformat(),
                    "decision_bar_high": decision.decision_bar_high,
                    "decision_bar_low": decision.decision_bar_low,
                    "average_range": decision.average_range,
                    "trigger_price": _atpe_entry_trigger_price(decision=decision, variant=variant),
                    "setup_signature": decision.setup_signature,
                    "setup_quality_bucket": decision.setup_quality_bucket,
                    "max_hold_bars_1m": variant.max_hold_bars_1m,
                    "stop_atr_multiple": variant.stop_atr_multiple,
                    "target_r_multiple": variant.target_r_multiple,
                    "reason_code": decision.variant_id,
                    "signal_source": decision.variant_id,
                }
            )
            decision_index += 1

    return closed_trades


def _runtime_processed_bar_row(bar: ResearchBar, spec: ProbationaryPaperLaneSpec) -> dict[str, Any]:
    return {
        "lane_id": spec.lane_id,
        "lane_name": spec.display_name,
        "experimental_status": spec.experimental_status,
        "paper_only": spec.paper_only,
        "symbol": bar.instrument,
        "timeframe": bar.timeframe,
        "start_ts": bar.start_ts.isoformat(),
        "end_ts": bar.end_ts.isoformat(),
        "open": bar.open,
        "high": bar.high,
        "low": bar.low,
        "close": bar.close,
        "volume": bar.volume,
        "session_label": bar.session_label,
        "session_segment": bar.session_segment,
        "provenance": bar.provenance,
    }


def _runtime_feature_row(feature: Any, spec: ProbationaryPaperLaneSpec) -> dict[str, Any]:
    return {
        "lane_id": spec.lane_id,
        "lane_name": spec.display_name,
        "experimental_status": spec.experimental_status,
        "paper_only": spec.paper_only,
        "symbol": feature.instrument,
        "feature_timestamp": feature.decision_ts.isoformat(),
        "session_date": feature.session_date.isoformat(),
        "session_label": feature.session_label,
        "session_segment": feature.session_segment,
        "trend_state": feature.trend_state,
        "pullback_state": feature.pullback_state,
        "expansion_state": feature.expansion_state,
        "momentum_persistence": feature.momentum_persistence,
        "bar_anatomy": feature.bar_anatomy,
        "volatility_bucket": feature.volatility_bucket,
        "regime_bucket": feature.regime_bucket,
        "mtf_agreement_state": feature.mtf_agreement_state,
        "direction_bias": feature.direction_bias,
        "atp_bias_state": feature.atp_bias_state,
        "atp_bias_score": feature.atp_bias_score,
        "atp_bias_reasons": list(feature.atp_bias_reasons),
        "atp_long_bias_blockers": list(feature.atp_long_bias_blockers),
        "atp_short_bias_blockers": list(feature.atp_short_bias_blockers),
        "atp_fast_ema": feature.atp_fast_ema,
        "atp_slow_ema": feature.atp_slow_ema,
        "atp_slow_ema_slope_norm": feature.atp_slow_ema_slope_norm,
        "atp_session_vwap": feature.atp_session_vwap,
        "atp_directional_persistence_score": feature.atp_directional_persistence_score,
        "atp_trend_extension_norm": feature.atp_trend_extension_norm,
        "atp_pullback_state": feature.atp_pullback_state,
        "atp_pullback_envelope_state": feature.atp_pullback_envelope_state,
        "atp_pullback_reason": feature.atp_pullback_reason,
        "atp_pullback_depth_points": feature.atp_pullback_depth_points,
        "atp_pullback_depth_score": feature.atp_pullback_depth_score,
        "atp_pullback_violence_score": feature.atp_pullback_violence_score,
        "atp_pullback_min_reset_depth": feature.atp_pullback_min_reset_depth,
        "atp_pullback_standard_depth": feature.atp_pullback_standard_depth,
        "atp_pullback_stretched_depth": feature.atp_pullback_stretched_depth,
        "atp_pullback_disqualify_depth": feature.atp_pullback_disqualify_depth,
        "atp_pullback_retracement_ratio": feature.atp_pullback_retracement_ratio,
        "atp_countertrend_velocity_norm": feature.atp_countertrend_velocity_norm,
        "atp_countertrend_range_expansion": feature.atp_countertrend_range_expansion,
        "atp_structure_damage": feature.atp_structure_damage,
        "atp_reference_displacement": feature.atp_reference_displacement,
    }


def _runtime_atpe_signal_row(
    *,
    spec: ProbationaryPaperLaneSpec,
    decision: Any,
    kill_switch_active: bool,
    observed_instruments: Sequence[str],
) -> dict[str, Any]:
    if kill_switch_active:
        allow_block_reason = "blocked_kill_switch"
        override_reason = "canary_kill_switch_active"
        signal_passed_flag = False
    elif str(decision.conflict_outcome.value) == "no_conflict":
        allow_block_reason = "allowed_no_conflict"
        override_reason = "paper_only_experimental_canary"
        signal_passed_flag = True
    elif str(decision.conflict_outcome.value) == "agreement":
        allow_block_reason = "blocked_higher_priority_agreement"
        override_reason = decision.block_reason or "agreement"
        signal_passed_flag = False
    elif str(decision.conflict_outcome.value) == "soft_conflict":
        allow_block_reason = "blocked_soft_conflict"
        override_reason = decision.block_reason or "soft_conflict"
        signal_passed_flag = False
    else:
        allow_block_reason = "blocked_hard_conflict_cooldown"
        override_reason = decision.block_reason or "hard_conflict_cooldown"
        signal_passed_flag = False
    return {
        "lane_id": spec.lane_id,
        "lane_name": spec.display_name,
        "experimental_status": spec.experimental_status,
        "paper_only": spec.paper_only,
        "non_approved": spec.non_approved,
        "symbol": decision.instrument,
        "instrument_scope": ",".join(observed_instruments),
        "variant_id": decision.variant_id,
        "family": decision.family,
        "side": decision.side,
        "signal_timestamp": decision.decision_ts.isoformat(),
        "bar_end_ts": decision.decision_ts.isoformat(),
        "session_date": decision.session_date.isoformat(),
        "session_segment": decision.session_segment,
        "decision_id": decision.decision_id,
        "decision": "allowed" if signal_passed_flag else "blocked",
        "signal_passed_flag": signal_passed_flag,
        "paper_canary_eligible": signal_passed_flag,
        "live_eligible": False,
        "shadow_only": False,
        "quality_bucket_policy": spec.quality_bucket_policy,
        "quality_bucket": decision.setup_quality_bucket,
        "setup_quality_score": decision.setup_quality_score,
        "conflict_outcome": decision.conflict_outcome.value,
        "allow_block_reason": allow_block_reason,
        "override_reason": override_reason,
        "rejection_reason_code": None if signal_passed_flag else (override_reason or allow_block_reason),
        "block_reason": None if signal_passed_flag else (override_reason or decision.block_reason or allow_block_reason),
        "priority_tier": "lower_priority_than_live_strategies",
        "lower_priority_policy": "yield_to_higher_priority_live_strategies",
        "setup_signature": decision.setup_signature,
        "setup_state_signature": decision.setup_state_signature,
        "feature_snapshot": decision.feature_snapshot,
    }


def _runtime_atp_companion_signal_row(
    *,
    spec: ProbationaryPaperLaneSpec,
    entry_state: Any | None,
    timing_state: Any,
    observed_instruments: Sequence[str],
) -> dict[str, Any]:
    signal_passed_flag = bool(timing_state.executable_entry)
    blocker = timing_state.primary_blocker or (entry_state.primary_blocker if entry_state is not None else None)
    return {
        "lane_id": spec.lane_id,
        "lane_name": spec.display_name,
        "experimental_status": spec.experimental_status,
        "paper_only": spec.paper_only,
        "non_approved": spec.non_approved,
        "symbol": timing_state.instrument,
        "instrument_scope": ",".join(observed_instruments),
        "variant_id": spec.observer_variant_id,
        "family": timing_state.family_name,
        "side": spec.observer_side,
        "signal_timestamp": timing_state.decision_ts.isoformat(),
        "bar_end_ts": timing_state.timing_bar_ts.isoformat() if timing_state.timing_bar_ts is not None else timing_state.decision_ts.isoformat(),
        "session_date": timing_state.session_date.isoformat(),
        "session_segment": timing_state.session_segment,
        "decision_id": f"{timing_state.instrument}|{timing_state.family_name}|{timing_state.decision_ts.isoformat()}",
        "decision": "allowed" if signal_passed_flag else "blocked",
        "signal_passed_flag": signal_passed_flag,
        "paper_canary_eligible": signal_passed_flag,
        "live_eligible": False,
        "shadow_only": False,
        "quality_bucket_policy": spec.quality_bucket_policy,
        "quality_bucket": (entry_state.setup_quality_bucket if entry_state is not None else None),
        "setup_quality_score": (entry_state.setup_quality_score if entry_state is not None else None),
        "conflict_outcome": "no_conflict",
        "allow_block_reason": "allowed_benchmark_timing_confirmed" if signal_passed_flag else "blocked_benchmark_timing",
        "override_reason": "tracked_paper_benchmark" if signal_passed_flag else blocker,
        "rejection_reason_code": None if signal_passed_flag else blocker,
        "block_reason": None if signal_passed_flag else blocker,
        "priority_tier": "paper_tracking_pre_live_soak",
        "lower_priority_policy": "yield_to_higher_priority_live_strategies",
        "setup_signature": timing_state.feature_snapshot.get("setup_signature"),
        "setup_state_signature": timing_state.feature_snapshot.get("setup_state_signature")
        or timing_state.feature_snapshot.get("setup_signature"),
        "feature_snapshot": timing_state.feature_snapshot,
    }


def _allow_block_override_summary(rows: Sequence[dict[str, Any]]) -> dict[str, Any]:
    allowed = sum(1 for row in rows if bool(row.get("signal_passed_flag")))
    blocked = sum(1 for row in rows if str(row.get("decision") or "").lower() == "blocked")
    reasons = Counter(str(row.get("override_reason") or "") for row in rows if row.get("override_reason"))
    top_override = reasons.most_common(1)[0][0] if reasons else None
    label = f"allowed={allowed} blocked={blocked}" + (f" override={top_override}" if top_override else "")
    return {
        "allowed": allowed,
        "blocked": blocked,
        "top_override_reason": top_override,
        "label": label,
    }


def _probationary_supervisor_higher_priority_signals(
    lanes: Sequence[ProbationaryPaperLaneRuntime],
) -> list[HigherPrioritySignal]:
    rows: list[HigherPrioritySignal] = []
    for lane in lanes:
        if getattr(lane.spec, "runtime_kind", "") in {
            ATPE_CANARY_RUNTIME_KIND,
            ATP_COMPANION_BENCHMARK_RUNTIME_KIND,
            GC_MGC_ACCEPTANCE_RUNTIME_KIND,
        }:
            continue
        state = lane.strategy_engine.state
        if state.position_side not in {PositionSide.LONG, PositionSide.SHORT} or state.entry_timestamp is None:
            continue
        rows.append(
            HigherPrioritySignal(
                instrument=str(lane.spec.symbol).strip().upper(),
                side=state.position_side.value,
                start_ts=state.entry_timestamp,
                end_ts=None,
                reason=f"probationary_runtime:{lane.spec.lane_id}",
                cooldown=bool(state.operator_halt or state.same_underlying_entry_hold),
            )
        )
    return rows


def _resolve_session_date(settings: StrategySettings, operator_status: dict[str, Any]) -> date:
    timestamp = operator_status.get("last_processed_bar_end_ts") or operator_status.get("updated_at")
    if timestamp:
        return datetime.fromisoformat(timestamp).astimezone(settings.timezone_info).date()
    return datetime.now(settings.timezone_info).date()


def _lane_session_override_active_for_realized_loser(
    lane_state: dict[str, Any],
    *,
    session_date: str,
) -> bool:
    return (
        bool(lane_state.get("session_override_active"))
        and str(lane_state.get("session_override_session_date") or "") == session_date
        and str(lane_state.get("session_override_reason") or "") == REALIZED_LOSER_SESSION_OVERRIDE_REASON
    )


def _records_for_session_date(
    records: Iterable[dict[str, Any]],
    session_date: date,
    timestamp_field: str,
    timezone_info,
) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for record in records:
        timestamp = record.get(timestamp_field)
        if not timestamp:
            continue
        try:
            record_date = datetime.fromisoformat(timestamp).astimezone(timezone_info).date()
        except ValueError:
            continue
        if record_date == session_date:
            filtered.append(record)
    return filtered


def _count_bars_for_session_date(engine, session_date: date, settings: StrategySettings) -> int:
    rows = _load_table_rows_for_session_date(
        engine,
        bars_table,
        timestamp_column="end_ts",
        session_date=session_date,
        timezone_info=settings.timezone_info,
    )
    return sum(1 for row in rows if row.get("data_source") == "schwab_live_poll")


def _load_bars_for_session_date(engine, session_date: date, settings: StrategySettings) -> list[Bar]:
    rows = _load_table_rows_for_session_date(
        engine,
        bars_table,
        timestamp_column="end_ts",
        session_date=session_date,
        timezone_info=settings.timezone_info,
    )
    return [
        Bar(
            bar_id=row["bar_id"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            start_ts=datetime.fromisoformat(row["start_ts"]),
            end_ts=datetime.fromisoformat(row["end_ts"]),
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(row["volume"]),
            is_final=bool(row["is_final"]),
            session_asia=bool(row["session_asia"]),
            session_london=bool(row["session_london"]),
            session_us=bool(row["session_us"]),
            session_allowed=bool(row["session_allowed"]),
        )
        for row in rows
        if row.get("data_source") == "schwab_live_poll"
    ]


def _load_open_order_intent_rows(repositories: RepositorySet) -> list[dict[str, Any]]:
    intent_rows = repositories.order_intents.list_all()
    fill_rows = repositories.fills.list_all()
    filled_order_intent_ids = {row["order_intent_id"] for row in fill_rows}
    return [
        row
        for row in intent_rows
        if row["order_intent_id"] not in filled_order_intent_ids
        and row.get("order_status") not in {OrderStatus.CANCELLED.value, OrderStatus.REJECTED.value, OrderStatus.FILLED.value}
    ]


def _restore_validation_state_snapshot(
    *,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
) -> dict[str, Any]:
    open_order_rows = _load_open_order_intent_rows(repositories)
    latest_intent = max(
        repositories.order_intents.list_all(),
        key=lambda row: str(row.get("created_at") or ""),
        default=None,
    )
    latest_fill = max(
        repositories.fills.list_all(),
        key=lambda row: str(row.get("fill_timestamp") or ""),
        default=None,
    )
    broker = execution_engine.broker
    try:
        broker_snapshot = broker.snapshot_state()
    except Exception:
        broker_snapshot = {}
    return {
        "strategy_status": strategy_engine.state.strategy_status.value,
        "position_side": strategy_engine.state.position_side.value,
        "internal_position_qty": int(strategy_engine.state.internal_position_qty),
        "broker_position_qty": int(strategy_engine.state.broker_position_qty),
        "entry_price": strategy_engine.state.entry_price,
        "open_entry_leg_count": len(strategy_engine.state.open_entry_legs),
        "open_add_count": max(0, len(strategy_engine.state.open_entry_legs) - 1),
        "open_entry_leg_quantities": [int(leg.quantity) for leg in strategy_engine.state.open_entry_legs],
        "open_entry_leg_prices": [str(leg.entry_price) for leg in strategy_engine.state.open_entry_legs],
        "additional_entry_allowed": strategy_engine._can_add_to_existing_position(strategy_engine.state),  # noqa: SLF001
        "participation_policy": strategy_engine._settings.participation_policy.value,  # noqa: SLF001
        "open_broker_order_id": strategy_engine.state.open_broker_order_id,
        "last_order_intent_id": strategy_engine.state.last_order_intent_id,
        "long_entry_family": strategy_engine.state.long_entry_family.value,
        "short_entry_family": strategy_engine.state.short_entry_family.value,
        "long_break_even_armed": bool(strategy_engine.state.long_be_armed),
        "short_break_even_armed": bool(strategy_engine.state.short_be_armed),
        "bars_in_trade": int(strategy_engine.state.bars_in_trade),
        "reconcile_required": bool(strategy_engine.state.reconcile_required),
        "fault_code": strategy_engine.state.fault_code,
        "pending_execution_count": len(execution_engine.pending_executions()),
        "pending_broker_order_ids": [pending.broker_order_id for pending in execution_engine.pending_executions()],
        "open_order_count": len(open_order_rows),
        "latest_order_intent": latest_intent,
        "latest_fill": latest_fill,
        "latest_fill_timestamp": _latest_fill_timestamp_from_rows(repositories.fills.list_all()).isoformat()
        if _latest_fill_timestamp_from_rows(repositories.fills.list_all()) is not None
        else None,
        "broker_snapshot": broker_snapshot,
    }


def _restore_validation_record_counts(repositories: RepositorySet) -> dict[str, int]:
    return {
        "order_intent_count": len(repositories.order_intents.list_all()),
        "fill_count": len(repositories.fills.list_all()),
        "reconciliation_event_count": len(repositories.reconciliation_events.list_all()),
        "fault_event_count": len(repositories.fault_events.list_all()),
    }


def _restore_validation_result_label(
    *,
    reconciliation: dict[str, Any],
    strategy_engine: StrategyEngine,
    restore_adjustments: Sequence[str],
) -> str:
    classification = str(reconciliation.get("classification") or "").strip().lower()
    if strategy_engine.state.strategy_status is StrategyStatus.FAULT:
        return "FAULT"
    if strategy_engine.state.reconcile_required or strategy_engine.state.strategy_status is StrategyStatus.RECONCILING:
        return "RECONCILING"
    if classification == RECONCILIATION_CLASS_SAFE_REPAIR or restore_adjustments:
        return "SAFE_CLEANUP_READY"
    return "READY"


def _record_restore_validation(
    *,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    structured_logger: StructuredLogger,
    alert_dispatcher: AlertDispatcher,
    restore_started_at: datetime,
    reconciliation: dict[str, Any],
    scope_label: str,
    runtime_name: str,
    lane_id: str | None = None,
    instrument: str | None = None,
    restore_adjustments: Sequence[str] = (),
    before_state_summary: dict[str, Any] | None = None,
    before_counts: dict[str, int] | None = None,
) -> dict[str, Any]:
    restore_completed_at = datetime.now(timezone.utc)
    after_snapshot = _restore_validation_state_snapshot(
        repositories=repositories,
        strategy_engine=strategy_engine,
        execution_engine=execution_engine,
    )
    pre_counts = dict(before_counts or {})
    post_counts = _restore_validation_record_counts(repositories)
    result_label = _restore_validation_result_label(
        reconciliation=reconciliation,
        strategy_engine=strategy_engine,
        restore_adjustments=restore_adjustments,
    )
    latest_intent = after_snapshot.get("latest_order_intent") or {}
    latest_fill = after_snapshot.get("latest_fill") or {}
    safe_cleanup_actions = list(dict.fromkeys([*list(restore_adjustments), *list(reconciliation.get("repair_actions") or [])]))
    payload = {
        "event_type": "restore_validation",
        "trigger": "startup",
        "runtime_name": runtime_name,
        "scope_label": scope_label,
        "lane_id": lane_id,
        "instrument": instrument,
        "restore_started_at": restore_started_at.isoformat(),
        "restore_completed_at": restore_completed_at.isoformat(),
        "restore_result": result_label,
        "restore_classification": reconciliation.get("classification"),
        "clean": bool(reconciliation.get("clean")),
        "reconcile_required": bool(strategy_engine.state.reconcile_required),
        "fault_code": strategy_engine.state.fault_code,
        "safe_cleanup_applied": bool(safe_cleanup_actions),
        "safe_cleanup_actions": safe_cleanup_actions,
        "recommended_action": reconciliation.get("recommended_action") or (
            "No action needed." if result_label in {"READY", "SAFE_CLEANUP_READY"} else "Inspect restore state before resuming entries."
        ),
        "manual_action_required": result_label in {"RECONCILING", "FAULT"},
        "unresolved_restore_issue": result_label in {"RECONCILING", "FAULT"},
        "duplicate_action_prevention_held": (
            post_counts.get("order_intent_count") == pre_counts.get("order_intent_count", post_counts.get("order_intent_count"))
            and post_counts.get("fill_count") == pre_counts.get("fill_count", post_counts.get("fill_count"))
        ),
        "duplicate_action_prevention_detail": "Startup restore replayed persisted state without creating new order intents or fills.",
        "pre_restore_state_summary": before_state_summary or {},
        "restored_state_summary": {
            "strategy_status": after_snapshot.get("strategy_status"),
            "position_side": after_snapshot.get("position_side"),
            "internal_position_qty": after_snapshot.get("internal_position_qty"),
            "broker_position_qty": after_snapshot.get("broker_position_qty"),
            "open_broker_order_id": after_snapshot.get("open_broker_order_id"),
            "last_order_intent_id": after_snapshot.get("last_order_intent_id"),
            "latest_order_intent_state": latest_intent.get("order_status"),
            "latest_order_intent_timestamp": latest_intent.get("created_at"),
            "latest_fill_timestamp": latest_fill.get("fill_timestamp"),
            "latest_fill_broker_order_id": latest_fill.get("broker_order_id"),
            "broker_snapshot": after_snapshot.get("broker_snapshot"),
            "pending_execution_count": after_snapshot.get("pending_execution_count"),
            "pending_broker_order_ids": after_snapshot.get("pending_broker_order_ids"),
            "reconcile_required": after_snapshot.get("reconcile_required"),
            "fault_code": after_snapshot.get("fault_code"),
        },
        "reconciliation_summary": {
            "classification": reconciliation.get("classification"),
            "mismatches": list(reconciliation.get("mismatches") or []),
            "repair_actions": list(reconciliation.get("repair_actions") or []),
            "resulting_strategy_status": reconciliation.get("resulting_strategy_status"),
            "resulting_fault_code": reconciliation.get("resulting_fault_code") or reconciliation.get("fault_code"),
            "entries_frozen": bool(reconciliation.get("entries_frozen") or strategy_engine.state.reconcile_required or strategy_engine.state.fault_code),
        },
        "count_snapshot": {
            "before": pre_counts,
            "after": post_counts,
        },
    }
    structured_logger.write_restore_validation_state(payload)
    structured_logger.log_restore_validation_event(payload)

    dedup_key = f"{runtime_name}:{lane_id or 'root'}:startup_restore_validation"
    if result_label == "READY":
        alert_dispatcher.sync_condition(
            code="paper_restore_ready",
            active=False,
            severity="RECOVERY",
            category="runtime_recovery",
            title="State Restore Succeeded",
            message=f"{scope_label} restored persisted state cleanly and returned to READY.",
            payload=payload,
            dedup_key=dedup_key,
            recommended_action="No action needed.",
        )
    elif result_label == "SAFE_CLEANUP_READY":
        alert_dispatcher.sync_condition(
            code="paper_restore_safe_cleanup",
            active=False,
            severity="RECOVERY",
            category="runtime_recovery",
            title="State Restore Safe Cleanup",
            message=f"{scope_label} restored persisted state and applied safe cleanup before returning to READY.",
            payload=payload,
            dedup_key=dedup_key,
            recommended_action="No action needed; safe cleanup was applied automatically.",
        )
    elif result_label == "RECONCILING":
        alert_dispatcher.sync_condition(
            code="paper_restore_reconciling",
            active=True,
            severity="ACTION",
            category="state_restore_failure",
            title="State Restore Requires Reconciliation",
            message=f"{scope_label} restored state but found unresolved ambiguity; entries remain frozen in RECONCILING.",
            payload=payload,
            dedup_key=dedup_key,
            recommended_action=str(payload["recommended_action"]),
        )
    else:
        alert_dispatcher.sync_condition(
            code="paper_restore_fault",
            active=True,
            severity="BLOCKING",
            category="persistent_fault",
            title="State Restore Fault",
            message=f"{scope_label} restored state into FAULT because ambiguity remained unsafe after startup restore.",
            payload=payload,
            dedup_key=dedup_key,
            recommended_action=str(payload["recommended_action"]),
        )
    return payload


def _restore_paper_runtime_state(
    *,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
) -> None:
    broker = execution_engine.broker
    if not isinstance(broker, PaperBroker):
        return

    state = strategy_engine.state
    open_order_rows = _load_open_order_intent_rows(repositories)
    pending_executions = [_pending_execution_from_row(row) for row in open_order_rows]
    for pending in pending_executions:
        execution_engine.restore_pending_execution(pending)

    position_quantity = _strategy_state_to_signed_quantity(state)
    broker.restore_state(
        position=PaperPosition(quantity=position_quantity, average_price=state.entry_price),
        open_order_ids=[pending.broker_order_id for pending in pending_executions],
        order_status={pending.broker_order_id: OrderStatus.ACKNOWLEDGED for pending in pending_executions},
        last_fill_timestamp=_latest_fill_timestamp_from_rows(repositories.fills.list_all()),
    )


def _restore_live_runtime_state(
    *,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    broker_truth_service: SchwabProductionLinkService | Any | None = None,
) -> dict[str, Any]:
    open_order_rows = _load_open_order_intent_rows(repositories)
    pending_executions = [_pending_execution_from_row(row) for row in open_order_rows]
    for pending in pending_executions:
        execution_engine.restore_pending_execution(pending)
    return _load_live_strategy_broker_truth_snapshot(
        execution_engine=execution_engine,
        broker_truth_service=broker_truth_service,
        force_refresh=True,
    )


def _apply_probationary_operator_control(
    *,
    settings: StrategySettings,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    structured_logger: StructuredLogger,
    alert_dispatcher: AlertDispatcher,
) -> dict[str, Any] | None:
    control_path = settings.resolved_probationary_operator_control_path
    if not control_path.exists():
        return None
    payload = _read_json(control_path)
    now = datetime.now(timezone.utc)
    if payload.get("action") == "flatten_and_halt" and payload.get("status") in {"flatten_pending", "applied"}:
        finalized = _maybe_finalize_flatten_and_halt_control(
            payload=payload,
            control_path=control_path,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
            structured_logger=structured_logger,
            alert_dispatcher=alert_dispatcher,
            now=now,
        )
        return finalized
    if payload.get("status") != "pending":
        return payload

    action = str(payload.get("action", ""))
    result = dict(payload)
    result["applied_at"] = now.isoformat()
    result["control_path"] = str(control_path)

    if action == "halt_entries":
        strategy_engine.set_operator_halt(now, True)
        result["status"] = "applied"
        result["message"] = "Entries halted for paper runtime."
        result["halt_reason"] = "operator_halt_entries"
    elif action == "force_reconcile":
        reconciliation = strategy_engine.force_reconcile(
            occurred_at=now,
            execution_engine=execution_engine,
        )
        result["status"] = "applied"
        result["message"] = (
            "Force Reconcile completed and the runtime is aligned."
            if reconciliation.get("clean") or reconciliation.get("classification") == "safe_repair"
            else "Force Reconcile completed, but runtime still requires review before entries can resume."
        )
        result["reconciliation"] = reconciliation
    elif action == "clear_risk_halts":
        reconciliation = _reconcile_paper_runtime(
            repositories=repositories,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
        )
        state = strategy_engine.state
        if (
            reconciliation["clean"]
            and state.position_side == PositionSide.FLAT
            and state.open_broker_order_id is None
            and state.internal_position_qty == 0
            and state.broker_position_qty == 0
            and state.fault_code is None
        ):
            result["status"] = "applied"
            result["message"] = "Paper risk halts cleared. Use Resume Entries to re-arm the runtime."
        else:
            result["status"] = "rejected"
            result["message"] = "Clear Risk Halts rejected because runtime is not safely flat/reconciled."
            result["reconciliation"] = reconciliation
    elif action == "resume_entries":
        strategy_engine.set_operator_halt(now, False)
        result["status"] = "applied"
        result["message"] = "Entries resumed for paper runtime."
        result["halt_reason"] = None
        result["flatten_state"] = None
    elif action == LIVE_STRATEGY_PILOT_REARM_ACTION:
        state = strategy_engine.state
        if (
            state.position_side == PositionSide.FLAT
            and state.internal_position_qty == 0
            and state.broker_position_qty == 0
            and state.open_broker_order_id is None
            and not execution_engine.pending_executions()
            and state.fault_code is None
            and state.strategy_status is not StrategyStatus.RECONCILING
        ):
            payload = _default_live_strategy_pilot_cycle_state(settings=settings, armed_at=now)
            _live_strategy_pilot_cycle_path(settings).write_text(json.dumps(payload, sort_keys=True, indent=2) + "\n", encoding="utf-8")
            result["status"] = "applied"
            result["message"] = "Live strategy pilot cycle re-armed. Submit remains subject to all normal live gates."
            result["rearm_action"] = LIVE_STRATEGY_PILOT_REARM_ACTION
        else:
            result["status"] = "rejected"
            result["message"] = "Live strategy pilot re-arm rejected because runtime is not safely flat and clear."
    elif action == "clear_fault":
        reconciliation = _reconcile_paper_runtime(
            repositories=repositories,
            strategy_engine=strategy_engine,
            execution_engine=execution_engine,
        )
        state = strategy_engine.state
        if (
            reconciliation["clean"]
            and state.position_side == PositionSide.FLAT
            and state.open_broker_order_id is None
            and state.internal_position_qty == 0
            and state.broker_position_qty == 0
        ):
            strategy_engine.clear_fault(now)
            result["status"] = "applied"
            result["message"] = "Fault cleared and runtime returned to READY."
        else:
            result["status"] = "rejected"
            result["message"] = "Fault clear rejected because runtime is not safely flat/reconciled."
            result["reconciliation"] = reconciliation
    elif action == "flatten_and_halt":
        strategy_engine.set_operator_halt(now, True)
        result["halt_reason"] = "operator_flatten_and_halt"
        state = strategy_engine.state
        if state.open_broker_order_id is not None or execution_engine.pending_executions():
            result["status"] = "rejected"
            result["flatten_state"] = "rejected_open_order_uncertainty"
            result["message"] = "Flatten And Halt rejected because an open paper order is already pending."
            result["reconciliation"] = _reconcile_paper_runtime(
                repositories=repositories,
                strategy_engine=strategy_engine,
                execution_engine=execution_engine,
            )
        elif (
            state.position_side == PositionSide.FLAT
            and state.internal_position_qty == 0
            and state.broker_position_qty == 0
        ):
            result["status"] = "applied"
            result["flatten_state"] = "complete"
            result["message"] = "Runtime halted and already flat."
        else:
            try:
                intent = strategy_engine.submit_operator_flatten_intent(now)
            except ValueError as exc:
                result["status"] = "rejected"
                result["flatten_state"] = "rejected"
                result["message"] = str(exc)
            else:
                if intent is None:
                    result["status"] = "applied"
                    result["flatten_state"] = "complete"
                    result["message"] = "Runtime halted and already flat."
                else:
                    result["status"] = "flatten_pending"
                    result["flatten_state"] = "pending_fill"
                    result["flatten_order_intent_id"] = intent.order_intent_id
                    result["message"] = "Flatten intent submitted; runtime remains halted until the paper exit fills and flatness is confirmed."
    elif action == "stop_after_cycle":
        strategy_engine.set_operator_halt(now, True)
        result["status"] = "applied"
        result["halt_reason"] = "operator_stop_after_cycle"
        result["stop_after_cycle_requested"] = True
        result["message"] = "Stop After Current Cycle requested; entries are halted and the paper runtime will stop at the next safe point."
    else:
        result["status"] = "rejected"
        result["message"] = f"Unsupported control action: {action}"

    control_path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
    structured_logger.log_operator_control(result)
    alert_dispatcher.emit(
        "info" if result["status"] == "applied" else "warning",
        "operator_control_applied" if result["status"] == "applied" else "operator_control_rejected",
        result["message"],
        result,
    )
    return result


def _maybe_finalize_flatten_and_halt_control(
    *,
    payload: dict[str, Any],
    control_path: Path,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    structured_logger: StructuredLogger,
    alert_dispatcher: AlertDispatcher,
    now: datetime,
) -> dict[str, Any]:
    state = strategy_engine.state
    pending_orders = execution_engine.pending_executions()
    if (
        state.position_side == PositionSide.FLAT
        and state.internal_position_qty == 0
        and state.broker_position_qty == 0
        and state.open_broker_order_id is None
        and not pending_orders
    ):
        result = dict(payload)
        result["status"] = "applied"
        result["flatten_state"] = "complete"
        result["completed_at"] = now.isoformat()
        result["message"] = "Flatten And Halt completed; runtime is halted and flat."
        control_path.write_text(json.dumps(result, sort_keys=True, indent=2) + "\n", encoding="utf-8")
        structured_logger.log_operator_control(result)
        alert_dispatcher.emit(
            "info",
            "operator_control_applied",
            result["message"],
            result,
        )
        return result
    return payload


def _stop_after_cycle_is_safe(
    control_result: dict[str, Any] | None,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
) -> bool:
    if control_result is None:
        return False
    if control_result.get("action") != "stop_after_cycle":
        return False
    if control_result.get("status") != "applied":
        return False
    state = strategy_engine.state
    return (
        state.position_side == PositionSide.FLAT
        and state.internal_position_qty == 0
        and state.broker_position_qty == 0
        and state.open_broker_order_id is None
        and not execution_engine.pending_executions()
    )


def _pending_execution_from_row(row: dict[str, Any]) -> PendingExecution:
    intent = decode_order_intent(dict(row))
    signal_bar_id = intent.bar_id if intent.is_entry else None
    long_entry_family = LongEntryFamily.K if intent.intent_type == OrderIntentType.BUY_TO_OPEN else LongEntryFamily.NONE
    short_entry_family = (
        ShortEntryFamily.ASIA_EARLY_PAUSE_RESUME_SHORT
        if intent.reason_code == "asiaEarlyPauseResumeShortTurn" and intent.intent_type == OrderIntentType.SELL_TO_OPEN
        else ShortEntryFamily.NONE
    )
    short_entry_source = intent.reason_code if intent.intent_type == OrderIntentType.SELL_TO_OPEN else None
    return PendingExecution(
        intent=intent,
        broker_order_id=row.get("broker_order_id") or f"paper-{intent.order_intent_id}",
        submitted_at=_parse_iso_datetime_or_none(row.get("submitted_at")) or intent.created_at,
        acknowledged_at=(
            _parse_iso_datetime_or_none(row.get("acknowledged_at"))
            or (intent.created_at if str(row.get("order_status") or "").upper() in {OrderStatus.ACKNOWLEDGED.value, OrderStatus.FILLED.value} else None)
        ),
        broker_order_status=str(row.get("broker_order_status") or row.get("order_status") or "").strip().upper() or None,
        last_status_checked_at=(
            _parse_iso_datetime_or_none(row.get("last_status_checked_at"))
            or _parse_iso_datetime_or_none(row.get("acknowledged_at"))
            or _parse_iso_datetime_or_none(row.get("submitted_at"))
            or intent.created_at
        ),
        retry_count=int(row.get("retry_count") or 0),
        signal_bar_id=signal_bar_id,
        long_entry_family=long_entry_family,
        short_entry_family=short_entry_family,
        short_entry_source=short_entry_source,
    )


def _reconcile_paper_runtime(
    *,
    repositories: RepositorySet,
    strategy_engine: StrategyEngine,
    execution_engine: ExecutionEngine,
    trigger: str = "scheduled_heartbeat",
    apply_repairs: bool = False,
    occurred_at: datetime | None = None,
) -> dict[str, Any]:
    observed_at = occurred_at or datetime.now(timezone.utc)
    if apply_repairs:
        payload = strategy_engine.apply_reconciliation(
            occurred_at=observed_at,
            trigger=trigger,
            execution_engine=execution_engine,
        )
    else:
        payload = strategy_engine.inspect_reconciliation(
            occurred_at=observed_at,
            trigger=trigger,
            execution_engine=execution_engine,
        )
    payload.setdefault("logged_at", observed_at.isoformat())
    return payload


def _latest_fill_timestamp_from_rows(fill_rows: Sequence[dict[str, Any]]) -> datetime | None:
    latest: datetime | None = None
    for row in fill_rows:
        fill_timestamp = str(row.get("fill_timestamp") or "").strip()
        if not fill_timestamp:
            continue
        try:
            parsed = datetime.fromisoformat(fill_timestamp)
        except ValueError:
            continue
        if latest is None or parsed > latest:
            latest = parsed
    return latest


def _strategy_state_to_signed_quantity(state) -> int:
    if state.position_side == PositionSide.LONG:
        return int(state.internal_position_qty)
    if state.position_side == PositionSide.SHORT:
        return -int(state.internal_position_qty)
    return 0


def _load_table_rows_for_session_date(
    engine,
    table,
    timestamp_column: str,
    session_date: date,
    timezone_info,
) -> list[dict[str, Any]]:
    with engine.begin() as connection:
        rows = connection.execute(select(table)).mappings().all()
    filtered: list[dict[str, Any]] = []
    for row in rows:
        timestamp = row.get(timestamp_column)
        if not timestamp:
            continue
        try:
            row_date = datetime.fromisoformat(timestamp).astimezone(timezone_info).date()
        except ValueError:
            continue
        if row_date == session_date:
            filtered.append(dict(row))
    return filtered


def _nested_get(payload: dict[str, Any], *keys: str, default: Any = None) -> Any:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    return current


def _render_daily_summary_markdown(payload: dict[str, Any]) -> str:
    lines = [
        f"# Probationary Shadow Daily Summary: {payload['session_date']}",
        "",
        "## Runtime",
        f"- Health status: `{payload['health_status']}`",
        f"- Strategy status: `{payload['strategy_status']}`",
        f"- Reconciliation clean: `{payload['reconciliation_clean']}`",
        f"- Position at end: `{payload['position_side_end']}`",
        f"- Flat at end: `{payload['flat_at_end']}`",
        f"- Unresolved open intents: `{payload['unresolved_open_intents']}`",
        f"- Processed bars: `{payload['processed_bars_session']}` session / `{payload['processed_bars_total']}` total",
        f"- Last processed bar: `{payload['last_processed_bar_end_ts']}`",
        "",
        "## Branch Decisions",
    ]
    if payload["allowed_branch_decisions_by_source"]:
        lines.extend(
            f"- Allowed `{source}`: `{count}`"
            for source, count in payload["allowed_branch_decisions_by_source"].items()
        )
    else:
        lines.append("- No allowed branch decisions recorded")
    if payload["blocked_branch_decisions_by_source"]:
        lines.extend(
            f"- Blocked `{source}`: `{count}`"
            for source, count in payload["blocked_branch_decisions_by_source"].items()
        )
    else:
        lines.append("- No blocked branch decisions recorded")
    lines.extend(
        [
            "",
            "## Orders And Fills",
            f"- Order intents: `{payload['order_intent_count']}`",
            f"- Fills: `{payload['fill_count']}`",
            f"- Closed trades: `{payload['closed_trade_count']}`",
            f"- Realized net P/L scope: `{payload.get('realized_net_pnl_scope', 'UNKNOWN')}`",
            f"- Realized net P/L: `{payload['realized_net_pnl']}`",
            f"- Realized expectancy: `{payload['realized_expectancy']}`",
            f"- Realized max drawdown: `{payload['realized_max_drawdown']}`",
        ]
    )
    if payload["entries_and_exits_by_branch"]:
        lines.extend(
            f"- Intent reason `{reason}`: `{count}`"
            for reason, count in payload["entries_and_exits_by_branch"].items()
        )
    else:
        lines.append("- No order intents recorded for this session")
    if payload["fills_by_intent_type"]:
        lines.extend(
            f"- Fill intent `{intent_type}`: `{count}`"
            for intent_type, count in payload["fills_by_intent_type"].items()
        )
    else:
        lines.append("- No fills recorded for this session")
    if payload.get("closed_trade_digest"):
        lines.extend(["", "## Closed Trade Digest"])
        lines.extend(
            (
                f"- Trade `{row['trade_id']}` `{row['setup_family']}`: "
                f"`{row['entry_ts']}` -> `{row['exit_ts']}` net `{row['net_pnl']}` exit `{row['exit_reason']}`"
            )
            for row in payload["closed_trade_digest"]
        )
    else:
        lines.extend(["", "## Closed Trade Digest", "- No closed trades paired into the daily blotter"])
    lines.extend(
        [
            "",
            "## Blocks And Alerts",
        ]
    )
    if payload["blocked_signals_by_reason"]:
        lines.extend(
            f"- Blocked `{reason}`: `{count}`"
            for reason, count in payload["blocked_signals_by_reason"].items()
        )
    else:
        lines.append("- No blocked signals recorded")
    lines.append(f"- Alerts total: `{payload['alerts_total']}`")
    lines.append(f"- Fault alerts: `{payload['fault_alerts']}`")
    if payload["alerts_by_code"]:
        lines.extend(
            f"- Alert `{code}`: `{count}`"
            for code, count in payload["alerts_by_code"].items()
        )
    else:
        lines.append("- No alerts recorded")
    lines.extend(
        [
            "",
            "## Notes",
        ]
    )
    lines.extend(f"- {note}" for note in payload["notes"])
    lines.extend(
        [
            "",
            "## Session-End Assertions",
            f"- Flat at end: `{payload['session_end_assertions']['flat_at_end']}`",
            f"- No unresolved open intents: `{payload['session_end_assertions']['no_unresolved_open_intents']}`",
            f"- Reconciliation clean: `{payload['session_end_assertions']['reconciliation_clean']}`",
        ]
    )
    lines.append("")
    return "\n".join(lines)


def _load_captured_live_bars(
    engine,
    settings: StrategySettings,
    start_timestamp: datetime | None,
    end_timestamp: datetime | None,
) -> list[Bar]:
    statement = (
        select(bars_table)
        .where(
            bars_table.c.ticker == settings.symbol,
            bars_table.c.timeframe == settings.timeframe,
        )
        .order_by(bars_table.c.end_ts.asc())
    )
    filters = []
    if start_timestamp is not None:
        filters.append(bars_table.c.end_ts >= start_timestamp.isoformat())
    if end_timestamp is not None:
        filters.append(bars_table.c.end_ts <= end_timestamp.isoformat())
    if filters:
        statement = statement.where(and_(*filters))
    with engine.begin() as connection:
        rows = connection.execute(statement).mappings().all()
    return [
        Bar(
            bar_id=row["bar_id"],
            symbol=row["symbol"],
            timeframe=row["timeframe"],
            start_ts=datetime.fromisoformat(row["start_ts"]),
            end_ts=datetime.fromisoformat(row["end_ts"]),
            open=Decimal(str(row["open"])),
            high=Decimal(str(row["high"])),
            low=Decimal(str(row["low"])),
            close=Decimal(str(row["close"])),
            volume=int(row["volume"]),
            is_final=bool(row["is_final"]),
            session_asia=bool(row["session_asia"]),
            session_london=bool(row["session_london"]),
            session_us=bool(row["session_us"]),
            session_allowed=bool(row["session_allowed"]),
        )
        for row in rows
    ]


def _collect_signal_sources(engine, start_timestamp: datetime | None, end_timestamp: datetime | None) -> dict[str, int]:
    statement = select(signals_table.c.created_at, signals_table.c.payload_json)
    filters = []
    if start_timestamp is not None:
        filters.append(signals_table.c.created_at >= start_timestamp.isoformat())
    if end_timestamp is not None:
        filters.append(signals_table.c.created_at <= end_timestamp.isoformat())
    if filters:
        statement = statement.where(and_(*filters))
    with engine.begin() as connection:
        rows = connection.execute(statement).all()
    counts: dict[str, int] = {}
    for _, payload_json in rows:
        payload = json.loads(payload_json)
        source = payload.get("long_entry_source") or payload.get("short_entry_source")
        if source is None:
            continue
        counts[source] = counts.get(source, 0) + 1
    return counts


def _collect_order_reasons(engine, start_timestamp: datetime | None, end_timestamp: datetime | None) -> dict[str, int]:
    statement = select(order_intents_table.c.created_at, order_intents_table.c.reason_code)
    filters = []
    if start_timestamp is not None:
        filters.append(order_intents_table.c.created_at >= start_timestamp.isoformat())
    if end_timestamp is not None:
        filters.append(order_intents_table.c.created_at <= end_timestamp.isoformat())
    if filters:
        statement = statement.where(and_(*filters))
    with engine.begin() as connection:
        rows = connection.execute(statement).all()
    counts: dict[str, int] = {}
    for _, reason_code in rows:
        counts[reason_code] = counts.get(reason_code, 0) + 1
    return counts


def _parity_report_name(start_timestamp: datetime | None, end_timestamp: datetime | None) -> str:
    if start_timestamp is None and end_timestamp is None:
        return f"parity_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}"
    start_label = start_timestamp.strftime("%Y%m%dT%H%M%S") if start_timestamp is not None else "begin"
    end_label = end_timestamp.strftime("%Y%m%dT%H%M%S") if end_timestamp is not None else "end"
    return f"parity_{start_label}_{end_label}"


def _parse_time_or_none(raw_value: str | None) -> dt_time | None:
    if not raw_value:
        return None
    return dt_time.fromisoformat(raw_value)


def _time_in_closed_open_window(
    value: dt_time,
    start: dt_time | None,
    end: dt_time | None,
) -> bool:
    if start is not None and value < start:
        return False
    if end is not None and value >= end:
        return False
    return True


def _session_has_order_reason(
    repositories: RepositorySet,
    *,
    session_date: date,
    timezone_info,
    reason_code: str,
) -> bool:
    return _session_order_reason_count(
        repositories,
        session_date=session_date,
        timezone_info=timezone_info,
        reason_code=reason_code,
    ) > 0


def _order_reason_count(
    repositories: RepositorySet,
    *,
    reason_code: str,
) -> int:
    count = 0
    for row in repositories.order_intents.list_all():
        if str(row.get("reason_code") or "") == reason_code:
            count += 1
    return count


def _session_order_reason_count(
    repositories: RepositorySet,
    *,
    session_date: date,
    timezone_info,
    reason_code: str,
) -> int:
    count = 0
    for row in repositories.order_intents.list_all():
        if str(row.get("reason_code") or "") != reason_code:
            continue
        created_at = row.get("created_at")
        if not created_at:
            continue
        if datetime.fromisoformat(str(created_at)).astimezone(timezone_info).date() == session_date:
            count += 1
    return count


def _normalize_canary_force_fire_token(raw_value: str) -> str:
    token = "".join(character if character.isalnum() else "_" for character in str(raw_value).strip())
    return token.strip("_")
