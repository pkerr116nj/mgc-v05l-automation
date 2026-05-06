"""Service-ready Track B monitor.

The monitor is the long-running observation owner for Track B. It owns
process/lock/heartbeat artifacts and orchestrates existing Track B market-data
and strategy components. SHADOW mode remains no-submit. PAPER mode is explicit
and delegates only through the guarded Track B paper lifecycle.
"""

from __future__ import annotations

import json
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from dataclasses import dataclass, replace
from datetime import UTC, datetime, timedelta
from enum import Enum
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Callable, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable
from .operator_status import DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT, OperatorStatusInputs, OperatorStatusResult, create_operator_status_summary
from .track_b_asian_drift_watch_chain import (
    DEFAULT_TRACK_B_ASIAN_DRIFT_WATCH_CHAIN_OUTPUT_ROOT,
    TrackBAsianDriftWatchChainResult,
    TrackBAsianDriftWatchChainVerdict,
    run_track_b_asian_drift_watch_chain,
)
from .track_b_multi_strategy_runtime_cycle import (
    DEFAULT_TRACK_B_MULTI_STRATEGY_RUNTIME_CYCLE_OUTPUT_ROOT,
    TrackBMultiStrategyRuntimeCycleConfig,
    TrackBMultiStrategyRuntimeCycleResult,
    TrackBMultiStrategyRuntimeCycleVerdict,
    run_track_b_multi_strategy_runtime_cycle,
)
from .track_b_databento_live_runtime_feed import DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT
from .track_b_paper_trade_ledger import (
    DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON,
    DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT,
    DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON,
    DEFAULT_TRACK_B_PNL_SUMMARY_JSON,
)
from .track_b_runtime_candle_capture import (
    DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT,
    MGC_CONTINUOUS_SYMBOL,
    MGC_DATASET,
    MGC_LOCAL_SYMBOL,
    TrackBRuntimeCandleCaptureResult,
    TrackBRuntimeCandleCaptureVerdict,
    capture_track_b_runtime_mgc_1m_candles,
    write_runtime_candle_capture_provider_error,
)
from .track_b_runtime_candle_capture_cli import (
    _fetch_records,
    _load_databento_api_key,
    _read_quote_payload,
    _runtime_payload_from_records,
)
from .track_b_session_strategy_envelope_producer import (
    DEFAULT_TRACK_B_SESSION_STRATEGY_ENVELOPE_OUTPUT_ROOT,
    TrackBSessionStrategyEnvelopeProducerResult,
    TrackBSessionStrategyEnvelopeProducerVerdict,
    produce_track_b_session_strategy_envelopes,
)
from .track_b_snap_turn_envelope_producer import (
    DEFAULT_TRACK_B_SNAP_TURN_ENVELOPE_OUTPUT_ROOT,
    TrackBSnapTurnEnvelopeProducerResult,
    TrackBSnapTurnEnvelopeProducerVerdict,
    produce_track_b_snap_turn_envelopes,
)


DEFAULT_TRACK_B_SHADOW_MONITOR_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_shadow_monitor")
DEFAULT_TRACK_B_DIAGNOSTIC_OUTPUT_ROOT = Path("outputs/track_b_execution_core/diagnostics")
DEFAULT_TRACK_B_STARTUP_READINESS_DIAGNOSTIC_JSON = (
    DEFAULT_TRACK_B_DIAGNOSTIC_OUTPUT_ROOT / "latest_track_b_startup_readiness_diagnostic.json"
)
DEFAULT_TRACK_B_MONITOR_LIVENESS_DIAGNOSTIC_JSON = (
    DEFAULT_TRACK_B_DIAGNOSTIC_OUTPUT_ROOT / "latest_track_b_monitor_liveness_diagnostic.json"
)
DEFAULT_CURRENT_QUOTE_REPORT_JSON = Path(
    "outputs/track_b_execution_core/databento_candle_observer/latest_databento_candle_observer_report.json"
)
DEFAULT_BACKEND_HEALTH_JSON = Path("outputs/operator_dashboard/runtime/operator_dashboard_readiness.json")
DEFAULT_LOCKFILE = DEFAULT_TRACK_B_SHADOW_MONITOR_OUTPUT_ROOT / "track_b_shadow_monitor.lock"
DEFAULT_PIDFILE = DEFAULT_TRACK_B_SHADOW_MONITOR_OUTPUT_ROOT / "track_b_shadow_monitor.pid"


class TrackBStrategyEvaluationMode(str, Enum):
    COMPLETED_BAR_ONLY = "COMPLETED_BAR_ONLY"
    SAME_BAR_ALLOWED = "SAME_BAR_ALLOWED"
    QUOTE_TRIGGERED = "QUOTE_TRIGGERED"


class TrackBRuntimeDataSource(str, Enum):
    DATABENTO_LIVE_ARTIFACT = "DATABENTO_LIVE_ARTIFACT"
    DATABENTO_HTTP_BACKFILL = "DATABENTO_HTTP_BACKFILL"


class TrackBShadowMonitorVerdict(str, Enum):
    OK_NO_SIGNAL = "TRACK_B_SHADOW_MONITOR_OK_NO_SIGNAL"
    OK_SIGNAL_READY_NO_SUBMIT = "TRACK_B_SHADOW_MONITOR_OK_SIGNAL_READY_NO_SUBMIT"
    PAPER_PROOF_PASSED = "TRACK_B_SHADOW_MONITOR_PAPER_PROOF_PASSED"
    PAPER_PROOF_REVIEW_REQUIRED = "TRACK_B_SHADOW_MONITOR_PAPER_PROOF_REVIEW_REQUIRED"
    HEARTBEAT_NO_NEW_COMPLETED_BAR = "TRACK_B_SHADOW_MONITOR_HEARTBEAT_NO_NEW_COMPLETED_BAR"
    NOT_READY_NO_STRATEGIES_CONFIGURED = "TRACK_B_SHADOW_MONITOR_NOT_READY_NO_STRATEGIES_CONFIGURED"
    NOT_READY_UNWIRED_INSTRUMENT = "TRACK_B_SHADOW_MONITOR_NOT_READY_UNWIRED_INSTRUMENT"
    NOT_READY_STALE_RUNTIME_CONTEXT = "TRACK_B_SHADOW_MONITOR_NOT_READY_STALE_RUNTIME_CONTEXT"
    LIVE_FEED_NOT_READY = "TRACK_B_SHADOW_MONITOR_LIVE_FEED_NOT_READY"
    LIVE_FEED_WARMING_UP = "TRACK_B_SHADOW_MONITOR_LIVE_FEED_WARMING_UP"
    LIVE_FEED_STALE = "TRACK_B_SHADOW_MONITOR_LIVE_FEED_STALE"
    LIVE_FEED_DISCONNECTED = "TRACK_B_SHADOW_MONITOR_LIVE_FEED_DISCONNECTED"
    BLOCKED_PROVIDER_ERROR = "TRACK_B_SHADOW_MONITOR_BLOCKED_PROVIDER_ERROR"
    BLOCKED_PRODUCER_ERROR = "TRACK_B_SHADOW_MONITOR_BLOCKED_PRODUCER_ERROR"
    CRITICAL_UNEXPECTED_MUTATION_FLAG = "TRACK_B_SHADOW_MONITOR_CRITICAL_UNEXPECTED_MUTATION_FLAG"
    ERROR = "TRACK_B_SHADOW_MONITOR_ERROR"
    LOCK_HELD = "TRACK_B_SHADOW_MONITOR_LOCK_HELD"


@dataclass(frozen=True)
class TrackBShadowMonitorInstrumentConfig:
    instrument_family: str
    contract_key: str
    local_symbol: str
    databento_continuous_symbol: str
    dataset: str
    timeframes: tuple[str, ...] = ("1m", "5m")
    enabled_strategies: tuple[str, ...] = ()
    execution_account_id: str = "DUM882026"
    expected_account_id: str = "DUM882026"
    con_id: int | None = None
    tick_size: str = "0.1"
    exchange: str = "COMEX"
    currency: str = "USD"
    evaluation_mode: TrackBStrategyEvaluationMode = TrackBStrategyEvaluationMode.COMPLETED_BAR_ONLY
    requires_quote_freshness: bool = False
    runtime_chain_wired: bool = False


@dataclass(frozen=True)
class TrackBShadowMonitorConfig:
    mode: str = "SHADOW"
    max_cycles: int = 1
    poll_seconds: float = 15.0
    max_backoff_seconds: float = 300.0
    data_refresh_seconds: float = 60.0
    max_consecutive_failures: int | None = None
    expected_account_id: str = "DUM882026"
    account_id: str = "DUM882026"
    enable_paper_trading: bool = False
    paper_on_signal: bool = False
    max_paper_trades_per_run: int = 1
    paper_trades_attempted_count: int = 0
    pause_after_paper_trade: bool = True
    quantity: int | None = None
    manual_open_limit_price: str | None = None
    manual_close_limit_price: str | None = None
    paper_order_pricing_policy: str = "MARKETABLE_LIMIT_FROM_LIVE_CONTEXT"
    paper_order_price_offset_ticks: int = 2
    paper_exit_price_offset_ticks: int = 2
    tick_size: str = "0.1"
    con_id: int | None = 712565978
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 17086
    contract_key: str = "MGC-202606"
    local_symbol: str = MGC_LOCAL_SYMBOL
    databento_continuous_symbol: str = MGC_CONTINUOUS_SYMBOL
    dataset: str = MGC_DATASET
    timeframe: str = "1m"
    lookback_minutes: int = 60
    max_bars: int = 90
    min_bars: int = 8
    provider_timeout_seconds: float = 20.0
    provider_transport: str = "http"
    provider_stype_out: str = "instrument_id"
    prefer_raw_local_symbol_for_runtime_fetch: bool = True
    allow_fresh_runtime_artifact_fallback: bool = True
    max_latest_1m_age_seconds: int = 900
    max_completed_5m_age_seconds: int = 900
    live_execution_min_1m_bars: int = 3
    live_execution_min_completed_5m_bars: int = 1
    startup_backfill_context_enabled: bool = True
    runtime_data_source: TrackBRuntimeDataSource | str = TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT
    live_runtime_feed_output_root: Path = DEFAULT_TRACK_B_DATABENTO_LIVE_RUNTIME_FEED_OUTPUT_ROOT
    manage_live_feed: bool = True
    live_feed_warmup_timeout_seconds: float = 3600.0
    leave_live_feed_running: bool = False
    force_stop_owned_feed: bool = False
    live_feed_restart_backoff_seconds: float = 60.0
    live_feed_max_records: int = 1_000_000
    live_feed_max_seconds: float = 86_400.0
    live_feed_min_bars: int = 40
    current_quote_report_json: Path | None = DEFAULT_CURRENT_QUOTE_REPORT_JSON
    env_file: Path | None = None
    base_url: str = "https://hist.databento.com/v0"
    stype_in: str = "continuous"
    schema: str = "ohlcv-1m"
    source_id: str = "track_b_shadow_monitor"
    inbox_dir: Path = Path("examples/track_b_shadow_listener/inbox")
    output_root: Path = DEFAULT_TRACK_B_SHADOW_MONITOR_OUTPUT_ROOT
    runtime_candle_capture_output_root: Path = DEFAULT_TRACK_B_RUNTIME_CANDLE_CAPTURE_OUTPUT_ROOT
    asian_drift_output_root: Path = DEFAULT_TRACK_B_ASIAN_DRIFT_WATCH_CHAIN_OUTPUT_ROOT
    snap_turn_output_root: Path = DEFAULT_TRACK_B_SNAP_TURN_ENVELOPE_OUTPUT_ROOT
    session_strategy_output_root: Path = DEFAULT_TRACK_B_SESSION_STRATEGY_ENVELOPE_OUTPUT_ROOT
    multi_strategy_output_root: Path = DEFAULT_TRACK_B_MULTI_STRATEGY_RUNTIME_CYCLE_OUTPUT_ROOT
    operator_status_output_root: Path = DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT
    diagnostic_output_root: Path = DEFAULT_TRACK_B_DIAGNOSTIC_OUTPUT_ROOT
    backend_health_json: Path | None = DEFAULT_BACKEND_HEALTH_JSON
    lockfile: Path = DEFAULT_LOCKFILE
    pidfile: Path = DEFAULT_PIDFILE
    update_operator_status: bool = False
    stop_on_error: bool = False
    force_takeover: bool = False
    retention_cycles: int = 20
    command: tuple[str, ...] = ()
    repo_root: Path | None = None
    instruments: tuple[TrackBShadowMonitorInstrumentConfig, ...] = ()


@dataclass(frozen=True)
class TrackBShadowMonitorStages:
    runtime_candle_capture: Callable[
        [TrackBShadowMonitorConfig, TrackBShadowMonitorInstrumentConfig, int, datetime],
        TrackBRuntimeCandleCaptureResult,
    ]
    asian_drift_watch_chain: Callable[
        [TrackBShadowMonitorConfig, TrackBShadowMonitorInstrumentConfig, int, datetime, TrackBRuntimeCandleCaptureResult],
        TrackBAsianDriftWatchChainResult,
    ]
    snap_turn_envelopes: Callable[
        [TrackBShadowMonitorConfig, TrackBShadowMonitorInstrumentConfig, int, datetime, TrackBAsianDriftWatchChainResult],
        TrackBSnapTurnEnvelopeProducerResult,
    ]
    session_strategy_envelopes: Callable[
        [TrackBShadowMonitorConfig, TrackBShadowMonitorInstrumentConfig, int, datetime, TrackBAsianDriftWatchChainResult],
        TrackBSessionStrategyEnvelopeProducerResult,
    ]
    multi_strategy_runtime_cycle: Callable[
        [
            TrackBShadowMonitorConfig,
            TrackBShadowMonitorInstrumentConfig,
            int,
            datetime,
            TrackBAsianDriftWatchChainResult,
            TrackBSnapTurnEnvelopeProducerResult,
            TrackBSessionStrategyEnvelopeProducerResult,
        ],
        TrackBMultiStrategyRuntimeCycleResult,
    ]
    operator_status: Callable[[TrackBShadowMonitorConfig, Path, Path | None, datetime], OperatorStatusResult]
    sleep: Callable[[float], None]
    pid_is_alive: Callable[[int], bool]
    live_feed_starter: Callable[
        [TrackBShadowMonitorConfig, TrackBShadowMonitorInstrumentConfig, str, datetime],
        subprocess.Popen[Any],
    ] | None = None
    live_feed_terminator: Callable[[subprocess.Popen[Any]], None] | None = None


@dataclass(frozen=True)
class TrackBShadowMonitorResult:
    verdict: TrackBShadowMonitorVerdict
    report_json: Path
    report: dict[str, Any]
    cycle_reports: tuple[dict[str, Any], ...]


@dataclass(frozen=True)
class TrackBShadowMonitorLock:
    acquired: bool
    lockfile: Path
    pidfile: Path
    owner: dict[str, Any]
    stale_lock_takeover: bool = False
    force_takeover_used: bool = False
    blocker: str | None = None


@dataclass
class TrackBLiveFeedProcessState:
    instrument_family: str
    managed: bool
    owned_by_monitor: bool = False
    process: subprocess.Popen[Any] | None = None
    pid: int | None = None
    status: str = "NOT_STARTED"
    started_at: datetime | None = None
    last_start_attempt_at: datetime | None = None
    blocker: str | None = None


@dataclass(frozen=True)
class TrackBLiveFeedReadiness:
    managed: bool
    owned_by_monitor: bool
    pid: int | None
    status: str
    verdict: TrackBShadowMonitorVerdict | None
    live_feed_connected: bool | None
    subscription_status: str | None
    heartbeat_age_seconds: float | None
    transport_connected: bool | None
    raw_messages_fresh: bool | None
    completed_1m_fresh: bool | None
    completed_5m_fresh: bool | None
    execution_fresh: bool | None
    latest_1m_age_seconds: float | None
    latest_completed_5m_age_seconds: float | None
    execution_freshness_blocker: str | None
    live_execution_approved: bool
    live_confirmation_1m_count: int
    live_confirmation_completed_5m_count: int
    live_execution_required_1m_count: int
    live_execution_required_completed_5m_count: int
    strategy_ready: bool
    warmup_1m_count: int
    warmup_completed_5m_count: int
    required_1m_count: int
    required_completed_5m_count: int
    feature_context_ready: bool
    feature_context_source: str | None
    blocker: str | None
    report_path: Path | None
    heartbeat_path: Path | None
    event_path: Path | None
    completed_5m_path: Path | None


@dataclass(frozen=True)
class TrackBStartupContextGap:
    start_timestamp: str
    end_timestamp: str
    missing_expected_bars: int
    gap_duration_seconds: int
    within_required_window: bool
    classification: str
    repair_attempted: bool = False
    repair_succeeded: bool = False
    repair_source: str | None = None
    remaining_blocker: str | None = None

    def to_payload(self) -> dict[str, Any]:
        return {
            "start_timestamp": self.start_timestamp,
            "end_timestamp": self.end_timestamp,
            "missing_expected_bars": self.missing_expected_bars,
            "gap_duration_seconds": self.gap_duration_seconds,
            "within_required_window": self.within_required_window,
            "classification": self.classification,
            "repair_attempted": self.repair_attempted,
            "repair_succeeded": self.repair_succeeded,
            "repair_source": self.repair_source,
            "remaining_blocker": self.remaining_blocker,
        }


def default_instruments(config: TrackBShadowMonitorConfig | None = None) -> tuple[TrackBShadowMonitorInstrumentConfig, ...]:
    base = config or TrackBShadowMonitorConfig()
    mgc_strategies = (
        "ASIAN_DRIFT_V1",
        "ASIA_EARLY_PAUSE_RESUME_SHORT_V1",
        "ASIA_EARLY_NORMAL_BREAKOUT_RETEST_HOLD_LONG_V1",
        "FIRST_BULL_SNAP_TURN_V1",
        "FIRST_BEAR_SNAP_TURN_V1",
        "LONDON_LATE_PAUSE_RESUME_SHORT_V1",
        "ASIA_LATE_FLAT_PULLBACK_PAUSE_RESUME_LONG_V1",
        "US_DERIVATIVE_BEAR_TURN_V1",
        "US_LATE_PAUSE_RESUME_LONG_V1",
    )
    mnq_strategies = ("MNQ_US_DERIVATIVE_BEAR_TURN_V1", "MNQ_FIRST_BEAR_SNAP_TURN_V1")
    return (
        TrackBShadowMonitorInstrumentConfig(
            instrument_family="GC",
            contract_key="GC-NOT_CONFIGURED",
            local_symbol="GC",
            databento_continuous_symbol="GC.v.0",
            dataset="GLBX.MDP3",
        ),
        TrackBShadowMonitorInstrumentConfig(
            instrument_family="MGC",
            contract_key=base.contract_key,
            local_symbol=base.local_symbol,
            databento_continuous_symbol=base.databento_continuous_symbol,
            dataset=base.dataset,
            enabled_strategies=mgc_strategies,
            execution_account_id=base.account_id,
            expected_account_id=base.expected_account_id,
            con_id=base.con_id,
            tick_size=base.tick_size,
            exchange="COMEX",
            currency="USD",
            runtime_chain_wired=True,
        ),
        TrackBShadowMonitorInstrumentConfig(
            instrument_family="ES",
            contract_key="ES-NOT_CONFIGURED",
            local_symbol="ES",
            databento_continuous_symbol="ES.v.0",
            dataset="GLBX.MDP3",
        ),
        TrackBShadowMonitorInstrumentConfig(
            instrument_family="MES",
            contract_key="MES-NOT_CONFIGURED",
            local_symbol="MES",
            databento_continuous_symbol="MES.v.0",
            dataset="GLBX.MDP3",
        ),
        TrackBShadowMonitorInstrumentConfig(
            instrument_family="NQ",
            contract_key="NQ-NOT_CONFIGURED",
            local_symbol="NQ",
            databento_continuous_symbol="NQ.v.0",
            dataset="GLBX.MDP3",
        ),
        TrackBShadowMonitorInstrumentConfig(
            instrument_family="MNQ",
            contract_key="MNQ-202606",
            local_symbol="MNQM6",
            databento_continuous_symbol="MNQ.v.0",
            dataset="GLBX.MDP3",
            enabled_strategies=mnq_strategies,
            execution_account_id=base.account_id,
            expected_account_id=base.expected_account_id,
            con_id=770561201,
            tick_size="0.25",
            exchange="CME",
            currency="USD",
            runtime_chain_wired=True,
        ),
    )


def default_stages() -> TrackBShadowMonitorStages:
    return TrackBShadowMonitorStages(
        runtime_candle_capture=_run_runtime_candle_capture,
        asian_drift_watch_chain=_run_asian_drift_watch_chain,
        snap_turn_envelopes=_run_snap_turn_envelopes,
        session_strategy_envelopes=_run_session_strategy_envelopes,
        multi_strategy_runtime_cycle=_run_multi_strategy_runtime_cycle,
        operator_status=_run_operator_status,
        sleep=time.sleep,
        pid_is_alive=_pid_is_alive,
    )


def run_track_b_shadow_monitor(
    *,
    config: TrackBShadowMonitorConfig,
    stages: TrackBShadowMonitorStages | None = None,
    monitor_id: str | None = None,
    now_func: Callable[[], datetime] | None = None,
) -> TrackBShadowMonitorResult:
    _validate_monitor_mode_config(config)
    if config.max_cycles <= 0:
        raise ValueError("max_cycles must be positive.")
    if config.poll_seconds < 0:
        raise ValueError("poll_seconds must be non-negative.")
    if config.max_backoff_seconds < 0:
        raise ValueError("max_backoff_seconds must be non-negative.")
    if config.data_refresh_seconds < 0:
        raise ValueError("data_refresh_seconds must be non-negative.")

    actual_stages = stages or default_stages()
    clock = now_func or (lambda: datetime.now(UTC))
    actual_monitor_id = monitor_id or f"track_b_shadow_monitor_{uuid.uuid4().hex}"
    instruments = config.instruments or default_instruments(config)
    lock = acquire_monitor_lock(
        config=config,
        monitor_id=actual_monitor_id,
        started_at=clock(),
        pid_is_alive=actual_stages.pid_is_alive,
    )
    if not lock.acquired:
        report = _lock_held_report(config=config, monitor_id=actual_monitor_id, lock=lock, now=clock())
        _write_monitor_report(Path(str(report["report_json_path"])), report)
        return TrackBShadowMonitorResult(
            verdict=TrackBShadowMonitorVerdict.LOCK_HELD,
            report_json=Path(str(report["report_json_path"])),
            report=report,
            cycle_reports=(report,),
        )

    cycle_reports: list[dict[str, Any]] = []
    final_verdict = TrackBShadowMonitorVerdict.OK_NO_SIGNAL
    final_report_json: Path | None = None
    consecutive_failures = 0
    last_evaluated_completed_5m: dict[str, str] = {}
    last_runtime_fetch_attempt_at: dict[str, datetime] = {}
    live_feed_processes: dict[str, TrackBLiveFeedProcessState] = {}
    paper_trades_attempted_count = 0
    shutdown_reason: str | None = None
    try:
        for cycle_index in range(1, config.max_cycles + 1):
            started_at = clock()
            require_aware_datetime(started_at, "cycle_started_at")
            cycle_id = f"{actual_monitor_id}_cycle_{cycle_index:04d}_{uuid.uuid4().hex}"
            _write_heartbeat(
                config=config,
                monitor_id=actual_monitor_id,
                cycle_id=cycle_id,
                cycle_index=cycle_index,
                generated_at=started_at,
                monitor_running=True,
                instruments=instruments,
                last_verdict=None,
                last_report_path=None,
                lock=lock,
            )
            report = _run_one_cycle(
                config=config,
                stages=actual_stages,
                instruments=instruments,
                monitor_id=actual_monitor_id,
                cycle_id=cycle_id,
                cycle_index=cycle_index,
                started_at=started_at,
                now_func=clock,
                last_evaluated_completed_5m=last_evaluated_completed_5m,
                last_runtime_fetch_attempt_at=last_runtime_fetch_attempt_at,
                live_feed_processes=live_feed_processes,
                paper_trades_attempted_count=paper_trades_attempted_count,
                lock=lock,
            )
            cycle_reports.append(report)
            paper_trades_attempted_count = int(report.get("paper_trades_attempted_count") or paper_trades_attempted_count)
            final_verdict = TrackBShadowMonitorVerdict(str(report["monitor_verdict"]))
            final_report_json = Path(str(report["report_json_path"]))
            failure = _failure_counts_for_backoff(report)
            consecutive_failures = consecutive_failures + 1 if failure else 0
            if config.max_consecutive_failures is not None and consecutive_failures >= config.max_consecutive_failures:
                report["max_consecutive_failures_reached"] = True
                report["primary_blocker"] = report.get("primary_blocker") or (
                    f"max_consecutive_failures reached: {consecutive_failures}."
                )
                _write_monitor_report(final_report_json, report)
                final_verdict = TrackBShadowMonitorVerdict(str(report["monitor_verdict"]))
                break
            should_stop = _must_stop(report, config)
            sleep_seconds = 0.0
            next_wake_at: datetime | None = None
            if not should_stop and cycle_index < config.max_cycles:
                sleep_seconds = _sleep_seconds(config=config, consecutive_failures=consecutive_failures)
                if sleep_seconds > 0:
                    next_wake_at = _parse_time(str(report["completed_at"])) + timedelta(seconds=sleep_seconds)
            _write_heartbeat(
                config=config,
                monitor_id=actual_monitor_id,
                cycle_id=cycle_id,
                cycle_index=cycle_index,
                generated_at=_parse_time(str(report["completed_at"])),
                monitor_running=cycle_index < config.max_cycles and not should_stop,
                instruments=instruments,
                last_verdict=final_verdict.value,
                last_report_path=final_report_json,
                lock=lock,
                current_blocker=report.get("primary_blocker"),
                sleep_seconds=sleep_seconds if sleep_seconds > 0 else None,
                next_wake_at=next_wake_at,
            )
            _write_monitor_liveness_diagnostic(
                config=config,
                now=_parse_time(str(report["completed_at"])),
                lock=lock,
                instruments=instruments,
                live_feed_processes=live_feed_processes,
                last_report=report,
                sleep_seconds=sleep_seconds if sleep_seconds > 0 else None,
                next_wake_at=next_wake_at,
            )
            if should_stop:
                break
            if sleep_seconds > 0:
                actual_stages.sleep(sleep_seconds)
    except KeyboardInterrupt:
        shutdown_reason = "KeyboardInterrupt"
    finally:
        _shutdown_owned_live_feeds(
            config=config,
            stages=actual_stages,
            live_feed_processes=live_feed_processes,
        )
        _write_final_heartbeat(
            config=config,
            monitor_id=actual_monitor_id,
            generated_at=clock(),
            last_report_path=final_report_json,
            last_verdict=final_verdict.value,
            shutdown_reason=shutdown_reason,
            lock=lock,
        )
        if config.update_operator_status and final_report_json is not None and cycle_reports:
            _refresh_final_operator_status(
                config=config,
                stages=actual_stages,
                final_report=cycle_reports[-1],
                final_report_json=final_report_json,
                now=clock(),
            )
        release_monitor_lock(lock)

    if not cycle_reports:
        raise RuntimeError("Track B shadow monitor did not produce any cycle reports.")
    if final_report_json is None:
        final_report_json = Path(str(cycle_reports[-1]["report_json_path"]))
    return TrackBShadowMonitorResult(
        verdict=final_verdict,
        report_json=final_report_json,
        report=cycle_reports[-1],
        cycle_reports=tuple(cycle_reports),
    )


def acquire_monitor_lock(
    *,
    config: TrackBShadowMonitorConfig,
    monitor_id: str,
    started_at: datetime,
    pid_is_alive: Callable[[int], bool] | None = None,
) -> TrackBShadowMonitorLock:
    actual_pid_is_alive = pid_is_alive or _pid_is_alive
    lockfile = Path(config.lockfile)
    pidfile = Path(config.pidfile)
    lockfile.parent.mkdir(parents=True, exist_ok=True)
    pidfile.parent.mkdir(parents=True, exist_ok=True)
    existing = _read_json_optional(lockfile)
    stale_takeover = False
    force_takeover = False
    if existing:
        existing_pid = _int_or_none(existing.get("pid"))
        live_owner = existing_pid is not None and actual_pid_is_alive(existing_pid)
        if live_owner and not config.force_takeover:
            return TrackBShadowMonitorLock(
                acquired=False,
                lockfile=lockfile,
                pidfile=pidfile,
                owner=dict(existing),
                blocker=f"Live Track B shadow monitor already owns lock with pid={existing_pid}.",
            )
        stale_takeover = not live_owner
        force_takeover = bool(live_owner and config.force_takeover)
    owner = {
        "schema_version": "track_b_shadow_monitor_lock_v1",
        "pid": os.getpid(),
        "host": socket.gethostname(),
        "monitor_id": monitor_id,
        "started_at": started_at.astimezone(UTC).isoformat(),
        "command": list(config.command or tuple(sys.argv)),
        "repo_root": str((config.repo_root or Path.cwd()).resolve()),
        "lockfile": str(lockfile),
        "pidfile": str(pidfile),
        "stale_lock_takeover": stale_takeover,
        "force_takeover_used": force_takeover,
    }
    lockfile.write_text(json.dumps(to_jsonable(owner), indent=2, sort_keys=True), encoding="utf-8")
    pidfile.write_text(str(os.getpid()), encoding="utf-8")
    return TrackBShadowMonitorLock(
        acquired=True,
        lockfile=lockfile,
        pidfile=pidfile,
        owner=owner,
        stale_lock_takeover=stale_takeover,
        force_takeover_used=force_takeover,
    )


def release_monitor_lock(lock: TrackBShadowMonitorLock) -> None:
    if not lock.acquired:
        return
    current = _read_json_optional(lock.lockfile)
    if current and current.get("pid") == lock.owner.get("pid") and current.get("monitor_id") == lock.owner.get("monitor_id"):
        try:
            lock.lockfile.unlink()
        except FileNotFoundError:
            pass
    try:
        if lock.pidfile.read_text(encoding="utf-8").strip() == str(lock.owner.get("pid")):
            lock.pidfile.unlink()
    except FileNotFoundError:
        pass


def _run_one_cycle(
    *,
    config: TrackBShadowMonitorConfig,
    stages: TrackBShadowMonitorStages,
    instruments: Sequence[TrackBShadowMonitorInstrumentConfig],
    monitor_id: str,
    cycle_id: str,
    cycle_index: int,
    started_at: datetime,
    now_func: Callable[[], datetime],
    last_evaluated_completed_5m: dict[str, str],
    last_runtime_fetch_attempt_at: dict[str, datetime],
    live_feed_processes: dict[str, TrackBLiveFeedProcessState],
    lock: TrackBShadowMonitorLock,
    paper_trades_attempted_count: int,
) -> dict[str, Any]:
    report_json = Path(config.output_root) / cycle_id / "track_b_shadow_monitor_report.json"
    instrument_reports: list[dict[str, Any]] = []
    runtime_cycle_report_json: Path | None = None
    cycle_config = replace(config, paper_trades_attempted_count=paper_trades_attempted_count)
    try:
        for instrument in instruments:
            instrument_report, maybe_runtime_cycle_report_json = _run_instrument_cycle(
                config=cycle_config,
                stages=stages,
                instrument=instrument,
                cycle_index=cycle_index,
                started_at=started_at,
                now_func=now_func,
                last_evaluated_completed_5m=last_evaluated_completed_5m,
                last_runtime_fetch_attempt_at=last_runtime_fetch_attempt_at,
                live_feed_processes=live_feed_processes,
                paper_trades_attempted_count=paper_trades_attempted_count,
            )
            instrument_reports.append(instrument_report)
            if maybe_runtime_cycle_report_json is not None:
                runtime_cycle_report_json = maybe_runtime_cycle_report_json
    except Exception as exc:  # noqa: BLE001 - monitor errors must become artifacts.
        instrument_reports.append(
            _instrument_report_base(
                instrument_family="GLOBAL",
                verdict=TrackBShadowMonitorVerdict.ERROR,
                primary_blocker=f"Track B shadow monitor error: {exc}",
                required_next_action="Review monitor diagnostics before restarting.",
            )
        )

    completed_at = now_func()
    verdict = _cycle_verdict(instrument_reports)
    critical = _global_critical_blocker(instrument_reports, config=config)
    if critical:
        verdict = TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG
    report = _report_for_cycle(
        config=config,
        monitor_id=monitor_id,
        cycle_id=cycle_id,
        cycle_index=cycle_index,
        started_at=started_at,
        completed_at=completed_at,
        report_json=report_json,
        verdict=verdict,
        instrument_reports=instrument_reports,
        primary_blocker=critical or _primary_blocker(instrument_reports),
        required_next_action=_required_next_action(verdict),
        lock=lock,
        paper_trades_attempted_count=paper_trades_attempted_count + _paper_trade_attempt_count_delta(instrument_reports),
    )
    return _finalize_cycle(
        config,
        stages,
        report_json,
        report,
        runtime_cycle_report_json=runtime_cycle_report_json,
        now=completed_at,
    )


def _run_instrument_cycle(
    *,
    config: TrackBShadowMonitorConfig,
    stages: TrackBShadowMonitorStages,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    started_at: datetime,
    now_func: Callable[[], datetime],
    last_evaluated_completed_5m: dict[str, str],
    last_runtime_fetch_attempt_at: dict[str, datetime],
    live_feed_processes: dict[str, TrackBLiveFeedProcessState] | None = None,
    paper_trades_attempted_count: int = 0,
) -> tuple[dict[str, Any], Path | None]:
    if not instrument.enabled_strategies:
        return (
            _instrument_report_base(
                instrument=instrument,
                verdict=TrackBShadowMonitorVerdict.NOT_READY_NO_STRATEGIES_CONFIGURED,
                primary_blocker=f"{instrument.instrument_family} has no enabled Track B strategies configured.",
                required_next_action="Register Track B-safe strategy adapters and envelope producers before evaluation.",
            ),
            None,
        )
    if not instrument.runtime_chain_wired:
        return (
            _instrument_report_base(
                instrument=instrument,
                verdict=TrackBShadowMonitorVerdict.NOT_READY_UNWIRED_INSTRUMENT,
                primary_blocker=f"{instrument.instrument_family} runtime candle/envelope chain is not wired yet.",
                required_next_action="Wire Track B runtime candle and envelope producers for this instrument.",
            ),
            None,
        )

    live_feed_readiness: TrackBLiveFeedReadiness | None = None
    if (
        _runtime_data_source(config) == TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT
        and stages.runtime_candle_capture is _run_runtime_candle_capture
    ):
        live_feed_readiness = _ensure_live_feed_for_instrument(
            config=config,
            stages=stages,
            instrument=instrument,
            now=started_at,
            live_feed_processes=live_feed_processes if live_feed_processes is not None else {},
        )
        if not live_feed_readiness.strategy_ready and not (
            config.startup_backfill_context_enabled and live_feed_readiness.live_execution_approved
        ):
            _write_live_readiness_startup_diagnostic(
                config=config,
                instrument=instrument,
                readiness=live_feed_readiness,
                now=started_at,
            )
            report = _instrument_report_base(
                instrument=instrument,
                verdict=live_feed_readiness.verdict or TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY,
                primary_blocker=live_feed_readiness.blocker or "Databento Live feed is not strategy-ready.",
                required_next_action=(
                    "Keep the managed Databento Live feed running until live_execution_approved=true, "
                    "then seed feature context from bounded backfill if needed."
                ),
                live_feed=live_feed_readiness,
            )
            report["runtime_data_source"] = TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value
            report["runtime_decision_source"] = TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value
            return (
                report,
                None,
            )

    runtime = _runtime_for_refresh_cadence(
        config=config,
        stages=stages,
        instrument=instrument,
        cycle_index=cycle_index,
        started_at=started_at,
        last_runtime_fetch_attempt_at=last_runtime_fetch_attempt_at,
    )
    if runtime.report.get("data_written") is not True:
        verdict = (
            TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY
            if runtime.report.get("runtime_data_source") == TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value
            else TrackBShadowMonitorVerdict.BLOCKED_PROVIDER_ERROR
        )
        return (
            _instrument_report_from_stages(
                instrument=instrument,
                verdict=verdict,
                runtime=runtime,
                live_feed=live_feed_readiness,
                primary_blocker=str(runtime.report.get("primary_blocker") or "Runtime candle capture did not write data."),
                required_next_action=str(runtime.report.get("required_next_action") or "Repair runtime candle provider before evaluation."),
            ),
            None,
        )
    if runtime.report.get("runtime_data_source") == TrackBRuntimeDataSource.DATABENTO_HTTP_BACKFILL.value:
        return (
            _instrument_report_from_stages(
                instrument=instrument,
                verdict=TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT,
                runtime=runtime,
                live_feed=live_feed_readiness,
                primary_blocker="Databento HTTP/historical data is backfill/recovery context only and is not a live runtime source.",
                required_next_action="Start the Databento Live runtime feed writer before Track B SHADOW strategy evaluation.",
            ),
            None,
        )
    if runtime.report.get("fresh_for_execution") is not True:
        return (
            _instrument_report_from_stages(
                instrument=instrument,
                verdict=TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT,
                runtime=runtime,
                live_feed=live_feed_readiness,
                primary_blocker=str(
                    runtime.report.get("execution_freshness_blocker")
                    or runtime.report.get("primary_blocker")
                    or "Runtime candle context is not fresh_for_execution=true."
                ),
                required_next_action="Refresh runtime candles until fresh_for_execution=true before strategy evaluation.",
            ),
            None,
        )
    if (
        runtime.report.get("runtime_data_source") == TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value
        and runtime.report.get("paper_evaluation_allowed") is False
    ):
        return (
            _instrument_report_from_stages(
                instrument=instrument,
                verdict=TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT,
                runtime=runtime,
                live_feed=live_feed_readiness,
                primary_blocker=str(
                    runtime.report.get("startup_readiness_classification")
                    or runtime.report.get("execution_freshness_blocker")
                    or "Startup feature context or Live execution approval is not ready."
                ),
                required_next_action="Seed bounded feature context and require latest decision bar from fresh Databento Live artifacts.",
            ),
            None,
        )

    completed_5m = str(runtime.report.get("latest_completed_5m_timestamp") or "")
    if (
        instrument.evaluation_mode == TrackBStrategyEvaluationMode.COMPLETED_BAR_ONLY
        and completed_5m
        and last_evaluated_completed_5m.get(instrument.instrument_family) == completed_5m
    ):
        return (
            _instrument_report_from_stages(
                instrument=instrument,
                verdict=TrackBShadowMonitorVerdict.HEARTBEAT_NO_NEW_COMPLETED_BAR,
                runtime=runtime,
                live_feed=live_feed_readiness,
                primary_blocker=None,
                required_next_action="No new completed 5m bar; heartbeat only for completed-bar strategies.",
            ),
            None,
        )

    asian = stages.asian_drift_watch_chain(config, instrument, cycle_index, started_at, runtime)
    if asian.verdict in {TrackBAsianDriftWatchChainVerdict.STALE_RUNTIME_CONTEXT, TrackBAsianDriftWatchChainVerdict.BLOCKED_SCHEMA_ERROR}:
        return (
            _instrument_report_from_stages(
                instrument=instrument,
                verdict=TrackBShadowMonitorVerdict.BLOCKED_PRODUCER_ERROR,
                runtime=runtime,
                live_feed=live_feed_readiness,
                asian=asian,
                primary_blocker=str(asian.report.get("primary_blocker") or "Asian Drift watch chain blocked."),
                required_next_action=str(asian.report.get("required_next_action") or "Resolve Asian Drift producer blocker."),
            ),
            None,
        )

    snap = stages.snap_turn_envelopes(config, instrument, cycle_index, started_at, asian)
    if snap.verdict != TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES:
        return (
            _instrument_report_from_stages(
                instrument=instrument,
                verdict=TrackBShadowMonitorVerdict.BLOCKED_PRODUCER_ERROR,
                runtime=runtime,
                live_feed=live_feed_readiness,
                asian=asian,
                snap=snap,
                primary_blocker=str(snap.report.get("primary_blocker") or "Snap-turn envelope producer blocked."),
                required_next_action=str(snap.report.get("required_next_action") or "Resolve snap-turn envelope producer blocker."),
            ),
            None,
        )

    session = stages.session_strategy_envelopes(config, instrument, cycle_index, started_at, asian)
    if session.verdict != TrackBSessionStrategyEnvelopeProducerVerdict.WROTE_ENVELOPES:
        return (
            _instrument_report_from_stages(
                instrument=instrument,
                verdict=TrackBShadowMonitorVerdict.BLOCKED_PRODUCER_ERROR,
                runtime=runtime,
                live_feed=live_feed_readiness,
                asian=asian,
                snap=snap,
                session=session,
                primary_blocker=str(session.report.get("primary_blocker") or "Session strategy envelope producer blocked."),
                required_next_action=str(session.report.get("required_next_action") or "Resolve session strategy envelope producer blocker."),
            ),
            None,
        )

    runtime_cycle = stages.multi_strategy_runtime_cycle(config, instrument, cycle_index, started_at, asian, snap, session)
    if completed_5m:
        last_evaluated_completed_5m[instrument.instrument_family] = completed_5m
    critical = _critical_mutation_flag(runtime_cycle.report, config=config)
    verdict = _verdict_for_runtime_cycle(runtime_cycle.report, critical)
    return (
        _instrument_report_from_stages(
            instrument=instrument,
            verdict=verdict,
            runtime=runtime,
            live_feed=live_feed_readiness,
            asian=asian,
            snap=snap,
            session=session,
            runtime_cycle=runtime_cycle,
            primary_blocker=critical or runtime_cycle.report.get("primary_blocker"),
            required_next_action=(
                "Stop SHADOW monitor and inspect Track B safety fields before continuing."
                if critical
                else str(runtime_cycle.report.get("required_next_action") or "Continue Track B SHADOW monitoring.")
            ),
        ),
        runtime_cycle.report_json,
    )


def _run_runtime_candle_capture(
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    now: datetime,
) -> TrackBRuntimeCandleCaptureResult:
    if _runtime_data_source(config) == TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT:
        return _run_live_runtime_artifact_capture(config, instrument, cycle_index, now)
    return _run_http_backfill_runtime_candle_capture(config, instrument, cycle_index, now)


def _ensure_live_feed_for_instrument(
    *,
    config: TrackBShadowMonitorConfig,
    stages: TrackBShadowMonitorStages,
    instrument: TrackBShadowMonitorInstrumentConfig,
    now: datetime,
    live_feed_processes: dict[str, TrackBLiveFeedProcessState],
) -> TrackBLiveFeedReadiness:
    state = live_feed_processes.get(instrument.instrument_family)
    if state is not None and state.owned_by_monitor and state.process is not None and state.process.poll() is not None:
        state.status = "LIVE_FEED_DISCONNECTED"
        state.blocker = f"Managed Databento Live feed exited with code {state.process.returncode}."

    readiness = _read_live_feed_readiness(config=config, instrument=instrument, state=state, now=now)
    if readiness.strategy_ready:
        return readiness
    if not config.manage_live_feed:
        return readiness

    already_alive = state is not None and state.process is not None and state.process.poll() is None
    if already_alive:
        return readiness
    last_attempt = state.last_start_attempt_at if state is not None else None
    if last_attempt is not None:
        elapsed = max(0.0, (now.astimezone(UTC) - last_attempt.astimezone(UTC)).total_seconds())
        if elapsed < config.live_feed_restart_backoff_seconds:
            return readiness

    try:
        process = _start_live_feed_process(
            config=config,
            stages=stages,
            instrument=instrument,
            now=now,
        )
    except Exception as exc:  # noqa: BLE001 - supervision errors must become readiness artifacts.
        new_state = TrackBLiveFeedProcessState(
            instrument_family=instrument.instrument_family,
            managed=True,
            owned_by_monitor=False,
            pid=None,
            status="LIVE_FEED_START_FAILED",
            started_at=None,
            last_start_attempt_at=now,
            blocker=f"Managed Databento Live feed start failed: {exc}",
        )
        live_feed_processes[instrument.instrument_family] = new_state
        _write_live_feed_process_status(config=config, instrument=instrument, state=new_state, generated_at=now)
        return _read_live_feed_readiness(config=config, instrument=instrument, state=new_state, now=now)

    new_state = TrackBLiveFeedProcessState(
        instrument_family=instrument.instrument_family,
        managed=True,
        owned_by_monitor=True,
        process=process,
        pid=process.pid,
        status="LIVE_FEED_STARTED",
        started_at=now,
        last_start_attempt_at=now,
    )
    live_feed_processes[instrument.instrument_family] = new_state
    _write_live_feed_process_status(config=config, instrument=instrument, state=new_state, generated_at=now)
    return _read_live_feed_readiness(config=config, instrument=instrument, state=new_state, now=now)


def _read_live_feed_readiness(
    *,
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    state: TrackBLiveFeedProcessState | None,
    now: datetime,
) -> TrackBLiveFeedReadiness:
    root = Path(config.live_runtime_feed_output_root)
    event_path = _latest_live_1m_path(root, instrument.instrument_family)
    completed_path = _latest_live_completed_5m_path(root, instrument.instrument_family)
    report_path = _latest_live_report_path(root, instrument.instrument_family)
    heartbeat_path = _latest_live_heartbeat_path(root, instrument.instrument_family)
    report = _read_json_optional(report_path)
    heartbeat = _read_json_optional(heartbeat_path)
    event = _read_json_optional(event_path)
    completed = _read_json_optional(completed_path)

    heartbeat_age = _age_seconds_from_payload(heartbeat, now)
    live_connected = _bool_or_none((heartbeat or {}).get("live_feed_connected"))
    if live_connected is None and report is not None:
        live_connected = _bool_or_none(report.get("live_feed_connected"))
    subscription_status = str((heartbeat or {}).get("subscription_status") or (report or {}).get("subscription_status") or "")
    if not subscription_status:
        subscription_status = None

    warmup_1m_count = _bars_available(event)
    warmup_completed_5m_count = _bars_available(completed)
    required_1m_count = max(int(config.live_feed_min_bars), int(config.min_bars))
    required_completed_5m_count = max(8, int(config.min_bars))
    live_execution_required_1m_count = max(1, int(config.live_execution_min_1m_bars))
    live_execution_required_completed_5m_count = max(0, int(config.live_execution_min_completed_5m_bars))
    heartbeat_fresh = heartbeat_age is not None and heartbeat_age <= max(float(config.max_latest_1m_age_seconds), config.poll_seconds * 3)
    owned_warmup_elapsed = None if state is None or state.started_at is None else (
        now.astimezone(UTC) - state.started_at.astimezone(UTC)
    ).total_seconds()
    owned_feed_is_warming = bool(
        state is not None
        and state.owned_by_monitor
        and state.process is not None
        and state.process.poll() is None
        and owned_warmup_elapsed is not None
        and owned_warmup_elapsed <= config.live_feed_warmup_timeout_seconds
    )
    artifact_matches = _live_feed_artifact_matches(report=report, event=event, instrument=instrument)
    latest_1m_age = _first_float(
        (event or {}).get("latest_1m_age_seconds"),
        (report or {}).get("latest_1m_age_seconds"),
        (heartbeat or {}).get("latest_1m_age_seconds"),
    )
    latest_completed_5m_age = _first_float(
        (event or {}).get("latest_completed_5m_age_seconds"),
        (report or {}).get("latest_completed_5m_age_seconds"),
        (heartbeat or {}).get("latest_completed_5m_age_seconds"),
    )
    completed_1m_fresh = (
        latest_1m_age is not None and latest_1m_age <= float(config.max_latest_1m_age_seconds)
    )
    completed_5m_fresh = (
        latest_completed_5m_age is not None
        and latest_completed_5m_age <= float(config.max_completed_5m_age_seconds)
    )
    latest_raw_message = _first_text(
        (heartbeat or {}).get("latest_record_ts_recv"),
        (heartbeat or {}).get("latest_record_ts_event"),
        (report or {}).get("latest_record_ts_recv"),
        (report or {}).get("latest_record_ts_event"),
        (event or {}).get("last_candle_timestamp"),
        (event or {}).get("candle_timestamp"),
    )
    raw_message_age = None
    if latest_raw_message:
        try:
            raw_message_age = max(0.0, (now.astimezone(UTC) - _parse_time(latest_raw_message)).total_seconds())
        except ValueError:
            raw_message_age = None
    raw_messages_fresh = None if raw_message_age is None else raw_message_age <= float(config.max_latest_1m_age_seconds)
    reported_execution_fresh = _bool_or_none((heartbeat or report or event or {}).get("fresh_for_execution"))
    execution_fresh = (
        completed_1m_fresh and completed_5m_fresh
        if latest_1m_age is not None and latest_completed_5m_age is not None
        else reported_execution_fresh
    )
    execution_freshness_blocker = None
    if execution_fresh is not True:
        stale_parts: list[str] = []
        if latest_1m_age is None:
            stale_parts.append("latest 1m candle age is unavailable")
        elif not completed_1m_fresh:
            stale_parts.append(
                f"latest 1m candle age {round(latest_1m_age, 3)}s exceeds max {config.max_latest_1m_age_seconds}s"
            )
        if latest_completed_5m_age is None:
            stale_parts.append("latest completed 5m candle age is unavailable")
        elif not completed_5m_fresh:
            stale_parts.append(
                "latest completed 5m candle age "
                f"{round(latest_completed_5m_age, 3)}s exceeds max {config.max_completed_5m_age_seconds}s"
            )
        execution_freshness_blocker = "; ".join(stale_parts) or "Databento Live execution freshness is unavailable."
    live_execution_approved = (
        artifact_matches is None
        and heartbeat_fresh
        and live_connected is True
        and execution_fresh is True
        and warmup_1m_count >= live_execution_required_1m_count
        and warmup_completed_5m_count >= live_execution_required_completed_5m_count
    )
    feature_context_ready = (
        warmup_1m_count >= required_1m_count
        and warmup_completed_5m_count >= required_completed_5m_count
    )
    strategy_ready = (
        live_execution_approved
        and feature_context_ready
    )
    live_execution_blocker = None
    if live_execution_approved is not True:
        if artifact_matches:
            live_execution_blocker = artifact_matches
        elif not heartbeat_fresh:
            live_execution_blocker = "Databento Live heartbeat is not fresh enough for execution approval."
        elif live_connected is not True:
            live_execution_blocker = "Databento Live transport is not connected."
        elif execution_fresh is not True:
            live_execution_blocker = execution_freshness_blocker or "Databento Live execution freshness is unavailable."
        elif warmup_1m_count < live_execution_required_1m_count:
            live_execution_blocker = (
                "Databento Live confirmation window is incomplete: "
                f"1m={warmup_1m_count}/{live_execution_required_1m_count}."
            )
        elif warmup_completed_5m_count < live_execution_required_completed_5m_count:
            live_execution_blocker = (
                "Databento Live confirmation window is incomplete: "
                f"completed_5m={warmup_completed_5m_count}/{live_execution_required_completed_5m_count}."
            )
    live_only_strategy_ready = (
        live_execution_approved
        and warmup_1m_count >= required_1m_count
        and warmup_completed_5m_count >= required_completed_5m_count
    )

    managed = bool(config.manage_live_feed)
    owned = bool(state and state.owned_by_monitor)
    pid = state.pid if state is not None else None
    blocker: str | None = None
    verdict: TrackBShadowMonitorVerdict | None = None
    status = state.status if state is not None else "LIVE_FEED_EXTERNAL_OR_MISSING"
    if not event_path.exists() or not heartbeat_path.exists():
        status = "LIVE_FEED_WARMING_UP" if state and state.owned_by_monitor else "LIVE_FEED_NOT_READY"
        verdict = TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP if state and state.owned_by_monitor else TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY
        blocker = "Databento Live feed hot artifacts are not available yet."
    elif artifact_matches:
        status = "LIVE_FEED_NOT_READY"
        verdict = TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY
        blocker = artifact_matches
    elif heartbeat_age is None:
        status = "LIVE_FEED_NOT_READY"
        verdict = TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY
        blocker = "Databento Live feed heartbeat has no generated_at timestamp."
    elif not heartbeat_fresh:
        if owned_feed_is_warming:
            status = "LIVE_FEED_WARMING_UP"
            verdict = TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP
            blocker = (
                "Managed Databento Live feed was started and is waiting for a fresh heartbeat; "
                f"previous_heartbeat_age_seconds={round(heartbeat_age, 3)}."
            )
        else:
            status = "LIVE_FEED_STALE"
            verdict = TrackBShadowMonitorVerdict.LIVE_FEED_STALE
            blocker = f"Databento Live feed heartbeat is stale: age_seconds={round(heartbeat_age, 3)}."
    elif live_connected is not True:
        status = "LIVE_FEED_DISCONNECTED"
        verdict = TrackBShadowMonitorVerdict.LIVE_FEED_DISCONNECTED
        blocker = "Databento Live feed is not connected."
    elif live_execution_approved is not True:
        status = "LIVE_FEED_STALE" if execution_fresh is not True else "LIVE_FEED_WARMING_UP"
        verdict = TrackBShadowMonitorVerdict.LIVE_FEED_STALE if execution_fresh is not True else TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP
        blocker = live_execution_blocker or "Databento Live feed is not approved for execution yet."
    elif live_only_strategy_ready is not True:
        warmup_elapsed = owned_warmup_elapsed
        if warmup_elapsed is not None and warmup_elapsed > config.live_feed_warmup_timeout_seconds:
            status = "LIVE_FEED_WARMUP_TIMEOUT"
            verdict = TrackBShadowMonitorVerdict.LIVE_FEED_STALE
            blocker = (
                "Databento Live feed warmup exceeded timeout: "
                f"elapsed_seconds={round(warmup_elapsed, 3)}, "
                f"1m={warmup_1m_count}/{required_1m_count}, "
                f"completed_5m={warmup_completed_5m_count}/{required_completed_5m_count}."
            )
        else:
            status = "LIVE_FEED_WARMING_UP"
            verdict = TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP
            blocker = (
                "Databento Live feed is execution-fresh but feature context is warming up: "
                f"1m={warmup_1m_count}/{required_1m_count}, "
                f"completed_5m={warmup_completed_5m_count}/{required_completed_5m_count}."
            )
    else:
        status = "LIVE_FEED_STRATEGY_READY"

    if state is not None and state.blocker and blocker is None:
        blocker = state.blocker
    return TrackBLiveFeedReadiness(
        managed=managed,
        owned_by_monitor=owned,
        pid=pid,
        status=status,
        verdict=verdict,
        live_feed_connected=live_connected,
        subscription_status=subscription_status,
        heartbeat_age_seconds=None if heartbeat_age is None else round(heartbeat_age, 3),
        transport_connected=live_connected,
        raw_messages_fresh=raw_messages_fresh,
        completed_1m_fresh=completed_1m_fresh,
        completed_5m_fresh=completed_5m_fresh,
        execution_fresh=execution_fresh,
        latest_1m_age_seconds=None if latest_1m_age is None else round(latest_1m_age, 3),
        latest_completed_5m_age_seconds=None if latest_completed_5m_age is None else round(latest_completed_5m_age, 3),
        execution_freshness_blocker=execution_freshness_blocker,
        live_execution_approved=live_execution_approved,
        live_confirmation_1m_count=warmup_1m_count,
        live_confirmation_completed_5m_count=warmup_completed_5m_count,
        live_execution_required_1m_count=live_execution_required_1m_count,
        live_execution_required_completed_5m_count=live_execution_required_completed_5m_count,
        strategy_ready=strategy_ready,
        warmup_1m_count=warmup_1m_count,
        warmup_completed_5m_count=warmup_completed_5m_count,
        required_1m_count=required_1m_count,
        required_completed_5m_count=required_completed_5m_count,
        feature_context_ready=feature_context_ready,
        feature_context_source="DATABENTO_LIVE_ARTIFACT" if feature_context_ready else None,
        blocker=blocker,
        report_path=report_path if report_path.exists() else None,
        heartbeat_path=heartbeat_path if heartbeat_path.exists() else None,
        event_path=event_path if event_path.exists() else None,
        completed_5m_path=completed_path if completed_path.exists() else None,
    )


def _artifact_symbol(instrument_family: str | None) -> str:
    return str(instrument_family or "MGC").strip().lower() or "mgc"


def _latest_live_1m_path(root: Path, instrument_family: str | None) -> Path:
    symbol = _artifact_symbol(instrument_family)
    legacy = Path(root) / "latest_live_mgc_1m_candles.json"
    if symbol == "mgc" and legacy.exists():
        return legacy
    return Path(root) / f"latest_live_{symbol}_1m_candles.json"


def _latest_live_completed_5m_path(root: Path, instrument_family: str | None) -> Path:
    symbol = _artifact_symbol(instrument_family)
    legacy = Path(root) / "latest_live_mgc_completed_5m_candles.json"
    if symbol == "mgc" and legacy.exists():
        return legacy
    return Path(root) / f"latest_live_{symbol}_completed_5m_candles.json"


def _latest_live_report_path(root: Path, instrument_family: str | None) -> Path:
    symbol = _artifact_symbol(instrument_family)
    legacy = Path(root) / "latest_databento_live_runtime_feed_report.json"
    if symbol == "mgc" and legacy.exists():
        return legacy
    return Path(root) / f"latest_databento_live_runtime_feed_{symbol}_report.json"


def _latest_live_heartbeat_path(root: Path, instrument_family: str | None) -> Path:
    symbol = _artifact_symbol(instrument_family)
    legacy = Path(root) / "latest_databento_live_runtime_feed_heartbeat.json"
    if symbol == "mgc" and legacy.exists():
        return legacy
    return Path(root) / f"latest_databento_live_runtime_feed_{symbol}_heartbeat.json"


def _latest_live_quote_status_path(root: Path, instrument_family: str | None) -> Path:
    symbol = _artifact_symbol(instrument_family)
    legacy = Path(root) / "latest_live_quote_status_report.json"
    if symbol == "mgc" and legacy.exists():
        return legacy
    return Path(root) / f"latest_live_{symbol}_quote_status_report.json"


def _latest_runtime_1m_path(root: Path, instrument: TrackBShadowMonitorInstrumentConfig) -> Path:
    symbol = _artifact_symbol(instrument.instrument_family)
    legacy = Path(root) / "latest_runtime_mgc_1m_candles.json"
    if symbol == "mgc" and legacy.exists():
        return legacy
    return Path(root) / f"latest_runtime_{symbol}_1m_candles.json"


def _latest_runtime_report_path(root: Path, instrument: TrackBShadowMonitorInstrumentConfig) -> Path:
    symbol = _artifact_symbol(instrument.instrument_family)
    legacy = Path(root) / "latest_runtime_candle_capture_report.json"
    if symbol == "mgc" and legacy.exists():
        return legacy
    return Path(root) / f"latest_runtime_candle_capture_{symbol}_report.json"


def _start_live_feed_process(
    *,
    config: TrackBShadowMonitorConfig,
    stages: TrackBShadowMonitorStages,
    instrument: TrackBShadowMonitorInstrumentConfig,
    now: datetime,
) -> subprocess.Popen[Any]:
    if stages.live_feed_starter is not None:
        return stages.live_feed_starter(config, instrument, config.source_id, now)
    root = Path(config.live_runtime_feed_output_root)
    root.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        "-m",
        "mgc_v05l.execution_core.track_b_databento_live_runtime_feed_cli",
        "--expected-account-id",
        instrument.expected_account_id,
        "--account-id",
        instrument.execution_account_id,
        "--contract-key",
        instrument.contract_key,
        "--instrument-family",
        instrument.instrument_family,
        "--local-symbol",
        instrument.local_symbol,
        "--databento-continuous-symbol",
        instrument.databento_continuous_symbol,
        "--dataset",
        instrument.dataset,
        "--schema",
        config.schema,
        "--stype-in",
        config.stype_in,
        "--max-records",
        str(config.live_feed_max_records),
        "--max-bars",
        str(config.max_bars),
        "--min-bars",
        str(config.min_bars),
        "--max-seconds",
        str(config.live_feed_max_seconds),
        "--max-latest-1m-age-seconds",
        str(config.max_latest_1m_age_seconds),
        "--max-completed-5m-age-seconds",
        str(config.max_completed_5m_age_seconds),
        "--source-id",
        f"{config.source_id}_managed_live_feed",
        "--output-root",
        str(root),
    ]
    if config.provider_stype_out:
        cmd.extend(["--stype-out", config.provider_stype_out])
    if config.env_file is not None:
        cmd.extend(["--env-file", str(config.env_file)])
    log_symbol = _artifact_symbol(instrument.instrument_family)
    stdout = open(root / f"track_b_live_feed_{log_symbol}_stdout.log", "ab", buffering=0)  # noqa: SIM115 - Popen needs file handles.
    stderr = open(root / f"track_b_live_feed_{log_symbol}_stderr.log", "ab", buffering=0)  # noqa: SIM115
    try:
        process = subprocess.Popen(cmd, stdout=stdout, stderr=stderr)
    finally:
        stdout.close()
        stderr.close()
    return process


def _shutdown_owned_live_feeds(
    *,
    config: TrackBShadowMonitorConfig,
    stages: TrackBShadowMonitorStages,
    live_feed_processes: Mapping[str, TrackBLiveFeedProcessState],
) -> None:
    if config.leave_live_feed_running:
        return
    for state in live_feed_processes.values():
        if not state.owned_by_monitor or state.process is None:
            continue
        _terminate_live_feed_process(stages=stages, process=state.process)


def _terminate_live_feed_process(*, stages: TrackBShadowMonitorStages, process: subprocess.Popen[Any]) -> None:
    if stages.live_feed_terminator is not None:
        stages.live_feed_terminator(process)
        return
    if process.poll() is not None:
        return
    process.terminate()
    try:
        process.wait(timeout=10)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=5)


def _write_live_feed_process_status(
    *,
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    state: TrackBLiveFeedProcessState,
    generated_at: datetime,
) -> None:
    payload = {
        "schema_version": "track_b_managed_live_feed_process_status_v1",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "instrument_family": instrument.instrument_family,
        "contract_key": instrument.contract_key,
        "local_symbol": instrument.local_symbol,
        "databento_continuous_symbol": instrument.databento_continuous_symbol,
        "dataset": instrument.dataset,
        "managed": state.managed,
        "owned_by_monitor": state.owned_by_monitor,
        "pid": state.pid,
        "status": state.status,
        "started_at": None if state.started_at is None else state.started_at.astimezone(UTC).isoformat(),
        "last_start_attempt_at": None
        if state.last_start_attempt_at is None
        else state.last_start_attempt_at.astimezone(UTC).isoformat(),
        "blocker": state.blocker,
        "submit_allowed": False,
        "submit_attempted": False,
        "paper_proof_invoked": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
    }
    _write_json_file(Path(config.live_runtime_feed_output_root) / "latest_live_feed_process_status.json", payload)


def _runtime_data_source(config: TrackBShadowMonitorConfig) -> TrackBRuntimeDataSource:
    if isinstance(config.runtime_data_source, TrackBRuntimeDataSource):
        return config.runtime_data_source
    return TrackBRuntimeDataSource(str(config.runtime_data_source).strip().upper())


def _validate_monitor_mode_config(config: TrackBShadowMonitorConfig) -> None:
    mode = str(config.mode).upper()
    if mode not in {"SHADOW", "PAPER"}:
        raise ValueError("Track B monitor mode must be SHADOW or PAPER.")
    if mode == "SHADOW":
        if config.enable_paper_trading or config.paper_on_signal:
            raise ValueError("SHADOW mode cannot enable paper trading or paper-on-signal.")
        return
    if not config.enable_paper_trading:
        raise ValueError("PAPER mode requires --enable-paper-trading.")
    if not config.paper_on_signal:
        raise ValueError("PAPER mode requires --paper-on-signal.")
    if _runtime_data_source(config) != TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT:
        raise ValueError("PAPER mode requires runtime_decision_source=DATABENTO_LIVE_ARTIFACT.")
    if config.max_paper_trades_per_run <= 0:
        raise ValueError("PAPER mode requires --max-paper-trades-per-run greater than zero.")
    if config.quantity is None or config.quantity <= 0:
        raise ValueError("PAPER mode requires explicit positive --quantity.")
    policy = str(config.paper_order_pricing_policy or "").strip().upper()
    if policy == "MANUAL_LIMIT_PRICES":
        if config.manual_open_limit_price is None:
            raise ValueError("PAPER mode with MANUAL_LIMIT_PRICES requires --manual-open-limit-price.")
        if config.manual_close_limit_price is None:
            raise ValueError("PAPER mode with MANUAL_LIMIT_PRICES requires --manual-close-limit-price.")
    elif config.manual_open_limit_price is not None or config.manual_close_limit_price is not None:
        raise ValueError("Manual PAPER prices are only accepted with --paper-order-pricing-policy MANUAL_LIMIT_PRICES.")
    elif policy not in {"MARKETABLE_LIMIT_FROM_LIVE_CONTEXT", "LIMIT_AT_LAST", "LIMIT_AT_SIGNAL_PRICE"}:
        raise ValueError("PAPER mode received an unsupported --paper-order-pricing-policy.")
    if config.expected_account_id != "DUM882026" or config.account_id != "DUM882026":
        raise ValueError("PAPER mode currently requires account and expected account DUM882026.")


def _monitor_mode(config: TrackBShadowMonitorConfig) -> str:
    return str(config.mode).upper()


def _monitor_paper_submit_requested(config: TrackBShadowMonitorConfig) -> bool:
    return bool(
        _monitor_mode(config) == "PAPER"
        and config.enable_paper_trading
        and config.paper_on_signal
        and config.paper_trades_attempted_count < config.max_paper_trades_per_run
    )


def _monitor_paper_side(config: TrackBShadowMonitorConfig) -> str:
    return "AUTO" if _monitor_mode(config) == "PAPER" else "BUY"


def _run_live_runtime_artifact_capture(
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    now: datetime,
) -> TrackBRuntimeCandleCaptureResult:
    live_event_json = _latest_live_1m_path(Path(config.live_runtime_feed_output_root), instrument.instrument_family)
    live_report_json = _latest_live_report_path(Path(config.live_runtime_feed_output_root), instrument.instrument_family)
    live_report = _read_json_optional(live_report_json)
    if not live_event_json.exists():
        result = write_runtime_candle_capture_provider_error(
            primary_blocker="Databento Live runtime feed artifact is missing; HTTP historical/backfill is not a live runtime source.",
            required_next_action="Start track_b_databento_live_runtime_feed_cli and wait for fresh Live artifacts.",
            verdict=TrackBRuntimeCandleCaptureVerdict.PROVIDER_ERROR,
            source_id=f"{config.source_id}_live_runtime_artifact_cycle_{cycle_index}",
            account_id=instrument.execution_account_id,
            contract_key=instrument.contract_key,
            local_symbol=instrument.local_symbol,
            databento_continuous_symbol=instrument.databento_continuous_symbol,
            dataset=instrument.dataset,
            timeframe=config.timeframe,
            provider_transport="databento_live",
            provider_request_symbol=instrument.databento_continuous_symbol,
            provider_request_stype_in=config.stype_in,
            provider_request_stype_out=config.provider_stype_out,
            max_bars=config.max_bars,
            min_bars=config.min_bars,
            max_latest_1m_age_seconds=config.max_latest_1m_age_seconds,
            max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
            output_root=config.runtime_candle_capture_output_root,
            capture_id=f"track_b_shadow_monitor_live_runtime_artifact_cycle_{cycle_index}_{uuid.uuid4().hex}",
            now=now,
        )
        _annotate_live_runtime_result(result, live_report_json=live_report_json, live_event_json=live_event_json, live_report=live_report)
        return result
    try:
        payload = _read_json_required(live_event_json)
    except Exception as exc:  # noqa: BLE001 - unreadable Live artifact is provider-not-ready evidence.
        result = write_runtime_candle_capture_provider_error(
            primary_blocker=f"Databento Live runtime feed artifact could not be read: {exc}",
            required_next_action="Repair the Live runtime feed writer artifact before strategy evaluation.",
            verdict=TrackBRuntimeCandleCaptureVerdict.PROVIDER_ERROR,
            source_id=f"{config.source_id}_live_runtime_artifact_cycle_{cycle_index}",
            account_id=instrument.execution_account_id,
            contract_key=instrument.contract_key,
            local_symbol=instrument.local_symbol,
            databento_continuous_symbol=instrument.databento_continuous_symbol,
            dataset=instrument.dataset,
            timeframe=config.timeframe,
            provider_transport="databento_live",
            provider_request_symbol=instrument.databento_continuous_symbol,
            provider_request_stype_in=config.stype_in,
            provider_request_stype_out=config.provider_stype_out,
            max_bars=config.max_bars,
            min_bars=config.min_bars,
            max_latest_1m_age_seconds=config.max_latest_1m_age_seconds,
            max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
            output_root=config.runtime_candle_capture_output_root,
            capture_id=f"track_b_shadow_monitor_live_runtime_artifact_cycle_{cycle_index}_{uuid.uuid4().hex}",
            now=now,
        )
        _annotate_live_runtime_result(result, live_report_json=live_report_json, live_event_json=live_event_json, live_report=live_report)
        return result
    startup_context_payload, startup_readiness = _startup_context_payload(
        config=config,
        instrument=instrument,
        cycle_index=cycle_index,
        now=now,
        live_payload=payload,
        live_report=live_report,
        live_event_json=live_event_json,
        live_report_json=live_report_json,
    )
    result = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=startup_context_payload,
        source_payload_path=live_event_json,
        expected_account_id=instrument.expected_account_id,
        account_id=instrument.execution_account_id,
        contract_key=instrument.contract_key,
        local_symbol=instrument.local_symbol,
        databento_continuous_symbol=instrument.databento_continuous_symbol,
        dataset=instrument.dataset,
        timeframe=config.timeframe,
        max_bars=config.max_bars,
        min_bars=config.min_bars,
        candle_source_mode="DATABENTO_LIVE_RUNTIME_FEED_MONITOR_READ",
        provider_transport="databento_live",
        provider_request_symbol=instrument.databento_continuous_symbol,
        provider_request_stype_in=config.stype_in,
        provider_request_stype_out=config.provider_stype_out,
        max_latest_1m_age_seconds=config.max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        source_id=f"{config.source_id}_live_runtime_artifact_cycle_{cycle_index}",
        output_root=config.runtime_candle_capture_output_root,
        capture_id=f"track_b_shadow_monitor_live_runtime_artifact_cycle_{cycle_index}_{uuid.uuid4().hex}",
        now=now,
    )
    result.report.update(
        {
            "feature_context_ready": startup_readiness.get("context_ready"),
            "feature_context_source": startup_readiness.get("context_source"),
            "startup_readiness_classification": startup_readiness.get("classification"),
            "startup_readiness_diagnostic_path": startup_readiness.get("diagnostic_path"),
            "backfill_gap_detected": startup_readiness.get("backfill_gap_detected"),
            "backfill_gap_filled": startup_readiness.get("backfill_gap_filled"),
            "backfill_source": startup_readiness.get("backfill_source"),
            "latest_decision_bar_source": startup_readiness.get("latest_decision_bar_source"),
            "live_execution_approved": startup_readiness.get("live_execution_approved"),
            "paper_evaluation_allowed": startup_readiness.get("paper_evaluation_allowed"),
        }
    )
    _annotate_live_runtime_result(result, live_report_json=live_report_json, live_event_json=live_event_json, live_report=live_report)
    return result


def _startup_context_payload(
    *,
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    now: datetime,
    live_payload: Mapping[str, Any],
    live_report: Mapping[str, Any] | None,
    live_event_json: Path,
    live_report_json: Path,
) -> tuple[dict[str, Any], dict[str, Any]]:
    live_report_payload = live_report or {}
    required_5m = max(8, int(config.min_bars))
    required_1m = max(int(config.live_feed_min_bars), int(config.min_bars), required_5m * 5)
    live_candles = _tag_context_candles(_payload_candles(live_payload), "DATABENTO_LIVE_ARTIFACT")
    merged = list(live_candles)
    backfill_gap_detected = len(merged) < required_1m or _completed_5m_count_from_1m(merged) < required_5m
    backfill_gap_filled = False
    backfill_source = None
    backfill_report_path = None
    gap_repair_attempted = False
    gap_repair_source = None
    initial_gap_details: list[TrackBStartupContextGap] = []
    recovery_event_path = _latest_runtime_1m_path(Path(config.runtime_candle_capture_output_root), instrument)
    recovery_payload = _read_json_optional(recovery_event_path)
    if backfill_gap_detected and recovery_payload:
        recovery_candles = _tag_context_candles(
            _payload_candles(recovery_payload),
            _context_source_tag(recovery_payload, default="RECOVERY_CONTEXT"),
        )
        merged = _merge_context_candles(recovery_candles, merged)
        backfill_gap_filled = len(merged) >= required_1m and _completed_5m_count_from_1m(merged) >= required_5m
        if backfill_gap_filled:
            backfill_source = "RECOVERY_CONTEXT"
            backfill_report_path = str(recovery_event_path)
    merged = _merge_context_candles(merged)
    initial_gap_details = _classify_context_gaps(merged, required_1m=required_1m, required_5m=required_5m)
    blocking_initial_gaps = _blocking_context_gaps(initial_gap_details)
    if blocking_initial_gaps:
        backfill_gap_detected = True
    if (
        backfill_gap_detected
        and (not backfill_gap_filled or blocking_initial_gaps)
        and config.startup_backfill_context_enabled
    ):
        gap_repair_attempted = bool(blocking_initial_gaps)
        backfill = _run_http_backfill_runtime_candle_capture(config, instrument, cycle_index, now)
        backfill_report_path = str(backfill.report_json)
        if backfill.report.get("data_written") is True and backfill.runtime_candles_event:
            http_candles = _tag_context_candles(
                _payload_candles(backfill.runtime_candles_event),
                "DATABENTO_HTTP_BACKFILL",
            )
            merged = _merge_context_candles(http_candles, live_candles)
            backfill_source = "DATABENTO_HTTP_BACKFILL"
            gap_repair_source = "DATABENTO_HTTP_BACKFILL" if gap_repair_attempted else None
            backfill_gap_filled = len(merged) >= required_1m and _completed_5m_count_from_1m(merged) >= required_5m
    merged = _merge_context_candles(merged)
    merged = merged[-max(int(config.max_bars), required_1m):]
    gap_details = _classify_context_gaps(
        merged,
        required_1m=required_1m,
        required_5m=required_5m,
        repair_attempted=gap_repair_attempted,
        repair_source=gap_repair_source,
    )
    if gap_repair_attempted and initial_gap_details:
        gap_details = _mark_repaired_gaps(initial_gap_details, gap_details, repair_source=gap_repair_source) + [
            gap
            for gap in gap_details
            if (gap.start_timestamp, gap.end_timestamp)
            not in {(item.start_timestamp, item.end_timestamp) for item in initial_gap_details}
        ]
    blocking_gaps = _blocking_context_gaps(gap_details)
    validation_candles = _required_context_window(merged, required_1m=required_1m, required_5m=required_5m)
    completed_5m_count = _completed_5m_count_from_1m(validation_candles)
    context_gap_count = len(blocking_gaps)
    primary_gap = blocking_gaps[0] if blocking_gaps else (gap_details[0] if gap_details else None)
    latest_decision_bar_source = merged[-1].get("source_tag") if merged else None
    context_ready = len(validation_candles) >= required_1m and completed_5m_count >= required_5m and context_gap_count == 0
    live_completed_1m_fresh = _first_bool(
        live_payload.get("completed_1m_fresh"),
        live_report_payload.get("completed_1m_fresh"),
    )
    live_completed_5m_fresh = _first_bool(
        live_payload.get("completed_5m_fresh"),
        live_report_payload.get("completed_5m_fresh"),
    )
    if live_completed_1m_fresh is None:
        latest_1m_age = _first_float(
            live_payload.get("latest_1m_age_seconds"),
            live_payload.get("latest_1m_candle_age_seconds"),
            live_report_payload.get("latest_1m_age_seconds"),
            live_report_payload.get("latest_1m_candle_age_seconds"),
        )
        live_completed_1m_fresh = (
            latest_1m_age <= float(config.max_latest_1m_age_seconds)
            if latest_1m_age is not None
            else _first_bool(live_payload.get("fresh_for_execution"), live_report_payload.get("fresh_for_execution"))
        )
    if live_completed_5m_fresh is None:
        latest_5m_age = _first_float(
            live_payload.get("latest_completed_5m_age_seconds"),
            live_payload.get("latest_completed_5m_candle_age_seconds"),
            live_report_payload.get("latest_completed_5m_age_seconds"),
            live_report_payload.get("latest_completed_5m_candle_age_seconds"),
        )
        live_completed_5m_fresh = (
            latest_5m_age <= float(config.max_completed_5m_age_seconds)
            if latest_5m_age is not None
            else _first_bool(live_payload.get("fresh_for_execution"), live_report_payload.get("fresh_for_execution"))
        )
    live_execution_approved = (
        latest_decision_bar_source == "DATABENTO_LIVE_ARTIFACT"
        and len(live_candles) >= max(1, int(config.live_execution_min_1m_bars))
        and _completed_5m_count_from_1m(live_candles) >= max(0, int(config.live_execution_min_completed_5m_bars))
        and live_completed_1m_fresh is True
        and live_completed_5m_fresh is True
    )
    paper_evaluation_allowed = context_ready and live_execution_approved
    if not context_ready:
        classification = "FEATURE_CONTEXT_NOT_READY"
        context_source = backfill_source
    elif latest_decision_bar_source != "DATABENTO_LIVE_ARTIFACT":
        classification = "LATEST_DECISION_BAR_NOT_LIVE"
        context_source = backfill_source or "NON_EXECUTION_CONTEXT"
        paper_evaluation_allowed = False
    elif not live_execution_approved:
        classification = "LIVE_EXECUTION_NOT_APPROVED"
        context_source = backfill_source or "DATABENTO_LIVE_ARTIFACT"
    elif context_ready and not backfill_gap_detected:
        classification = "READY_WITH_LIVE_ONLY_CONTEXT"
        context_source = "DATABENTO_LIVE_ARTIFACT"
    elif context_ready and backfill_gap_filled:
        classification = "READY_WITH_BACKFILL_SEEDED_CONTEXT"
        context_source = "MIXED_BACKFILL_SEEDED_CONTEXT"
    elif backfill_gap_detected and config.startup_backfill_context_enabled and not backfill_gap_filled:
        classification = "BACKFILL_FAILED" if backfill_report_path else "BACKFILL_REQUIRED_IN_PROGRESS"
        context_source = backfill_source
    else:
        classification = "DIAGNOSTIC_INCONCLUSIVE"
        context_source = backfill_source
    diagnostic = {
        "instrument_family": instrument.instrument_family,
        "strategy_ids": list(instrument.enabled_strategies),
        "required_1m_context_bars": required_1m,
        "available_1m_context_bars": len(merged),
        "usable_1m_context_bars": len(validation_candles),
        "required_5m_context_bars": required_5m,
        "available_5m_context_bars": completed_5m_count,
        "total_5m_context_bars": _completed_5m_count_from_1m(merged),
        "live_1m_bars": len(live_candles),
        "live_completed_5m_bars": _completed_5m_count_from_1m(live_candles),
        "required_live_1m_bars": max(1, int(config.live_execution_min_1m_bars)),
        "required_live_completed_5m_bars": max(0, int(config.live_execution_min_completed_5m_bars)),
        "backfill_gap_detected": backfill_gap_detected,
        "backfill_gap_filled": backfill_gap_filled,
        "backfill_source": backfill_source,
        "context_source": context_source,
        "backfill_report_path": backfill_report_path,
        "gap_count": len(gap_details),
        "gaps": [gap.to_payload() for gap in gap_details],
        "gap_start": primary_gap.start_timestamp if primary_gap else None,
        "gap_end": primary_gap.end_timestamp if primary_gap else None,
        "missing_expected_bars": primary_gap.missing_expected_bars if primary_gap else None,
        "gap_classification": primary_gap.classification if primary_gap else None,
        "gap_repair_attempted": primary_gap.repair_attempted if primary_gap else False,
        "gap_repair_succeeded": primary_gap.repair_succeeded if primary_gap else False,
        "gap_repair_source": primary_gap.repair_source if primary_gap else None,
        "remaining_blocker": primary_gap.remaining_blocker if primary_gap else None,
        "context_continuity_verdict": "CONTEXT_CONTINUITY_READY" if not blocking_gaps else "CONTEXT_CONTINUITY_BLOCKED",
        "context_gap_count": context_gap_count,
        "context_ready": context_ready,
        "live_transport_connected": True,
        "live_raw_messages_fresh": None,
        "live_completed_1m_fresh": live_completed_1m_fresh,
        "live_completed_5m_fresh": live_completed_5m_fresh,
        "latest_decision_bar_source": latest_decision_bar_source,
        "live_execution_approved": live_execution_approved,
        "strategy_ready": paper_evaluation_allowed,
        "paper_evaluation_allowed": paper_evaluation_allowed,
        "feature_context_ready_after_repair": context_ready,
        "paper_evaluation_allowed_after_repair": paper_evaluation_allowed,
        "blocked_reason": _startup_blocked_reason(
            context_ready=context_ready,
            live_execution_approved=live_execution_approved,
            latest_decision_bar_source=latest_decision_bar_source,
            context_gap_count=context_gap_count,
            available_1m=len(merged),
            required_1m=required_1m,
            available_5m=completed_5m_count,
            required_5m=required_5m,
        ),
        "classification": classification,
        "live_event_path": str(live_event_json),
        "live_report_path": str(live_report_json) if live_report_json.exists() else None,
    }
    diagnostic_path = _write_startup_readiness_diagnostic(config=config, instrument_payload=diagnostic, now=now)
    diagnostic["diagnostic_path"] = str(diagnostic_path)
    output_payload = dict(live_payload)
    output_payload.update(
        {
            "candle_source_mode": (
                "DATABENTO_LIVE_WITH_BACKFILL_SEEDED_CONTEXT"
                if context_source == "MIXED_BACKFILL_SEEDED_CONTEXT"
                else "DATABENTO_LIVE_RUNTIME_FEED"
            ),
            "candles": validation_candles if context_ready else merged,
            "candle_history": validation_candles if context_ready else merged,
            "bars_available": len(validation_candles) if context_ready else len(merged),
            "feature_context_ready": context_ready,
            "feature_context_source": context_source,
            "startup_readiness_classification": classification,
            "latest_decision_bar_source": latest_decision_bar_source,
            "live_execution_approved": live_execution_approved,
            "paper_evaluation_allowed": paper_evaluation_allowed,
            "startup_context_gap_count": context_gap_count,
            "startup_context_gaps": [gap.to_payload() for gap in gap_details],
            "context_continuity_verdict": "CONTEXT_CONTINUITY_READY" if not blocking_gaps else "CONTEXT_CONTINUITY_BLOCKED",
            "source_lineage": {
                "live_event_path": str(live_event_json),
                "live_report_path": str(live_report_json) if live_report_json.exists() else None,
                "backfill_report_path": backfill_report_path,
                "context_source": context_source,
            },
        }
    )
    return output_payload, diagnostic


def _payload_candles(payload: Mapping[str, Any]) -> list[dict[str, Any]]:
    for key in ("candles", "candle_history", "runtime_candles", "bars", "ohlcv"):
        raw = payload.get(key)
        if isinstance(raw, list):
            return [dict(item) for item in raw if isinstance(item, Mapping)]
    return []


def _tag_context_candles(candles: Sequence[Mapping[str, Any]], source_tag: str) -> list[dict[str, Any]]:
    tagged: list[dict[str, Any]] = []
    for candle in candles:
        item = dict(candle)
        item["source_tag"] = item.get("source_tag") or source_tag
        item["source_role"] = "EXECUTION_LIVE" if item["source_tag"] == "DATABENTO_LIVE_ARTIFACT" else "NON_EXECUTION_CONTEXT"
        tagged.append(item)
    return tagged


def _context_source_tag(payload: Mapping[str, Any], *, default: str) -> str:
    mode = str(payload.get("candle_source_mode") or "").upper()
    if "HTTP" in mode or "HISTORICAL" in mode or "BACKFILL" in mode:
        return "DATABENTO_HTTP_BACKFILL"
    if "LIVE" in mode:
        return "RECOVERY_CONTEXT"
    return default


def _merge_context_candles(*groups: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    by_ts: dict[str, dict[str, Any]] = {}
    for group in groups:
        for candle in group:
            ts = _first_text(candle.get("candle_timestamp"), candle.get("timestamp"), candle.get("observed_at"))
            if not ts:
                continue
            try:
                normalized_ts = _normalize_context_minute(_parse_time(ts)).isoformat()
            except ValueError:
                continue
            item = dict(candle)
            item["candle_timestamp"] = normalized_ts
            existing = by_ts.get(normalized_ts)
            if existing is None or item.get("source_tag") == "DATABENTO_LIVE_ARTIFACT":
                by_ts[normalized_ts] = item
    return [by_ts[key] for key in sorted(by_ts, key=lambda value: _parse_time(value))]


def _normalize_context_minute(value: datetime) -> datetime:
    value = value.astimezone(UTC)
    if value.second >= 30:
        value = value + timedelta(minutes=1)
    return value.replace(second=0, microsecond=0)


def _context_gap_count(candles: Sequence[Mapping[str, Any]]) -> int:
    if len(candles) < 2:
        return 0
    gaps = 0
    previous = _parse_time(str(candles[0]["candle_timestamp"]))
    for candle in candles[1:]:
        current = _parse_time(str(candle["candle_timestamp"]))
        if current - previous > timedelta(minutes=1):
            gaps += 1
        previous = current
    return gaps


def _required_context_window(
    candles: Sequence[Mapping[str, Any]],
    *,
    required_1m: int,
    required_5m: int = 0,
) -> list[dict[str, Any]]:
    if required_1m <= 0:
        return [dict(item) for item in candles]
    base_start = max(0, len(candles) - required_1m)
    if required_5m <= 0:
        return [dict(item) for item in candles[base_start:]]
    for start in range(base_start, -1, -1):
        candidate = [dict(item) for item in candles[start:]]
        if len(candidate) >= required_1m and _completed_5m_count_from_1m(candidate) >= required_5m:
            return candidate
    return [dict(item) for item in candles[base_start:]]


def _classify_context_gaps(
    candles: Sequence[Mapping[str, Any]],
    *,
    required_1m: int,
    required_5m: int = 0,
    repair_attempted: bool = False,
    repair_source: str | None = None,
) -> list[TrackBStartupContextGap]:
    if len(candles) < 2:
        return []
    required_window = _required_context_window(candles, required_1m=required_1m, required_5m=required_5m)
    required_times = [
        _parse_time(str(item.get("candle_timestamp")))
        for item in required_window
        if item.get("candle_timestamp") is not None
    ]
    required_start = min(required_times) if required_times else None
    required_end = max(required_times) if required_times else None
    gaps: list[TrackBStartupContextGap] = []
    previous_candle = candles[0]
    previous = _parse_time(str(previous_candle["candle_timestamp"]))
    for candle in candles[1:]:
        current = _parse_time(str(candle["candle_timestamp"]))
        delta = current - previous
        if delta > timedelta(minutes=1):
            missing_expected_bars = max(0, int(delta.total_seconds() // 60) - 1)
            start = previous + timedelta(minutes=1)
            end = current - timedelta(minutes=1)
            within_required = (
                required_start is not None
                and required_end is not None
                and start <= required_end
                and end >= required_start
            )
            classification = _classify_context_gap(
                previous=previous,
                current=current,
                previous_source=_first_text(previous_candle.get("source_tag")),
                current_source=_first_text(candle.get("source_tag")),
                within_required_window=within_required,
            )
            remaining_blocker = None
            if within_required and classification not in {"SESSION_BOUNDARY_GAP_ALLOWED", "GAP_OUTSIDE_REQUIRED_WINDOW"}:
                remaining_blocker = "Required startup feature context still has an unrepaired 1m gap."
                if repair_attempted:
                    classification = "UNREPAIRABLE_REQUIRED_CONTEXT_GAP"
            gaps.append(
                TrackBStartupContextGap(
                    start_timestamp=start.isoformat(),
                    end_timestamp=end.isoformat(),
                    missing_expected_bars=missing_expected_bars,
                    gap_duration_seconds=int(delta.total_seconds()),
                    within_required_window=within_required,
                    classification=classification,
                    repair_attempted=repair_attempted and within_required,
                    repair_succeeded=False,
                    repair_source=repair_source if repair_attempted and within_required else None,
                    remaining_blocker=remaining_blocker,
                )
            )
        previous = current
        previous_candle = candle
    return gaps


def _classify_context_gap(
    *,
    previous: datetime,
    current: datetime,
    previous_source: str | None,
    current_source: str | None,
    within_required_window: bool,
) -> str:
    if not within_required_window:
        return "GAP_OUTSIDE_REQUIRED_WINDOW"
    if _is_allowed_session_boundary_gap(previous, current):
        return "SESSION_BOUNDARY_GAP_ALLOWED"
    if previous_source != current_source and "DATABENTO_LIVE_ARTIFACT" in {previous_source, current_source}:
        return "LIVE_BACKFILL_STITCH_GAP"
    return "REPAIRABLE_BACKFILL_GAP"


def _is_allowed_session_boundary_gap(previous: datetime, current: datetime) -> bool:
    previous_utc = previous.astimezone(UTC)
    current_utc = current.astimezone(UTC)
    # CME futures generally have a weekday maintenance break near 21:00-22:00 UTC
    # during daylight time. Treat only a narrow boundary as a non-data gap.
    expected_reopen = previous_utc.replace(hour=22, minute=0, second=0, microsecond=0)
    expected_close = previous_utc.replace(hour=20, minute=59, second=0, microsecond=0)
    return previous_utc == expected_close and current_utc == expected_reopen


def _blocking_context_gaps(gaps: Sequence[TrackBStartupContextGap]) -> list[TrackBStartupContextGap]:
    allowed = {"SESSION_BOUNDARY_GAP_ALLOWED", "GAP_OUTSIDE_REQUIRED_WINDOW", "REPAIRED_BACKFILL_GAP"}
    return [gap for gap in gaps if gap.within_required_window and gap.classification not in allowed]


def _mark_repaired_gaps(
    before: Sequence[TrackBStartupContextGap],
    after: Sequence[TrackBStartupContextGap],
    *,
    repair_source: str | None,
) -> list[TrackBStartupContextGap]:
    remaining = {(gap.start_timestamp, gap.end_timestamp): gap for gap in after}
    marked: list[TrackBStartupContextGap] = []
    for gap in before:
        key = (gap.start_timestamp, gap.end_timestamp)
        if key not in remaining and gap.within_required_window:
            marked.append(
                TrackBStartupContextGap(
                    start_timestamp=gap.start_timestamp,
                    end_timestamp=gap.end_timestamp,
                    missing_expected_bars=gap.missing_expected_bars,
                    gap_duration_seconds=gap.gap_duration_seconds,
                    within_required_window=gap.within_required_window,
                    classification="REPAIRED_BACKFILL_GAP",
                    repair_attempted=True,
                    repair_succeeded=True,
                    repair_source=repair_source,
                    remaining_blocker=None,
                )
            )
        elif key in remaining and gap.within_required_window:
            marked.append(remaining[key])
        else:
            marked.append(gap)
    return marked


def _completed_5m_count_from_1m(candles: Sequence[Mapping[str, Any]]) -> int:
    groups: dict[datetime, int] = {}
    for candle in candles:
        try:
            ts = _parse_time(str(candle.get("candle_timestamp") or candle.get("timestamp")))
        except ValueError:
            continue
        key = ts.replace(minute=ts.minute - (ts.minute % 5), second=0, microsecond=0)
        groups[key] = groups.get(key, 0) + 1
    return sum(1 for count in groups.values() if count >= 5)


def _startup_blocked_reason(
    *,
    context_ready: bool,
    live_execution_approved: bool,
    latest_decision_bar_source: object,
    context_gap_count: int,
    available_1m: int,
    required_1m: int,
    available_5m: int,
    required_5m: int,
) -> str | None:
    if context_ready and live_execution_approved and latest_decision_bar_source == "DATABENTO_LIVE_ARTIFACT":
        return None
    if latest_decision_bar_source != "DATABENTO_LIVE_ARTIFACT":
        return "Latest decision bar is not sourced from DATABENTO_LIVE_ARTIFACT."
    if context_gap_count > 0:
        return f"Startup feature context has {context_gap_count} detected 1m gap(s)."
    if available_1m < required_1m:
        return f"Feature context has {available_1m}/{required_1m} required 1m bars."
    if available_5m < required_5m:
        return f"Feature context has {available_5m}/{required_5m} required completed 5m bars."
    if not live_execution_approved:
        return "Databento Live execution approval is not satisfied."
    return "Startup readiness is inconclusive."


def _write_live_readiness_startup_diagnostic(
    *,
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    readiness: TrackBLiveFeedReadiness,
    now: datetime,
) -> Path:
    if readiness.live_execution_approved is not True:
        classification = "LIVE_EXECUTION_NOT_APPROVED"
    elif readiness.feature_context_ready is not True:
        classification = "FEATURE_CONTEXT_NOT_READY"
    else:
        classification = "READY_WITH_LIVE_ONLY_CONTEXT"
    payload = {
        "instrument_family": instrument.instrument_family,
        "strategy_ids": list(instrument.enabled_strategies),
        "required_1m_context_bars": readiness.required_1m_count,
        "available_1m_context_bars": readiness.warmup_1m_count,
        "required_5m_context_bars": readiness.required_completed_5m_count,
        "available_5m_context_bars": readiness.warmup_completed_5m_count,
        "live_1m_bars": readiness.warmup_1m_count,
        "live_completed_5m_bars": readiness.warmup_completed_5m_count,
        "required_live_1m_bars": readiness.live_execution_required_1m_count,
        "required_live_completed_5m_bars": readiness.live_execution_required_completed_5m_count,
        "backfill_gap_detected": readiness.feature_context_ready is not True,
        "backfill_gap_filled": False,
        "backfill_source": None,
        "context_gap_count": None,
        "context_ready": readiness.feature_context_ready,
        "live_transport_connected": readiness.transport_connected,
        "live_raw_messages_fresh": readiness.raw_messages_fresh,
        "live_completed_1m_fresh": readiness.completed_1m_fresh,
        "live_completed_5m_fresh": readiness.completed_5m_fresh,
        "latest_decision_bar_source": None,
        "live_execution_approved": readiness.live_execution_approved,
        "strategy_ready": readiness.strategy_ready,
        "paper_evaluation_allowed": False,
        "blocked_reason": readiness.blocker,
        "classification": classification,
        "live_event_path": None if readiness.event_path is None else str(readiness.event_path),
        "live_report_path": None if readiness.report_path is None else str(readiness.report_path),
    }
    return _write_startup_readiness_diagnostic(config=config, instrument_payload=payload, now=now)


def _write_startup_readiness_diagnostic(
    *,
    config: TrackBShadowMonitorConfig,
    instrument_payload: Mapping[str, Any],
    now: datetime,
) -> Path:
    path = Path(config.diagnostic_output_root) / "latest_track_b_startup_readiness_diagnostic.json"
    existing = _read_json_optional(path) or {}
    instruments = existing.get("instruments") if isinstance(existing.get("instruments"), Mapping) else {}
    updated = dict(instruments)
    family = str(instrument_payload.get("instrument_family") or "UNKNOWN")
    updated[family] = dict(instrument_payload)
    classifications = [
        str(item.get("classification"))
        for item in updated.values()
        if isinstance(item, Mapping) and item.get("classification")
    ]
    ready_classifications = {"READY_WITH_BACKFILL_SEEDED_CONTEXT", "READY_WITH_LIVE_ONLY_CONTEXT"}
    if classifications and all(item in ready_classifications for item in classifications):
        classification = (
            "READY_WITH_BACKFILL_SEEDED_CONTEXT"
            if any(item == "READY_WITH_BACKFILL_SEEDED_CONTEXT" for item in classifications)
            else "READY_WITH_LIVE_ONLY_CONTEXT"
        )
    elif all(item == "READY_WITH_LIVE_ONLY_CONTEXT" for item in classifications) and classifications:
        classification = "READY_WITH_LIVE_ONLY_CONTEXT"
    elif any(item == "LIVE_EXECUTION_NOT_APPROVED" for item in classifications):
        classification = "LIVE_EXECUTION_NOT_APPROVED"
    elif any(item == "LATEST_DECISION_BAR_NOT_LIVE" for item in classifications):
        classification = "LATEST_DECISION_BAR_NOT_LIVE"
    elif any(item == "BACKFILL_FAILED" for item in classifications):
        classification = "BACKFILL_FAILED"
    elif any(item == "BACKFILL_REQUIRED_IN_PROGRESS" for item in classifications):
        classification = "BACKFILL_REQUIRED_IN_PROGRESS"
    elif any(item == "FEATURE_CONTEXT_NOT_READY" for item in classifications):
        classification = "FEATURE_CONTEXT_NOT_READY"
    else:
        classification = "DIAGNOSTIC_INCONCLUSIVE"
    report = {
        "schema_version": "track_b_startup_readiness_diagnostic_v1",
        "generated_at": now.astimezone(UTC).isoformat(),
        "diagnosis_classification": classification,
        "instruments": updated,
        "submit_allowed": False,
        "submit_attempted": False,
        "paper_proof_invoked": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "report_json_path": str(path),
    }
    _write_json_file(path, report)
    return path


def _annotate_live_runtime_result(
    result: TrackBRuntimeCandleCaptureResult,
    *,
    live_report_json: Path,
    live_event_json: Path,
    live_report: Mapping[str, Any] | None,
) -> None:
    result.report.update(
        {
            "runtime_data_source": TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT.value,
            "monitor_runtime_candle_source": "DATABENTO_LIVE_RUNTIME_FEED_ARTIFACT",
            "live_feed_report_path": str(live_report_json) if live_report_json.exists() else None,
            "live_feed_event_path": str(live_event_json) if live_event_json.exists() else None,
            "live_feed_connected": None if live_report is None else live_report.get("live_feed_connected"),
            "live_feed_subscription_status": None if live_report is None else live_report.get("subscription_status"),
            "live_feed_verdict": None if live_report is None else live_report.get("live_runtime_feed_verdict"),
            "latest_record_ts_event": None if live_report is None else live_report.get("latest_record_ts_event"),
            "latest_record_ts_recv": None if live_report is None else live_report.get("latest_record_ts_recv"),
            "latency_ms": None if live_report is None else live_report.get("latency_ms"),
            "source_lineage": {
                "live_feed_report_path": str(live_report_json) if live_report_json.exists() else None,
                "live_feed_event_path": str(live_event_json),
            },
        }
    )
    _rewrite_runtime_result_files(result)


def _run_http_backfill_runtime_candle_capture(
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    now: datetime,
) -> TrackBRuntimeCandleCaptureResult:
    requested_window_end = now
    requested_window_start = requested_window_end - timedelta(minutes=max(config.lookback_minutes, 1))
    raw_api_key, credential_status, credential_source = _load_databento_api_key(config.env_file)
    if not raw_api_key:
        return write_runtime_candle_capture_provider_error(
            primary_blocker="DATABENTO_API_KEY is missing for Track B SHADOW runtime candle fetch.",
            required_next_action="Set DATABENTO_API_KEY in process environment or repo .env.local before running the shadow monitor.",
            source_id=f"{config.source_id}_runtime_candle_capture_cycle_{cycle_index}",
            account_id=instrument.execution_account_id,
            contract_key=instrument.contract_key,
            local_symbol=instrument.local_symbol,
            databento_continuous_symbol=instrument.databento_continuous_symbol,
            dataset=instrument.dataset,
            timeframe=config.timeframe,
            requested_window_start=requested_window_start,
            requested_window_end=requested_window_end,
            max_bars=config.max_bars,
            min_bars=config.min_bars,
            max_latest_1m_age_seconds=config.max_latest_1m_age_seconds,
            max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
            provider_credential_status=credential_status,
            provider_credential_source=credential_source,
            provider_transport=config.provider_transport,
            provider_request_symbol=instrument.local_symbol if config.prefer_raw_local_symbol_for_runtime_fetch else instrument.databento_continuous_symbol,
            provider_request_stype_in="raw_symbol" if config.prefer_raw_local_symbol_for_runtime_fetch else config.stype_in,
            provider_request_stype_out=config.provider_stype_out if config.provider_transport == "http" else None,
            output_root=config.runtime_candle_capture_output_root,
            capture_id=f"track_b_shadow_monitor_runtime_capture_cycle_{cycle_index}_{uuid.uuid4().hex}",
            now=now,
        )
    args = SimpleNamespace(
        databento_symbol=instrument.local_symbol if config.prefer_raw_local_symbol_for_runtime_fetch else None,
        databento_continuous_symbol=instrument.databento_continuous_symbol,
        stype_in=config.stype_in,
        dataset=instrument.dataset,
        schema=config.schema,
        max_bars=config.max_bars,
        provider_timeout_seconds=config.provider_timeout_seconds,
        provider_transport=config.provider_transport,
        stype_out=config.provider_stype_out,
        base_url=config.base_url,
        lookback_minutes=config.lookback_minutes,
        source_id=f"{config.source_id}_runtime_candle_capture_cycle_{cycle_index}",
        account_id=instrument.execution_account_id,
        expected_account_id=instrument.expected_account_id,
        contract_key=instrument.contract_key,
        local_symbol=instrument.local_symbol,
        strategy_id="track_b_shadow_monitor",
        lane_id=f"{instrument.instrument_family.lower()}_shadow_monitor",
        timeframe=config.timeframe,
    )
    try:
        with _provider_fetch_deadline(config.provider_timeout_seconds):
            records, provider_available_end, history_end_used, available_end_lag_seconds, candle_source_mode = _fetch_records(
                args=args,
                api_key=raw_api_key,
                requested_window_start=requested_window_start,
                requested_window_end=requested_window_end,
            )
    except Exception as exc:  # noqa: BLE001
        fallback_candidate_report_json = _latest_runtime_report_path(Path(config.runtime_candle_capture_output_root), instrument)
        fallback_candidate_event_json = _latest_runtime_1m_path(Path(config.runtime_candle_capture_output_root), instrument)
        fallback_candidate_report = _read_json_optional(fallback_candidate_report_json)
        fallback_candidate_payload = _read_json_optional(fallback_candidate_event_json)
        provider_error = write_runtime_candle_capture_provider_error(
            primary_blocker=f"Track B SHADOW Databento runtime candle fetch failed: {exc}",
            required_next_action="Retry after provider availability/entitlement is healthy, or inspect Databento runtime diagnostics.",
            verdict=TrackBRuntimeCandleCaptureVerdict.FETCH_FAILED,
            source_id=f"{config.source_id}_runtime_candle_capture_cycle_{cycle_index}",
            account_id=instrument.execution_account_id,
            contract_key=instrument.contract_key,
            local_symbol=instrument.local_symbol,
            databento_continuous_symbol=instrument.databento_continuous_symbol,
            dataset=instrument.dataset,
            timeframe=config.timeframe,
            requested_window_start=requested_window_start,
            requested_window_end=requested_window_end,
            provider_available_end=getattr(exc, "provider_available_end", None),
            max_bars=config.max_bars,
            min_bars=config.min_bars,
            max_latest_1m_age_seconds=config.max_latest_1m_age_seconds,
            max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
            provider_credential_status=credential_status,
            provider_credential_source=credential_source,
            output_root=config.runtime_candle_capture_output_root,
            capture_id=f"track_b_shadow_monitor_runtime_capture_cycle_{cycle_index}_{uuid.uuid4().hex}",
            now=now,
        )
        fallback = _fresh_runtime_artifact_fallback(
            config=config,
            instrument=instrument,
            provider_result=provider_error,
            cycle_index=cycle_index,
            now=now,
            latest_report_json=fallback_candidate_report_json,
            latest_event_json=fallback_candidate_event_json,
            latest_report=fallback_candidate_report,
            latest_payload=fallback_candidate_payload,
        )
        result = fallback or provider_error
        _annotate_http_backfill_runtime_result(result)
        return result
    quote_payload = _safe_quote_payload(config.current_quote_report_json)
    payload = _runtime_payload_from_records(
        records=records,
        args=args,
        quote_payload=quote_payload,
        candle_source_mode=candle_source_mode,
        requested_window_start=requested_window_start,
        requested_window_end=requested_window_end,
        provider_available_end=provider_available_end,
        history_end_used=history_end_used,
        available_end_lag_seconds=available_end_lag_seconds,
    )
    result = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=payload,
        source_payload_path=None,
        expected_account_id=instrument.expected_account_id,
        account_id=instrument.execution_account_id,
        contract_key=instrument.contract_key,
        local_symbol=instrument.local_symbol,
        databento_continuous_symbol=instrument.databento_continuous_symbol,
        dataset=instrument.dataset,
        timeframe=config.timeframe,
        max_bars=config.max_bars,
        min_bars=config.min_bars,
        candle_source_mode=candle_source_mode,
        requested_window_start=requested_window_start,
        requested_window_end=requested_window_end,
        provider_available_end=provider_available_end,
        history_end_used=history_end_used,
        available_end_lag_seconds=available_end_lag_seconds,
        provider_transport=config.provider_transport,
        provider_request_symbol=instrument.local_symbol if config.prefer_raw_local_symbol_for_runtime_fetch else instrument.databento_continuous_symbol,
        provider_request_stype_in="raw_symbol" if config.prefer_raw_local_symbol_for_runtime_fetch else config.stype_in,
        provider_request_stype_out=config.provider_stype_out if config.provider_transport == "http" else None,
        max_latest_1m_age_seconds=config.max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        provider_credential_status=credential_status,
        provider_credential_source=credential_source,
        source_id=f"{config.source_id}_runtime_candle_capture_cycle_{cycle_index}",
        output_root=config.runtime_candle_capture_output_root,
        capture_id=f"track_b_shadow_monitor_runtime_capture_cycle_{cycle_index}_{uuid.uuid4().hex}",
        now=now,
    )
    _annotate_http_backfill_runtime_result(result)
    return result


def _annotate_http_backfill_runtime_result(result: TrackBRuntimeCandleCaptureResult) -> None:
    result.report.update(
        {
            "runtime_data_source": TrackBRuntimeDataSource.DATABENTO_HTTP_BACKFILL.value,
            "monitor_runtime_candle_source": "DATABENTO_HTTP_BACKFILL_NOT_LIVE",
            "http_backfill_used": True,
            "execution_live_source": False,
            "required_next_action": (
                "HTTP/historical data was captured for backfill or recovery only; start Databento Live runtime feed "
                "before strategy evaluation."
            ),
        }
    )
    _rewrite_runtime_result_files(result)


def _runtime_for_refresh_cadence(
    *,
    config: TrackBShadowMonitorConfig,
    stages: TrackBShadowMonitorStages,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    started_at: datetime,
    last_runtime_fetch_attempt_at: dict[str, datetime],
) -> TrackBRuntimeCandleCaptureResult:
    last_fetch_at = last_runtime_fetch_attempt_at.get(instrument.instrument_family)
    if _should_reuse_runtime_artifact_for_cadence(
        config=config,
        instrument=instrument,
        now=started_at,
        last_fetch_at=last_fetch_at,
    ):
        reused = _runtime_artifact_reuse_for_refresh_cadence(
            config=config,
            instrument=instrument,
            cycle_index=cycle_index,
            now=started_at,
        )
        if reused is not None:
            return reused

    result = stages.runtime_candle_capture(config, instrument, cycle_index, started_at)
    last_runtime_fetch_attempt_at[instrument.instrument_family] = started_at
    return result


def _should_reuse_runtime_artifact_for_cadence(
    *,
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    now: datetime,
    last_fetch_at: datetime | None,
) -> bool:
    if config.data_refresh_seconds <= 0:
        return False
    if _runtime_data_source(config) == TrackBRuntimeDataSource.DATABENTO_LIVE_ARTIFACT:
        return False
    if last_fetch_at is None:
        return False
    if instrument.evaluation_mode != TrackBStrategyEvaluationMode.COMPLETED_BAR_ONLY:
        return False
    elapsed = max(0.0, (now.astimezone(UTC) - last_fetch_at.astimezone(UTC)).total_seconds())
    return elapsed < float(config.data_refresh_seconds)


def _runtime_artifact_reuse_for_refresh_cadence(
    *,
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    now: datetime,
) -> TrackBRuntimeCandleCaptureResult | None:
    event_json = _latest_runtime_1m_path(Path(config.runtime_candle_capture_output_root), instrument)
    report_json = _latest_runtime_report_path(Path(config.runtime_candle_capture_output_root), instrument)
    if not event_json.exists():
        return None
    try:
        payload = _read_json_required(event_json)
    except Exception:  # noqa: BLE001 - provider fetch can still repair unreadable cached context.
        return None

    source_mode = f"{payload.get('candle_source_mode') or 'RUNTIME_CANDLES'}_REFRESH_CADENCE_REUSE"
    result = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=payload,
        source_payload_path=event_json,
        expected_account_id=instrument.expected_account_id,
        account_id=instrument.execution_account_id,
        contract_key=instrument.contract_key,
        local_symbol=instrument.local_symbol,
        databento_continuous_symbol=instrument.databento_continuous_symbol,
        dataset=instrument.dataset,
        timeframe=config.timeframe,
        max_bars=config.max_bars,
        min_bars=config.min_bars,
        candle_source_mode=source_mode,
        provider_transport=config.provider_transport,
        provider_request_symbol=instrument.local_symbol if config.prefer_raw_local_symbol_for_runtime_fetch else instrument.databento_continuous_symbol,
        provider_request_stype_in="raw_symbol" if config.prefer_raw_local_symbol_for_runtime_fetch else config.stype_in,
        provider_request_stype_out=config.provider_stype_out if config.provider_transport == "http" else None,
        max_latest_1m_age_seconds=config.max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        source_id=f"{config.source_id}_runtime_candle_capture_cycle_{cycle_index}_refresh_cadence_reuse",
        output_root=config.runtime_candle_capture_output_root,
        capture_id=f"track_b_shadow_monitor_runtime_capture_cycle_{cycle_index}_refresh_cadence_reuse_{uuid.uuid4().hex}",
        now=now,
    )
    if result.report.get("data_written") is not True:
        return None
    _annotate_runtime_refresh_cadence_reuse_report(
        result=result,
        latest_report_json=report_json,
        latest_event_json=event_json,
        source_mode=source_mode,
        data_refresh_seconds=config.data_refresh_seconds,
    )
    return result


def _annotate_runtime_refresh_cadence_reuse_report(
    *,
    result: TrackBRuntimeCandleCaptureResult,
    latest_report_json: Path,
    latest_event_json: Path,
    source_mode: str,
    data_refresh_seconds: float,
) -> None:
    source = (
        "FRESH_EXISTING_RUNTIME_ARTIFACT_REFRESH_CADENCE"
        if result.report.get("fresh_for_execution") is True
        else "EXISTING_RUNTIME_ARTIFACT_REFRESH_CADENCE_NOT_FRESH"
    )
    result.report.update(
        {
            "monitor_runtime_candle_source": source,
            "provider_fetch_skipped_for_refresh_cadence": True,
            "provider_fetch_failed_before_fallback": False,
            "data_refresh_seconds": data_refresh_seconds,
            "fallback_source_report_path": str(latest_report_json),
            "fallback_source_event_path": str(latest_event_json),
            "candle_source_mode": source_mode,
            "source_lineage": {
                "refresh_cadence_source_report_path": str(latest_report_json),
                "refresh_cadence_source_event_path": str(latest_event_json),
            },
        }
    )
    _rewrite_runtime_result_files(result)


def _fresh_runtime_artifact_fallback(
    *,
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    provider_result: TrackBRuntimeCandleCaptureResult,
    cycle_index: int,
    now: datetime,
    latest_report_json: Path | None = None,
    latest_event_json: Path | None = None,
    latest_report: Mapping[str, Any] | None = None,
    latest_payload: Mapping[str, Any] | None = None,
) -> TrackBRuntimeCandleCaptureResult | None:
    if not config.allow_fresh_runtime_artifact_fallback:
        return None
    actual_report_json = latest_report_json or _latest_runtime_report_path(Path(config.runtime_candle_capture_output_root), instrument)
    actual_event_json = latest_event_json or _latest_runtime_1m_path(Path(config.runtime_candle_capture_output_root), instrument)
    actual_report = latest_report or _read_json_optional(actual_report_json)
    if not actual_report or not actual_event_json.exists():
        return None
    if actual_report.get("data_written") is not True or actual_report.get("fresh_for_execution") is not True:
        return None
    mismatch = _runtime_artifact_mismatch(report=actual_report, instrument=instrument, dataset=instrument.dataset)
    if mismatch:
        return None
    payload = dict(latest_payload or {})
    if not payload:
        try:
            payload = _read_json_required(actual_event_json)
        except Exception:  # noqa: BLE001
            return None
    fallback_source_mode = f"{actual_report.get('candle_source_mode') or 'RUNTIME_CANDLES'}_FRESH_ARTIFACT_FALLBACK"
    fallback = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=payload,
        source_payload_path=actual_event_json,
        expected_account_id=instrument.expected_account_id,
        account_id=instrument.execution_account_id,
        contract_key=instrument.contract_key,
        local_symbol=instrument.local_symbol,
        databento_continuous_symbol=instrument.databento_continuous_symbol,
        dataset=instrument.dataset,
        timeframe=config.timeframe,
        max_bars=config.max_bars,
        min_bars=config.min_bars,
        candle_source_mode=fallback_source_mode,
        provider_transport=config.provider_transport,
        provider_request_symbol=instrument.local_symbol if config.prefer_raw_local_symbol_for_runtime_fetch else instrument.databento_continuous_symbol,
        provider_request_stype_in="raw_symbol" if config.prefer_raw_local_symbol_for_runtime_fetch else config.stype_in,
        provider_request_stype_out=config.provider_stype_out if config.provider_transport == "http" else None,
        max_latest_1m_age_seconds=config.max_latest_1m_age_seconds,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        source_id=f"{config.source_id}_runtime_candle_capture_cycle_{cycle_index}_fresh_artifact_fallback",
        output_root=config.runtime_candle_capture_output_root,
        capture_id=f"track_b_shadow_monitor_runtime_capture_cycle_{cycle_index}_fallback_{uuid.uuid4().hex}",
        now=now,
    )
    _annotate_runtime_fallback_report(
        fallback=fallback,
        provider_result=provider_result,
        latest_report_json=actual_report_json,
        latest_event_json=actual_event_json,
        fallback_source_mode=fallback_source_mode,
    )
    return fallback if fallback.report.get("fresh_for_execution") is True else None


def _runtime_artifact_mismatch(
    *,
    report: Mapping[str, Any],
    instrument: TrackBShadowMonitorInstrumentConfig,
    dataset: str,
) -> str | None:
    expected = {
        "contract_key": instrument.contract_key,
        "local_symbol": instrument.local_symbol,
        "databento_continuous_symbol": instrument.databento_continuous_symbol,
        "dataset": dataset,
    }
    for key, value in expected.items():
        if str(report.get(key) or "") != str(value):
            return f"{key} mismatch: expected {value}, observed {report.get(key)}"
    return None


def _annotate_runtime_fallback_report(
    *,
    fallback: TrackBRuntimeCandleCaptureResult,
    provider_result: TrackBRuntimeCandleCaptureResult,
    latest_report_json: Path,
    latest_event_json: Path,
    fallback_source_mode: str,
) -> None:
    fallback.report.update(
        {
            "monitor_runtime_candle_source": "FRESH_EXISTING_RUNTIME_ARTIFACT_AFTER_PROVIDER_FAILURE",
            "provider_fetch_failed_before_fallback": True,
            "provider_fetch_failure_report_path": str(provider_result.report_json),
            "provider_fetch_failure_verdict": provider_result.report.get("runtime_candle_capture_verdict"),
            "provider_fetch_failure_category": _provider_failure_category(provider_result.report),
            "provider_fetch_failure_blocker": provider_result.report.get("primary_blocker"),
            "fallback_source_report_path": str(latest_report_json),
            "fallback_source_event_path": str(latest_event_json),
            "candle_source_mode": fallback_source_mode,
            "source_lineage": {
                "provider_fetch_failure_report_path": str(provider_result.report_json),
                "fallback_source_report_path": str(latest_report_json),
                "fallback_source_event_path": str(latest_event_json),
            },
        }
    )
    _rewrite_runtime_result_files(fallback)


def _rewrite_runtime_result_files(result: TrackBRuntimeCandleCaptureResult) -> None:
    payload = json.dumps(to_jsonable(result.report), indent=2, sort_keys=True)
    result.report_json.write_text(payload, encoding="utf-8")
    latest = result.report.get("latest_report_json_path")
    if latest:
        Path(str(latest)).write_text(payload, encoding="utf-8")


def _run_asian_drift_watch_chain(
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    now: datetime,
    runtime: TrackBRuntimeCandleCaptureResult,
) -> TrackBAsianDriftWatchChainResult:
    return run_track_b_asian_drift_watch_chain(
        candle_payload=runtime.runtime_candles_event or _read_json_required(runtime.runtime_candles_json),
        source_payload_path=runtime.runtime_candles_json,
        current_quote_report_payload=_safe_quote_payload(config.current_quote_report_json),
        current_quote_report_json=config.current_quote_report_json if config.current_quote_report_json and config.current_quote_report_json.exists() else None,
        expected_account_id=instrument.expected_account_id,
        account_id=instrument.execution_account_id,
        contract_key=instrument.contract_key,
        instrument_family=instrument.instrument_family,
        local_symbol=instrument.local_symbol,
        dataset=instrument.dataset,
        source_id=f"{config.source_id}_asian_drift_cycle_{cycle_index}",
        max_source_bars=config.max_bars,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        output_root=config.asian_drift_output_root,
        inbox_dir=config.inbox_dir,
        now=now,
    )


def _run_snap_turn_envelopes(
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    now: datetime,
    asian: TrackBAsianDriftWatchChainResult,
) -> TrackBSnapTurnEnvelopeProducerResult:
    if not any(
        strategy in instrument.enabled_strategies
        for strategy in ("FIRST_BULL_SNAP_TURN_V1", "FIRST_BEAR_SNAP_TURN_V1", "MNQ_FIRST_BEAR_SNAP_TURN_V1")
    ):
        report_json = Path(config.snap_turn_output_root) / (
            f"track_b_snap_turn_envelope_producer_skipped_{instrument.instrument_family.lower()}_{cycle_index}"
        ) / "snap_turn_envelope_producer_report.json"
        report = {
            "snap_turn_envelope_producer_verdict": TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES.value,
            "producer_id": f"track_b_snap_turn_envelope_producer_skipped_{instrument.instrument_family.lower()}_{cycle_index}",
            "source_id": f"{config.source_id}_snap_turn_cycle_{cycle_index}",
            "generated_at": now.isoformat(),
            "report_json_path": str(report_json),
            "instrument_family": instrument.instrument_family,
            "skipped_for_instrument_strategy_set": True,
            "first_bull_snap_turn_envelope_ready": False,
            "first_bear_snap_turn_envelope_ready": False,
            "primary_blocker": None,
            "submit_allowed": False,
            "submit_attempted": False,
            "broker_state_mutated": False,
            "live_money_readiness": False,
        }
        report_json.parent.mkdir(parents=True, exist_ok=True)
        report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
        return TrackBSnapTurnEnvelopeProducerResult(
            verdict=TrackBSnapTurnEnvelopeProducerVerdict.WROTE_ENVELOPES,
            report_json=report_json,
            report=report,
            first_bull_snap_turn_event_json=None,
            first_bear_snap_turn_event_json=None,
            first_bull_snap_turn_event=None,
            first_bear_snap_turn_event=None,
        )
    return produce_track_b_snap_turn_envelopes(
        runtime_5m_payload=asian.completed_5m_candles_payload or _read_json_required(asian.completed_5m_candles_json),
        runtime_5m_payload_path=asian.completed_5m_candles_json,
        expected_account_id=instrument.expected_account_id,
        source_id=f"{config.source_id}_snap_turn_cycle_{cycle_index}",
        output_root=config.snap_turn_output_root,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        now=now,
    )


def _run_session_strategy_envelopes(
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    now: datetime,
    asian: TrackBAsianDriftWatchChainResult,
) -> TrackBSessionStrategyEnvelopeProducerResult:
    return produce_track_b_session_strategy_envelopes(
        runtime_5m_payload=asian.completed_5m_candles_payload or _read_json_required(asian.completed_5m_candles_json),
        runtime_5m_payload_path=asian.completed_5m_candles_json,
        expected_account_id=instrument.expected_account_id,
        source_id=f"{config.source_id}_session_strategy_cycle_{cycle_index}",
        output_root=config.session_strategy_output_root,
        max_completed_5m_age_seconds=config.max_completed_5m_age_seconds,
        now=now,
    )


def _run_multi_strategy_runtime_cycle(
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    cycle_index: int,
    now: datetime,
    asian: TrackBAsianDriftWatchChainResult,
    snap: TrackBSnapTurnEnvelopeProducerResult,
    session: TrackBSessionStrategyEnvelopeProducerResult,
) -> TrackBMultiStrategyRuntimeCycleResult:
    return run_track_b_multi_strategy_runtime_cycle(
        config=TrackBMultiStrategyRuntimeCycleConfig(
            enabled_strategy_ids=instrument.enabled_strategies,
            asian_drift_event_json=_path_from_report(asian.report, "asian_drift_state_snapshot_path"),
            pause_resume_short_event_json=session.asia_early_pause_resume_short_event_json,
            breakout_retest_hold_long_event_json=session.asia_early_normal_breakout_retest_hold_long_event_json,
            first_bull_snap_turn_event_json=snap.first_bull_snap_turn_event_json,
            first_bear_snap_turn_event_json=snap.first_bear_snap_turn_event_json,
            london_late_pause_resume_short_event_json=session.london_late_pause_resume_short_event_json,
            asia_late_flat_pullback_pause_resume_long_event_json=session.asia_late_flat_pullback_pause_resume_long_event_json,
            us_derivative_bear_turn_event_json=session.us_derivative_bear_turn_event_json,
            mnq_us_derivative_bear_turn_event_json=session.mnq_us_derivative_bear_turn_event_json,
            mnq_first_bear_snap_turn_event_json=(
                snap.first_bear_snap_turn_event_json
                if "MNQ_FIRST_BEAR_SNAP_TURN_V1" in instrument.enabled_strategies
                else None
            ),
            us_late_pause_resume_long_event_json=session.us_late_pause_resume_long_event_json,
            inbox_dir=config.inbox_dir,
            source_id=f"{config.source_id}_multi_strategy_cycle_{cycle_index}",
            mode="PAPER",
            host=config.host,
            port=config.port,
            client_id=config.client_id,
            account_id=instrument.execution_account_id,
            expected_account_id=instrument.expected_account_id,
            contract_key=instrument.contract_key,
            side=_monitor_paper_side(config),
            quantity=config.quantity,
            submit_paper=_monitor_paper_submit_requested(config),
            confirm_paper_submit=_monitor_paper_submit_requested(config),
            manual_open_limit_price=config.manual_open_limit_price,
            manual_close_limit_price=config.manual_close_limit_price,
            paper_order_pricing_policy=config.paper_order_pricing_policy,
            paper_order_price_offset_ticks=config.paper_order_price_offset_ticks,
            paper_exit_price_offset_ticks=config.paper_exit_price_offset_ticks,
            pricing_context_json=_latest_runtime_1m_path(Path(config.runtime_candle_capture_output_root), instrument),
            live_quote_report_json=_latest_live_quote_status_path(Path(config.live_runtime_feed_output_root), instrument.instrument_family),
            max_pricing_context_age_seconds=config.max_latest_1m_age_seconds,
            tick_size=instrument.tick_size or config.tick_size,
            allowlisted_local_symbol=instrument.local_symbol,
            con_id=instrument.con_id if instrument.con_id is not None else config.con_id,
            output_root=config.multi_strategy_output_root,
            update_operator_status=config.update_operator_status,
            operator_status_output_root=config.operator_status_output_root,
            backend_health_json=config.backend_health_json,
        ),
        now=now,
    )


def _run_operator_status(
    config: TrackBShadowMonitorConfig,
    monitor_report_json: Path,
    runtime_cycle_report_json: Path | None,
    now: datetime,
) -> OperatorStatusResult:
    return create_operator_status_summary(
        inputs=OperatorStatusInputs(
            backend_health_json=_existing_optional_path(config.backend_health_json),
            track_b_shadow_monitor_report_json=monitor_report_json,
            track_b_shadow_monitor_heartbeat_json=_existing_optional_path(
                Path(config.output_root) / "latest_track_b_shadow_monitor_heartbeat.json"
            ),
            track_b_multi_strategy_runtime_cycle_report_json=runtime_cycle_report_json,
            databento_candle_observer_report_json=_existing_optional_path(DEFAULT_CURRENT_QUOTE_REPORT_JSON),
            output_root=config.operator_status_output_root,
        ),
        now=now,
    )


def _instrument_report_base(
    *,
    instrument: TrackBShadowMonitorInstrumentConfig | None = None,
    instrument_family: str | None = None,
    verdict: TrackBShadowMonitorVerdict,
    primary_blocker: str | None,
    required_next_action: str,
    live_feed: TrackBLiveFeedReadiness | None = None,
) -> dict[str, Any]:
    family = instrument.instrument_family if instrument else str(instrument_family)
    return {
        "instrument_family": family,
        "instrument_verdict": verdict.value,
        "contract_key": instrument.contract_key if instrument else None,
        "local_symbol": instrument.local_symbol if instrument else None,
        "databento_continuous_symbol": instrument.databento_continuous_symbol if instrument else None,
        "dataset": instrument.dataset if instrument else None,
        "timeframes": list(instrument.timeframes) if instrument else [],
        "enabled_strategies": list(instrument.enabled_strategies) if instrument else [],
        "evaluation_mode": instrument.evaluation_mode.value if instrument else None,
        "requires_quote_freshness": instrument.requires_quote_freshness if instrument else False,
        "runtime_chain_wired": instrument.runtime_chain_wired if instrument else False,
        "runtime_candle_capture_verdict": None,
        "runtime_data_source": None,
        "runtime_decision_source": None,
        "live_feed_managed": None if live_feed is None else live_feed.managed,
        "live_feed_owned_by_monitor": None if live_feed is None else live_feed.owned_by_monitor,
        "live_feed_pid": None if live_feed is None else live_feed.pid,
        "live_feed_status": None if live_feed is None else live_feed.status,
        "live_feed_connected": None if live_feed is None else live_feed.live_feed_connected,
        "live_feed_subscription_status": None if live_feed is None else live_feed.subscription_status,
        "live_feed_heartbeat_age_seconds": None if live_feed is None else live_feed.heartbeat_age_seconds,
        "live_feed_transport_connected": None if live_feed is None else live_feed.transport_connected,
        "live_feed_raw_messages_fresh": None if live_feed is None else live_feed.raw_messages_fresh,
        "live_feed_completed_1m_fresh": None if live_feed is None else live_feed.completed_1m_fresh,
        "live_feed_completed_5m_fresh": None if live_feed is None else live_feed.completed_5m_fresh,
        "live_feed_execution_fresh": None if live_feed is None else live_feed.execution_fresh,
        "live_execution_approved": None if live_feed is None else live_feed.live_execution_approved,
        "live_confirmation_1m_count": None if live_feed is None else live_feed.live_confirmation_1m_count,
        "live_confirmation_completed_5m_count": None if live_feed is None else live_feed.live_confirmation_completed_5m_count,
        "live_execution_required_1m_count": None if live_feed is None else live_feed.live_execution_required_1m_count,
        "live_execution_required_completed_5m_count": None
        if live_feed is None
        else live_feed.live_execution_required_completed_5m_count,
        "live_feed_latest_1m_age_seconds": None if live_feed is None else live_feed.latest_1m_age_seconds,
        "live_feed_latest_completed_5m_age_seconds": None if live_feed is None else live_feed.latest_completed_5m_age_seconds,
        "live_feed_execution_freshness_blocker": None if live_feed is None else live_feed.execution_freshness_blocker,
        "live_feed_strategy_ready": None if live_feed is None else live_feed.strategy_ready,
        "live_feed_warmup_1m_count": None if live_feed is None else live_feed.warmup_1m_count,
        "live_feed_warmup_completed_5m_count": None if live_feed is None else live_feed.warmup_completed_5m_count,
        "live_feed_required_1m_count": None if live_feed is None else live_feed.required_1m_count,
        "live_feed_required_completed_5m_count": None if live_feed is None else live_feed.required_completed_5m_count,
        "feature_context_ready": None if live_feed is None else live_feed.feature_context_ready,
        "feature_context_source": None if live_feed is None else live_feed.feature_context_source,
        "paper_evaluation_allowed": False,
        "live_feed_blocker": None if live_feed is None else live_feed.blocker,
        "live_feed_report_path": None if live_feed is None or live_feed.report_path is None else str(live_feed.report_path),
        "live_feed_heartbeat_path": None if live_feed is None or live_feed.heartbeat_path is None else str(live_feed.heartbeat_path),
        "live_feed_event_path": None if live_feed is None or live_feed.event_path is None else str(live_feed.event_path),
        "data_written": False,
        "fresh_for_execution": False,
        "latest_1m_timestamp": None,
        "latest_completed_5m_timestamp": None,
        "runtime_candle_age_seconds": None,
        "evaluated_strategy_count": 0,
        "not_ready_strategy_count": len(instrument.enabled_strategies) if instrument else 0,
        "no_signal_strategy_count": 0,
        "signal_strategy_count": 0,
        "candidate_signals": [],
        "suppressed_signals": [],
        "arbitration_result": {},
        "decision_journal_tier_counts": {},
        "submit_allowed": False,
        "readiness_invoked": False,
        "paper_proof_invoked": False,
        "submit_attempted": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "primary_blocker": primary_blocker,
        "required_next_action": required_next_action,
    }


def _instrument_report_from_stages(
    *,
    instrument: TrackBShadowMonitorInstrumentConfig,
    verdict: TrackBShadowMonitorVerdict,
    runtime: TrackBRuntimeCandleCaptureResult | None = None,
    asian: TrackBAsianDriftWatchChainResult | None = None,
    snap: TrackBSnapTurnEnvelopeProducerResult | None = None,
    session: TrackBSessionStrategyEnvelopeProducerResult | None = None,
    runtime_cycle: TrackBMultiStrategyRuntimeCycleResult | None = None,
    live_feed: TrackBLiveFeedReadiness | None = None,
    primary_blocker: object | None = None,
    required_next_action: str,
) -> dict[str, Any]:
    base = _instrument_report_base(
        instrument=instrument,
        verdict=verdict,
        primary_blocker=None if primary_blocker is None else str(primary_blocker),
        required_next_action=required_next_action,
        live_feed=live_feed,
    )
    runtime_report = runtime.report if runtime is not None else {}
    runtime_cycle_report = runtime_cycle.report if runtime_cycle is not None else {}
    strategy_verdicts = [
        {
            "strategy_id": item.get("strategy_id"),
            "strategy_runtime_verdict": item.get("strategy_runtime_verdict"),
            "decision": item.get("decision"),
            "signal_emitted": item.get("signal_emitted"),
            "primary_blocker": item.get("primary_blocker"),
        }
        for item in runtime_cycle_report.get("evaluated_strategies", [])
        if isinstance(item, Mapping)
    ]
    base.update(
        {
            "runtime_candle_capture_verdict": runtime_report.get("runtime_candle_capture_verdict"),
            "runtime_data_source": runtime_report.get("runtime_data_source"),
            "runtime_decision_source": runtime_report.get("runtime_data_source"),
            "runtime_candle_capture_report_path": str(runtime.report_json) if runtime is not None else None,
            "runtime_provider_status_category": _provider_failure_category(runtime_report),
            "monitor_runtime_candle_source": runtime_report.get("monitor_runtime_candle_source") or "PROVIDER_FETCH",
            "live_feed_managed": base.get("live_feed_managed"),
            "live_feed_owned_by_monitor": base.get("live_feed_owned_by_monitor"),
            "live_feed_pid": base.get("live_feed_pid"),
            "live_feed_status": base.get("live_feed_status"),
            "live_feed_report_path": runtime_report.get("live_feed_report_path") or base.get("live_feed_report_path"),
            "live_feed_heartbeat_path": base.get("live_feed_heartbeat_path"),
            "live_feed_event_path": runtime_report.get("live_feed_event_path") or base.get("live_feed_event_path"),
            "live_feed_connected": runtime_report.get("live_feed_connected")
            if runtime_report.get("live_feed_connected") is not None
            else base.get("live_feed_connected"),
            "live_feed_subscription_status": runtime_report.get("live_feed_subscription_status")
            or base.get("live_feed_subscription_status"),
            "live_feed_heartbeat_age_seconds": base.get("live_feed_heartbeat_age_seconds"),
            "live_feed_transport_connected": base.get("live_feed_transport_connected"),
            "live_feed_raw_messages_fresh": base.get("live_feed_raw_messages_fresh"),
            "live_feed_completed_1m_fresh": base.get("live_feed_completed_1m_fresh"),
            "live_feed_completed_5m_fresh": base.get("live_feed_completed_5m_fresh"),
            "live_feed_execution_fresh": base.get("live_feed_execution_fresh"),
            "live_execution_approved": base.get("live_execution_approved"),
            "live_confirmation_1m_count": base.get("live_confirmation_1m_count"),
            "live_confirmation_completed_5m_count": base.get("live_confirmation_completed_5m_count"),
            "live_execution_required_1m_count": base.get("live_execution_required_1m_count"),
            "live_execution_required_completed_5m_count": base.get("live_execution_required_completed_5m_count"),
            "live_feed_latest_1m_age_seconds": base.get("live_feed_latest_1m_age_seconds"),
            "live_feed_latest_completed_5m_age_seconds": base.get("live_feed_latest_completed_5m_age_seconds"),
            "live_feed_execution_freshness_blocker": base.get("live_feed_execution_freshness_blocker"),
            "live_feed_strategy_ready": (
                True
                if runtime_report.get("feature_context_ready") is True
                and base.get("live_execution_approved") is True
                and runtime_report.get("fresh_for_execution", False) is True
                else base.get("live_feed_strategy_ready")
            ),
            "live_feed_warmup_1m_count": base.get("live_feed_warmup_1m_count"),
            "live_feed_warmup_completed_5m_count": base.get("live_feed_warmup_completed_5m_count"),
            "live_feed_required_1m_count": base.get("live_feed_required_1m_count"),
            "live_feed_required_completed_5m_count": base.get("live_feed_required_completed_5m_count"),
            "feature_context_ready": runtime_report.get("feature_context_ready", base.get("feature_context_ready")),
            "feature_context_source": runtime_report.get("feature_context_source") or base.get("feature_context_source"),
            "paper_evaluation_allowed": bool(
                runtime_report.get("feature_context_ready", base.get("feature_context_ready")) is True
                and base.get("live_execution_approved") is True
                and runtime_report.get("fresh_for_execution", False) is True
            ),
            "live_feed_blocker": base.get("live_feed_blocker"),
            "live_feed_verdict": runtime_report.get("live_feed_verdict"),
            "latest_record_ts_event": runtime_report.get("latest_record_ts_event"),
            "latest_record_ts_recv": runtime_report.get("latest_record_ts_recv"),
            "latency_ms": runtime_report.get("latency_ms"),
            "provider_fetch_skipped_for_refresh_cadence": runtime_report.get("provider_fetch_skipped_for_refresh_cadence", False),
            "data_refresh_seconds": runtime_report.get("data_refresh_seconds"),
            "provider_fetch_failed_before_fallback": runtime_report.get("provider_fetch_failed_before_fallback", False),
            "provider_fetch_failure_category": runtime_report.get("provider_fetch_failure_category"),
            "provider_fetch_failure_report_path": runtime_report.get("provider_fetch_failure_report_path"),
            "source_lineage": runtime_report.get("source_lineage") or {},
            "data_written": runtime_report.get("data_written", False),
            "fresh_for_execution": runtime_report.get("fresh_for_execution", False),
            "latest_1m_timestamp": runtime_report.get("latest_1m_timestamp"),
            "latest_completed_5m_timestamp": runtime_report.get("latest_completed_5m_timestamp"),
            "latest_1m_age_seconds": runtime_report.get("latest_1m_candle_age_seconds"),
            "latest_completed_5m_age_seconds": runtime_report.get("latest_completed_5m_candle_age_seconds"),
            "execution_freshness_blocker": runtime_report.get("execution_freshness_blocker"),
            "runtime_candle_age_seconds": runtime_report.get("latest_completed_5m_candle_age_seconds"),
            "asian_drift_watch_chain_report_path": str(asian.report_json) if asian is not None else None,
            "asian_drift_watch_chain_verdict": asian.report.get("asian_drift_watch_chain_verdict") if asian is not None else None,
            "snap_turn_producer_report_path": str(snap.report_json) if snap is not None else None,
            "snap_turn_producer_verdict": snap.report.get("snap_turn_envelope_producer_verdict") if snap is not None else None,
            "session_producer_report_path": str(session.report_json) if session is not None else None,
            "session_producer_verdict": session.report.get("session_strategy_envelope_producer_verdict") if session is not None else None,
            "multi_strategy_runtime_cycle_report_path": str(runtime_cycle.report_json) if runtime_cycle is not None else None,
            "multi_strategy_runtime_cycle_verdict": runtime_cycle_report.get("multi_strategy_runtime_cycle_verdict"),
            "paper_runner_report_path": runtime_cycle_report.get("paper_runner_report_path"),
            "paper_runner_verdict": runtime_cycle_report.get("paper_runner_verdict"),
            "paper_proof_classification": runtime_cycle_report.get("paper_proof_classification"),
            "paper_order_parameters": runtime_cycle_report.get("paper_order_parameters") or {},
            "paper_order_parameter_blocker": runtime_cycle_report.get("paper_order_parameter_blocker"),
            "order_action": runtime_cycle_report.get("order_action"),
            "quantity": runtime_cycle_report.get("quantity"),
            "pricing_policy": runtime_cycle_report.get("pricing_policy"),
            "reference_price_source": runtime_cycle_report.get("reference_price_source"),
            "reference_price": runtime_cycle_report.get("reference_price"),
            "open_limit_price": runtime_cycle_report.get("open_limit_price"),
            "close_exit_policy": runtime_cycle_report.get("close_exit_policy"),
            "close_limit_price": runtime_cycle_report.get("close_limit_price"),
            "latest_broker_state_classification": runtime_cycle_report.get("paper_proof_classification")
            or runtime_cycle_report.get("final_broker_state_classification"),
            "final_broker_state_classification": runtime_cycle_report.get("final_broker_state_classification"),
            "final_flat": runtime_cycle_report.get("final_flat"),
            "evaluated_strategy_count": len(runtime_cycle_report.get("evaluated_strategies") or []),
            "strategy_verdicts": strategy_verdicts,
            "not_ready_strategy_count": _count_strategy_verdicts(strategy_verdicts, "NOT_READY"),
            "no_signal_strategy_count": _count_strategy_verdicts(strategy_verdicts, "NO_SIGNAL"),
            "signal_strategy_count": len(runtime_cycle_report.get("candidate_signals") or []),
            "candidate_signals": runtime_cycle_report.get("candidate_signals") or [],
            "suppressed_signals": runtime_cycle_report.get("suppressed_signals") or [],
            "arbitration_result": runtime_cycle_report.get("arbitration_result") or {},
            "chosen_signal": runtime_cycle_report.get("chosen_signal") or {},
            "decision_journal_summary_path": runtime_cycle_report.get("decision_journal_summary_path"),
            "decision_journal_tier_counts": runtime_cycle_report.get("decision_journal_tier_counts") or {},
            "submit_allowed": bool(runtime_cycle_report.get("submit_allowed", False)),
            "readiness_invoked": bool(runtime_cycle_report.get("readiness_invoked", False)),
            "paper_proof_invoked": bool(runtime_cycle_report.get("paper_proof_invoked", False)),
            "submit_attempted": bool(runtime_cycle_report.get("submit_attempted", False)),
            "broker_state_mutated": bool(runtime_cycle_report.get("broker_state_mutated", False)),
            "live_money_readiness": bool(runtime_cycle_report.get("live_money_readiness", False)),
        }
    )
    return base


def _report_for_cycle(
    *,
    config: TrackBShadowMonitorConfig,
    monitor_id: str,
    cycle_id: str,
    cycle_index: int,
    started_at: datetime,
    completed_at: datetime,
    report_json: Path,
    verdict: TrackBShadowMonitorVerdict,
    instrument_reports: Sequence[Mapping[str, Any]],
    primary_blocker: object | None,
    required_next_action: str,
    lock: TrackBShadowMonitorLock,
    paper_trades_attempted_count: int,
) -> dict[str, Any]:
    completed_at = completed_at.astimezone(UTC)
    aggregate_tiers = _aggregate_tier_counts(instrument_reports)
    paper_trade_summary = _read_json_optional(DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON) or {}
    live_position_status = _read_json_optional(DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON) or {}
    pnl_summary = _read_json_optional(DEFAULT_TRACK_B_PNL_SUMMARY_JSON) or {}
    all_strategy_verdicts = [
        verdict
        for item in instrument_reports
        for verdict in item.get("strategy_verdicts", [])
        if isinstance(verdict, Mapping)
    ]
    return {
        "monitor_schema_version": "track_b_shadow_monitor_v2",
        "schema_version": "track_b_shadow_monitor_v2",
        "monitor_id": monitor_id,
        "cycle_id": cycle_id,
        "mode": _monitor_mode(config),
        "monitor_mode": _monitor_mode(config),
        "runtime_data_source": _runtime_data_source(config).value,
        "runtime_decision_source": _runtime_data_source(config).value,
        "live_feed_managed": config.manage_live_feed,
        "paper_trading_enabled": config.enable_paper_trading,
        "paper_on_signal": config.paper_on_signal,
        "max_paper_trades_per_run": config.max_paper_trades_per_run,
        "paper_trades_attempted_count": paper_trades_attempted_count,
        "pause_after_paper_trade": config.pause_after_paper_trade,
        "paper_order_pricing_policy": config.paper_order_pricing_policy,
        "paper_order_price_offset_ticks": config.paper_order_price_offset_ticks,
        "paper_exit_price_offset_ticks": config.paper_exit_price_offset_ticks,
        "tick_size": config.tick_size,
        "started_at": started_at.astimezone(UTC).isoformat(),
        "completed_at": completed_at.isoformat(),
        "cycle_elapsed_seconds": round(max(0.0, (completed_at - started_at.astimezone(UTC)).total_seconds()), 3),
        "poll_seconds": config.poll_seconds,
        "data_refresh_seconds": config.data_refresh_seconds,
        "runtime_data_source": _runtime_data_source(config).value,
        "provider_timeout_seconds": config.provider_timeout_seconds,
        "provider_transport": config.provider_transport,
        "provider_stype_out": config.provider_stype_out,
        "prefer_raw_local_symbol_for_runtime_fetch": config.prefer_raw_local_symbol_for_runtime_fetch,
        "max_cycles": config.max_cycles,
        "cycle_index": cycle_index,
        "lockfile_path": str(lock.lockfile),
        "pidfile_path": str(lock.pidfile),
        "pid": lock.owner.get("pid"),
        "host": lock.owner.get("host"),
        "stale_lock_takeover": lock.stale_lock_takeover,
        "force_takeover_used": lock.force_takeover_used,
        "instrument_count": len(instrument_reports),
        "instrument_reports": list(instrument_reports),
        "instrument_families": [item.get("instrument_family") for item in instrument_reports],
        "runtime_data_freshness_by_instrument": {
            str(item.get("instrument_family")): {
                "data_written": item.get("data_written"),
                "runtime_data_source": item.get("runtime_data_source"),
                "runtime_decision_source": item.get("runtime_decision_source"),
                "live_feed_managed": item.get("live_feed_managed"),
                "live_feed_pid": item.get("live_feed_pid"),
                "live_feed_status": item.get("live_feed_status"),
                "live_feed_connected": item.get("live_feed_connected"),
                "live_feed_subscription_status": item.get("live_feed_subscription_status"),
                "live_feed_heartbeat_age_seconds": item.get("live_feed_heartbeat_age_seconds"),
                "live_feed_transport_connected": item.get("live_feed_transport_connected"),
                "live_feed_raw_messages_fresh": item.get("live_feed_raw_messages_fresh"),
                "live_feed_completed_1m_fresh": item.get("live_feed_completed_1m_fresh"),
                "live_feed_completed_5m_fresh": item.get("live_feed_completed_5m_fresh"),
                "live_feed_execution_fresh": item.get("live_feed_execution_fresh"),
                "live_execution_approved": item.get("live_execution_approved"),
                "live_confirmation_1m_count": item.get("live_confirmation_1m_count"),
                "live_confirmation_completed_5m_count": item.get("live_confirmation_completed_5m_count"),
                "live_execution_required_1m_count": item.get("live_execution_required_1m_count"),
                "live_execution_required_completed_5m_count": item.get("live_execution_required_completed_5m_count"),
                "live_feed_latest_1m_age_seconds": item.get("live_feed_latest_1m_age_seconds"),
                "live_feed_latest_completed_5m_age_seconds": item.get("live_feed_latest_completed_5m_age_seconds"),
                "live_feed_execution_freshness_blocker": item.get("live_feed_execution_freshness_blocker"),
                "live_feed_strategy_ready": item.get("live_feed_strategy_ready"),
                "live_feed_warmup_1m_count": item.get("live_feed_warmup_1m_count"),
                "live_feed_warmup_completed_5m_count": item.get("live_feed_warmup_completed_5m_count"),
                "live_feed_required_1m_count": item.get("live_feed_required_1m_count"),
                "live_feed_required_completed_5m_count": item.get("live_feed_required_completed_5m_count"),
                "feature_context_ready": item.get("feature_context_ready"),
                "feature_context_source": item.get("feature_context_source"),
                "paper_evaluation_allowed": item.get("paper_evaluation_allowed"),
                "live_feed_blocker": item.get("live_feed_blocker"),
                "fresh_for_execution": item.get("fresh_for_execution"),
                "latest_1m_timestamp": item.get("latest_1m_timestamp"),
                "latest_completed_5m_timestamp": item.get("latest_completed_5m_timestamp"),
                "latest_1m_age_seconds": item.get("latest_1m_age_seconds"),
                "latest_completed_5m_age_seconds": item.get("latest_completed_5m_age_seconds"),
                "execution_freshness_blocker": item.get("execution_freshness_blocker"),
                "runtime_candle_age_seconds": item.get("runtime_candle_age_seconds"),
                "runtime_candle_capture_verdict": item.get("runtime_candle_capture_verdict"),
                "runtime_provider_status_category": item.get("runtime_provider_status_category"),
                "monitor_runtime_candle_source": item.get("monitor_runtime_candle_source"),
                "provider_fetch_skipped_for_refresh_cadence": item.get("provider_fetch_skipped_for_refresh_cadence"),
                "provider_fetch_failed_before_fallback": item.get("provider_fetch_failed_before_fallback"),
            }
            for item in instrument_reports
        },
        "enabled_strategies_by_instrument": {
            str(item.get("instrument_family")): item.get("enabled_strategies") or []
            for item in instrument_reports
        },
        "evaluated_strategy_count": sum(int(item.get("evaluated_strategy_count") or 0) for item in instrument_reports),
        "not_ready_strategy_count": sum(int(item.get("not_ready_strategy_count") or 0) for item in instrument_reports),
        "no_signal_strategy_count": sum(int(item.get("no_signal_strategy_count") or 0) for item in instrument_reports),
        "signal_strategy_count": sum(int(item.get("signal_strategy_count") or 0) for item in instrument_reports),
        "strategy_verdicts": all_strategy_verdicts,
        "candidate_signals": [sig for item in instrument_reports for sig in item.get("candidate_signals", [])],
        "suppressed_signals": [sig for item in instrument_reports for sig in item.get("suppressed_signals", [])],
        "arbitration_result": _first_nonempty(item.get("arbitration_result") for item in instrument_reports),
        "chosen_signal": _first_nonempty(item.get("chosen_signal") for item in instrument_reports),
        "latest_signal_strategy_id": _latest_signal_field(instrument_reports, "strategy_id"),
        "latest_signal_side": _latest_signal_field(instrument_reports, "signal_direction"),
        "latest_paper_lifecycle_report_path": _first_nonempty(item.get("paper_runner_report_path") for item in instrument_reports),
        "latest_broker_state_classification": _first_nonempty(
            item.get("latest_broker_state_classification") for item in instrument_reports
        ),
        "latest_trade_ledger_path": (
            paper_trade_summary.get("latest_trade_ledger_path")
            or str(DEFAULT_TRACK_B_PAPER_TRADE_LEDGER_OUTPUT_ROOT / "track_b_paper_trade_ledger.jsonl")
        ),
        "latest_paper_trade_summary_path": (
            paper_trade_summary.get("latest_trade_summary_path") or str(DEFAULT_TRACK_B_PAPER_TRADE_SUMMARY_JSON)
        ),
        "latest_live_position_status_path": (
            live_position_status.get("latest_live_position_status_path")
            or paper_trade_summary.get("latest_live_position_status_path")
            or str(DEFAULT_TRACK_B_LIVE_POSITION_STATUS_JSON)
        ),
        "latest_pnl_summary_path": (
            pnl_summary.get("latest_pnl_summary_path")
            or paper_trade_summary.get("latest_pnl_summary_path")
            or str(DEFAULT_TRACK_B_PNL_SUMMARY_JSON)
        ),
        "latest_startup_readiness_diagnostic_path": str(
            Path(config.diagnostic_output_root) / "latest_track_b_startup_readiness_diagnostic.json"
        ),
        "open_position_count": live_position_status.get("open_position_count", 0),
        "realized_pnl_today": pnl_summary.get("total_realized_pnl_today", "0"),
        "realized_pnl_week": pnl_summary.get("total_realized_pnl_week", "0"),
        "unrealized_pnl": pnl_summary.get("total_unrealized_pnl", "0"),
        "last_trade_strategy": pnl_summary.get("last_trade_strategy"),
        "last_trade_pnl": pnl_summary.get("last_trade_pnl"),
        "review_required_count": pnl_summary.get("review_required_count", paper_trade_summary.get("review_required_count", 0)),
        "paper_results_source": (
            paper_trade_summary.get("source")
            or live_position_status.get("source")
            or pnl_summary.get("source")
            or "NO_TRACK_B_PAPER_TRADES_RECORDED"
        ),
        "paper_results_broker_reconciled": bool(live_position_status.get("broker_reconciled", False)),
        "latest_paper_order_parameters": _first_nonempty(item.get("paper_order_parameters") for item in instrument_reports),
        "latest_paper_order_parameter_blocker": _first_nonempty(
            item.get("paper_order_parameter_blocker") for item in instrument_reports
        ),
        "decision_journal_summary_path": _first_nonempty(item.get("decision_journal_summary_path") for item in instrument_reports),
        "decision_journal_tier_counts": aggregate_tiers,
        "operator_status_path": None,
        "operator_status_verdict": None,
        "dashboard_backend_health": None,
        "submit_allowed": any(item.get("submit_allowed") is True for item in instrument_reports),
        "readiness_invoked": any(item.get("readiness_invoked") is True for item in instrument_reports),
        "paper_proof_invoked": any(item.get("paper_proof_invoked") is True for item in instrument_reports),
        "submit_attempted": any(item.get("submit_attempted") is True for item in instrument_reports),
        "broker_state_mutated": any(item.get("broker_state_mutated") is True for item in instrument_reports),
        "live_money_readiness": any(item.get("live_money_readiness") is True for item in instrument_reports),
        "monitor_verdict": verdict.value,
        "primary_blocker": None if primary_blocker is None else str(primary_blocker),
        "required_next_action": required_next_action,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_shadow_monitor_report.json"),
        "heartbeat_json_path": str(report_json.parent.parent / "latest_track_b_shadow_monitor_heartbeat.json"),
        "submit_path_enabled": False,
        "paper_flags_accepted": False,
        "dashboard_is_observer_only": True,
        "live_money_readiness_permitted": False,
    }


def _finalize_cycle(
    config: TrackBShadowMonitorConfig,
    stages: TrackBShadowMonitorStages,
    report_json: Path,
    report: dict[str, Any],
    *,
    runtime_cycle_report_json: Path | None,
    now: datetime,
) -> dict[str, Any]:
    _write_monitor_report(report_json, report)
    if config.update_operator_status:
        try:
            status = stages.operator_status(config, Path(str(report["latest_report_json_path"])), runtime_cycle_report_json, now)
            report["operator_status_path"] = str(status.report_json)
            report["operator_status_verdict"] = status.report.get("status_verdict")
        except Exception as exc:  # noqa: BLE001
            report["operator_status_path"] = None
            report["operator_status_verdict"] = None
            report["operator_status_error"] = str(exc)
    _write_monitor_report(report_json, report)
    _prune_old_cycles(Path(config.output_root), keep=config.retention_cycles, current_cycle_dir=report_json.parent)
    return report


def _refresh_final_operator_status(
    *,
    config: TrackBShadowMonitorConfig,
    stages: TrackBShadowMonitorStages,
    final_report: dict[str, Any],
    final_report_json: Path,
    now: datetime,
) -> None:
    raw_runtime_cycle_path = final_report.get("multi_strategy_runtime_cycle_report_path")
    runtime_cycle_report_json = Path(str(raw_runtime_cycle_path)) if raw_runtime_cycle_path else None
    try:
        status = stages.operator_status(
            config,
            Path(str(final_report["latest_report_json_path"])),
            runtime_cycle_report_json,
            now,
        )
        final_report["operator_status_path"] = str(status.report_json)
        final_report["operator_status_verdict"] = status.report.get("status_verdict")
    except Exception as exc:  # noqa: BLE001
        final_report["operator_status_error"] = str(exc)
    _write_monitor_report(final_report_json, final_report)


def _write_monitor_report(report_json: Path, report: Mapping[str, Any]) -> None:
    payload = json.dumps(to_jsonable(dict(report)), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report = Path(str(report["latest_report_json_path"]))
    latest_report.parent.mkdir(parents=True, exist_ok=True)
    latest_report.write_text(payload, encoding="utf-8")


def _write_monitor_liveness_diagnostic(
    *,
    config: TrackBShadowMonitorConfig,
    now: datetime,
    lock: TrackBShadowMonitorLock,
    instruments: Sequence[TrackBShadowMonitorInstrumentConfig],
    live_feed_processes: Mapping[str, TrackBLiveFeedProcessState] | None = None,
    last_report: Mapping[str, Any] | None = None,
    sleep_seconds: float | None = None,
    next_wake_at: datetime | None = None,
) -> Path:
    path = Path(config.diagnostic_output_root) / "latest_track_b_monitor_liveness_diagnostic.json"
    latest_report_path = Path(config.output_root) / "latest_track_b_shadow_monitor_report.json"
    latest_heartbeat_path = Path(config.output_root) / "latest_track_b_shadow_monitor_heartbeat.json"
    report_payload = _read_json_optional(latest_report_path) or dict(last_report or {})
    heartbeat_payload = _read_json_optional(latest_heartbeat_path) or {}
    heartbeat_age = _age_seconds_from_payload(heartbeat_payload, now)
    report_age = _age_seconds_from_payload(report_payload, now)
    instrument_statuses = [
        _live_child_artifact_liveness(
            config=config,
            instrument=instrument,
            state=(live_feed_processes or {}).get(instrument.instrument_family),
            now=now,
        )
        for instrument in instruments
    ]
    live_child_artifact_ages = [
        age
        for status in instrument_statuses
        for age in (
            status.get("heartbeat_artifact_age_seconds"),
            status.get("one_minute_artifact_age_seconds"),
            status.get("completed_5m_artifact_age_seconds"),
        )
        if isinstance(age, (int, float))
    ]
    child_artifacts_fresh = any(
        age <= max(float(config.max_latest_1m_age_seconds), config.poll_seconds * 3)
        for age in live_child_artifact_ages
    )
    threshold = max(float(config.poll_seconds) * 3, 60.0)
    now_utc = now.astimezone(UTC)
    sleeping_expected = next_wake_at is not None and now_utc < next_wake_at.astimezone(UTC)
    classification = "MONITOR_LIVE"
    if sleeping_expected:
        classification = "MONITOR_SLEEPING_EXPECTED"
    elif child_artifacts_fresh and (
        (heartbeat_age is not None and heartbeat_age > threshold)
        or (report_age is not None and report_age > threshold)
    ):
        classification = "CHILDREN_ADVANCING_MONITOR_STALE"
    elif heartbeat_age is not None and heartbeat_age > threshold:
        classification = "MONITOR_HEARTBEAT_STALE"
    elif report_age is not None and report_age > threshold:
        classification = "MONITOR_REPORT_STALE"
    elif _blocker_mentions_5m(report_payload.get("primary_blocker")):
        classification = "BLOCKED_ON_5M_AGGREGATION"
    elif _blocker_mentions_startup(report_payload.get("primary_blocker")):
        classification = "BLOCKED_ON_STARTUP_READINESS"

    payload = {
        "schema_version": "track_b_monitor_liveness_diagnostic_v1",
        "generated_at": now_utc.isoformat(),
        "monitor_pid": lock.owner.get("pid"),
        "monitor_id": lock.owner.get("monitor_id"),
        "monitor_mode": _monitor_mode(config),
        "runtime_decision_source": _runtime_data_source(config).value,
        "last_heartbeat_timestamp": _first_text(
            heartbeat_payload.get("generated_at"),
            heartbeat_payload.get("completed_at"),
            heartbeat_payload.get("wall_clock_time"),
        ),
        "heartbeat_age_seconds": None if heartbeat_age is None else round(heartbeat_age, 3),
        "last_report_timestamp": _first_text(
            report_payload.get("completed_at"),
            report_payload.get("generated_at"),
            report_payload.get("wall_clock_time"),
        ),
        "report_age_seconds": None if report_age is None else round(report_age, 3),
        "last_cycle_number": report_payload.get("cycle_index") or heartbeat_payload.get("cycle_index"),
        "expected_cycle_interval_seconds": config.poll_seconds,
        "sleep_seconds": None if sleep_seconds is None else round(float(sleep_seconds), 3),
        "next_wake_at": None if next_wake_at is None else next_wake_at.astimezone(UTC).isoformat(),
        "last_branch_outcome": report_payload.get("monitor_verdict") or heartbeat_payload.get("last_monitor_verdict"),
        "current_blocker": report_payload.get("primary_blocker") or heartbeat_payload.get("current_blocker"),
        "live_child_statuses": instrument_statuses,
        "live_child_pids": {
            instrument.instrument_family: (live_feed_processes or {}).get(instrument.instrument_family).pid
            for instrument in instruments
            if (live_feed_processes or {}).get(instrument.instrument_family) is not None
        },
        "live_child_artifacts_advancing": child_artifacts_fresh,
        "monitor_artifact_advancing": (
            (heartbeat_age is not None and heartbeat_age <= threshold)
            or (report_age is not None and report_age <= threshold)
        ),
        "suspected_stall_classification": classification,
        "submit_allowed": False,
        "submit_attempted": False,
        "paper_proof_invoked": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "report_json_path": str(path),
    }
    _write_json_file(path, payload)
    return path


def _live_child_artifact_liveness(
    *,
    config: TrackBShadowMonitorConfig,
    instrument: TrackBShadowMonitorInstrumentConfig,
    state: TrackBLiveFeedProcessState | None,
    now: datetime,
) -> dict[str, Any]:
    root = Path(config.live_runtime_feed_output_root)
    heartbeat_path = _latest_live_heartbeat_path(root, instrument.instrument_family)
    one_minute_path = _latest_live_1m_path(root, instrument.instrument_family)
    completed_5m_path = _latest_live_completed_5m_path(root, instrument.instrument_family)
    heartbeat_payload = _read_json_optional(heartbeat_path)
    one_minute_payload = _read_json_optional(one_minute_path)
    completed_5m_payload = _read_json_optional(completed_5m_path)
    completed_reason = _completed_5m_unavailable_reason(completed_5m_path, completed_5m_payload)
    return {
        "instrument_family": instrument.instrument_family,
        "live_feed_pid": None if state is None else state.pid,
        "live_feed_status": None if state is None else state.status,
        "heartbeat_path": str(heartbeat_path),
        "heartbeat_exists": heartbeat_path.exists(),
        "heartbeat_artifact_age_seconds": _path_mtime_age_seconds(heartbeat_path, now),
        "heartbeat_generated_age_seconds": _age_seconds_from_payload(heartbeat_payload, now),
        "one_minute_path": str(one_minute_path),
        "one_minute_exists": one_minute_path.exists(),
        "one_minute_artifact_age_seconds": _path_mtime_age_seconds(one_minute_path, now),
        "latest_1m_timestamp": _first_text(
            (one_minute_payload or {}).get("latest_1m_timestamp"),
            (heartbeat_payload or {}).get("latest_1m_timestamp"),
        ),
        "completed_5m_path": str(completed_5m_path),
        "completed_5m_exists": completed_5m_path.exists(),
        "completed_5m_artifact_age_seconds": _path_mtime_age_seconds(completed_5m_path, now),
        "completed_5m_unavailable_reason": completed_reason,
        "latest_completed_5m_timestamp": _latest_candle_timestamp(completed_5m_payload)
        or _first_text((heartbeat_payload or {}).get("latest_completed_5m_timestamp")),
        "completed_5m_bar_count": _bars_available(completed_5m_payload),
    }


def _path_mtime_age_seconds(path: Path, now: datetime) -> float | None:
    try:
        mtime = datetime.fromtimestamp(path.stat().st_mtime, tz=UTC)
    except FileNotFoundError:
        return None
    return round(max(0.0, (now.astimezone(UTC) - mtime).total_seconds()), 3)


def _latest_candle_timestamp(payload: Mapping[str, Any] | None) -> str | None:
    candles = _payload_candles(payload or {})
    if not candles:
        return None
    for candle in reversed(candles):
        text = _first_text(candle.get("candle_timestamp"), candle.get("timestamp"), candle.get("observed_at"))
        if text:
            try:
                return _parse_time(text).isoformat()
            except ValueError:
                return None
    return None


def _completed_5m_unavailable_reason(path: Path, payload: Mapping[str, Any] | None) -> str | None:
    if not path.exists():
        return "artifact missing"
    if payload is None:
        return "timestamp parse failure or unreadable JSON"
    candles = _payload_candles(payload)
    if not candles:
        return "no completed bars yet"
    latest = _latest_candle_timestamp(payload)
    if not latest:
        return "timestamp parse failure"
    return None


def _blocker_mentions_5m(value: object) -> bool:
    return "5m" in str(value or "").lower()


def _blocker_mentions_startup(value: object) -> bool:
    text = str(value or "").lower()
    return "startup" in text or "feature context" in text or "confirmation window" in text


def _write_heartbeat(
    *,
    config: TrackBShadowMonitorConfig,
    monitor_id: str,
    cycle_id: str,
    cycle_index: int,
    generated_at: datetime,
    monitor_running: bool,
    instruments: Sequence[TrackBShadowMonitorInstrumentConfig],
    last_verdict: str | None,
    last_report_path: Path | None,
    lock: TrackBShadowMonitorLock,
    current_blocker: object | None = None,
    sleep_seconds: float | None = None,
    next_wake_at: datetime | None = None,
) -> None:
    path = Path(config.output_root) / "latest_track_b_shadow_monitor_heartbeat.json"
    payload = {
        "schema_version": "track_b_shadow_monitor_heartbeat_v2",
        "generated_at": generated_at.astimezone(UTC).isoformat(),
        "monitor_id": monitor_id,
        "cycle_id": cycle_id,
        "cycle_index": cycle_index,
        "mode": _monitor_mode(config),
        "monitor_mode": _monitor_mode(config),
        "monitor_running": monitor_running,
        "pid": lock.owner.get("pid"),
        "host": lock.owner.get("host"),
        "lockfile_path": str(lock.lockfile),
        "pidfile_path": str(lock.pidfile),
        "instrument_families": [instrument.instrument_family for instrument in instruments],
        "last_monitor_verdict": last_verdict,
        "last_report_path": None if last_report_path is None else str(last_report_path),
        "current_blocker": None if current_blocker is None else str(current_blocker),
        "sleep_seconds": None if sleep_seconds is None else round(float(sleep_seconds), 3),
        "next_wake_at": None if next_wake_at is None else next_wake_at.astimezone(UTC).isoformat(),
        "expected_cycle_interval_seconds": config.poll_seconds,
        "heartbeat_json_path": str(path),
        "submit_allowed": False,
        "submit_attempted": False,
        "paper_proof_invoked": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
    }
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(payload), indent=2, sort_keys=True), encoding="utf-8")


def _write_final_heartbeat(
    *,
    config: TrackBShadowMonitorConfig,
    monitor_id: str,
    generated_at: datetime,
    last_report_path: Path | None,
    last_verdict: str | None,
    shutdown_reason: str | None,
    lock: TrackBShadowMonitorLock,
) -> None:
    path = Path(config.output_root) / "latest_track_b_shadow_monitor_heartbeat.json"
    prior = _read_json_optional(path) or {}
    prior.update(
        {
            "generated_at": generated_at.astimezone(UTC).isoformat(),
            "monitor_id": monitor_id,
            "mode": _monitor_mode(config),
            "monitor_mode": _monitor_mode(config),
            "monitor_running": False,
            "shutdown_reason": shutdown_reason or "max_cycles_completed_or_stopped",
            "last_monitor_verdict": last_verdict,
            "last_report_path": None if last_report_path is None else str(last_report_path),
            "pid": lock.owner.get("pid"),
            "host": lock.owner.get("host"),
            "submit_allowed": False,
            "submit_attempted": False,
            "paper_proof_invoked": False,
            "broker_state_mutated": False,
            "live_money_readiness": False,
        }
    )
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(prior), indent=2, sort_keys=True), encoding="utf-8")


def _verdict_for_runtime_cycle(
    report: Mapping[str, Any],
    critical_blocker: str | None,
) -> TrackBShadowMonitorVerdict:
    if critical_blocker:
        return TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG
    verdict = str(report.get("multi_strategy_runtime_cycle_verdict") or "")
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.SIGNAL_READY_NO_SUBMIT.value:
        return TrackBShadowMonitorVerdict.OK_SIGNAL_READY_NO_SUBMIT
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.NO_SIGNAL_NO_MUTATION.value:
        return TrackBShadowMonitorVerdict.OK_NO_SIGNAL
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_PASSED.value:
        return TrackBShadowMonitorVerdict.PAPER_PROOF_PASSED
    if verdict == TrackBMultiStrategyRuntimeCycleVerdict.PAPER_PROOF_REVIEW_REQUIRED.value:
        return TrackBShadowMonitorVerdict.PAPER_PROOF_REVIEW_REQUIRED
    return TrackBShadowMonitorVerdict.BLOCKED_PRODUCER_ERROR


def _cycle_verdict(instrument_reports: Sequence[Mapping[str, Any]]) -> TrackBShadowMonitorVerdict:
    verdicts = {str(report.get("instrument_verdict") or "") for report in instrument_reports}
    if TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG.value in verdicts:
        return TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG
    if TrackBShadowMonitorVerdict.OK_SIGNAL_READY_NO_SUBMIT.value in verdicts:
        return TrackBShadowMonitorVerdict.OK_SIGNAL_READY_NO_SUBMIT
    if TrackBShadowMonitorVerdict.PAPER_PROOF_REVIEW_REQUIRED.value in verdicts:
        return TrackBShadowMonitorVerdict.PAPER_PROOF_REVIEW_REQUIRED
    if TrackBShadowMonitorVerdict.PAPER_PROOF_PASSED.value in verdicts:
        return TrackBShadowMonitorVerdict.PAPER_PROOF_PASSED
    if TrackBShadowMonitorVerdict.BLOCKED_PROVIDER_ERROR.value in verdicts:
        return TrackBShadowMonitorVerdict.BLOCKED_PROVIDER_ERROR
    if TrackBShadowMonitorVerdict.LIVE_FEED_DISCONNECTED.value in verdicts:
        return TrackBShadowMonitorVerdict.LIVE_FEED_DISCONNECTED
    if TrackBShadowMonitorVerdict.LIVE_FEED_STALE.value in verdicts:
        return TrackBShadowMonitorVerdict.LIVE_FEED_STALE
    if TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP.value in verdicts:
        return TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP
    if TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY.value in verdicts:
        return TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY
    if TrackBShadowMonitorVerdict.BLOCKED_PRODUCER_ERROR.value in verdicts:
        return TrackBShadowMonitorVerdict.BLOCKED_PRODUCER_ERROR
    if TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT.value in verdicts:
        return TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT
    if TrackBShadowMonitorVerdict.OK_NO_SIGNAL.value in verdicts:
        return TrackBShadowMonitorVerdict.OK_NO_SIGNAL
    if TrackBShadowMonitorVerdict.HEARTBEAT_NO_NEW_COMPLETED_BAR.value in verdicts:
        return TrackBShadowMonitorVerdict.HEARTBEAT_NO_NEW_COMPLETED_BAR
    return TrackBShadowMonitorVerdict.NOT_READY_NO_STRATEGIES_CONFIGURED


def _critical_mutation_flag(report: Mapping[str, Any], *, config: TrackBShadowMonitorConfig) -> str | None:
    if report.get("live_money_readiness") is True:
        return "Unexpected monitor safety flag live_money_readiness=true."
    if _monitor_mode(config) == "SHADOW":
        for key in ("submit_allowed", "submit_attempted", "paper_proof_invoked", "broker_state_mutated"):
            if report.get(key) is True:
                return f"Unexpected SHADOW mutation/safety flag {key}=true."
        return None
    if report.get("submit_attempted") is True and not _has_guarded_paper_lifecycle_provenance(report):
        return "PAPER submit_attempted=true without guarded lifecycle provenance."
    if report.get("broker_state_mutated") is True and not _has_guarded_paper_lifecycle_provenance(report):
        return "PAPER broker_state_mutated=true outside guarded lifecycle provenance."
    if report.get("paper_proof_invoked") is True and not _has_guarded_paper_lifecycle_provenance(report):
        return "PAPER paper_proof_invoked=true without guarded lifecycle provenance."
    return None


def _global_critical_blocker(
    instrument_reports: Sequence[Mapping[str, Any]],
    *,
    config: TrackBShadowMonitorConfig,
) -> str | None:
    for report in instrument_reports:
        blocker = _critical_mutation_flag(report, config=config)
        if blocker:
            return f"GLOBAL_SAFETY_BLOCKER: {blocker} Instrument={report.get('instrument_family')}."
    return None


def _has_guarded_paper_lifecycle_provenance(report: Mapping[str, Any]) -> bool:
    return bool(report.get("paper_runner_report_path") and report.get("paper_proof_classification"))


def _must_stop(report: Mapping[str, Any], config: TrackBShadowMonitorConfig) -> bool:
    if report.get("monitor_verdict") == TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG.value:
        return True
    if (
        _monitor_mode(config) == "PAPER"
        and config.pause_after_paper_trade
        and int(report.get("paper_trades_attempted_count") or 0) > int(config.paper_trades_attempted_count or 0)
    ):
        return True
    if _monitor_mode(config) == "PAPER" and int(report.get("paper_trades_attempted_count") or 0) >= config.max_paper_trades_per_run:
        return True
    if config.stop_on_error and report.get("monitor_verdict") in {
        TrackBShadowMonitorVerdict.BLOCKED_PROVIDER_ERROR.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_STALE.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_DISCONNECTED.value,
        TrackBShadowMonitorVerdict.BLOCKED_PRODUCER_ERROR.value,
        TrackBShadowMonitorVerdict.ERROR.value,
    }:
        return True
    return False


def _paper_trade_attempt_count_delta(instrument_reports: Sequence[Mapping[str, Any]]) -> int:
    return sum(1 for report in instrument_reports if report.get("paper_proof_invoked") is True)


def _failure_counts_for_backoff(report: Mapping[str, Any]) -> bool:
    return str(report.get("monitor_verdict") or "") in {
        TrackBShadowMonitorVerdict.BLOCKED_PROVIDER_ERROR.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_STALE.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_DISCONNECTED.value,
        TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT.value,
        TrackBShadowMonitorVerdict.BLOCKED_PRODUCER_ERROR.value,
        TrackBShadowMonitorVerdict.ERROR.value,
    }


def _sleep_seconds(*, config: TrackBShadowMonitorConfig, consecutive_failures: int) -> float:
    if consecutive_failures <= 0:
        return config.poll_seconds
    multiplier = 2 ** min(consecutive_failures - 1, 4)
    return min(config.max_backoff_seconds, config.poll_seconds * multiplier)


def _safe_quote_payload(path: Path | None) -> dict[str, object]:
    if path is not None and path.exists():
        try:
            return _read_quote_payload(path)
        except Exception:  # noqa: BLE001
            pass
    return {
        "quote_provider_mode": "NOT_PROVIDED",
        "realtime_quote_received": False,
        "current_quote_available": False,
        "quote_freshness_verdict": "NOT_PROVIDED",
        "report_json_path": None if path is None else str(path),
    }


def _provider_failure_category(report: Mapping[str, Any]) -> str:
    if not report:
        return "NOT_PROVIDED"
    if report.get("provider_credential_status") == "MISSING":
        return "PROVIDER_CREDENTIAL_MISSING"
    if report.get("data_written") is True and report.get("fresh_for_execution") is True:
        return "DATA_WRITTEN_EXECUTION_FRESH"
    if report.get("data_written") is True and report.get("fresh_for_execution") is not True:
        return "DATA_WRITTEN_NOT_EXECUTION_FRESH"
    blocker = str(report.get("primary_blocker") or report.get("execution_freshness_blocker") or "").lower()
    if "timed out" in blocker or "timeout" in blocker:
        return "PROVIDER_TIMEOUT"
    if "available_end" in blocker or "available end" in blocker or report.get("provider_available_end"):
        return "PROVIDER_STALE_AVAILABLE_END"
    if "no data" in blocker or "insufficient" in blocker or "at least" in blocker:
        return "PROVIDER_RETURNED_NO_DATA"
    verdict = str(report.get("runtime_candle_capture_verdict") or "")
    if "FETCH_FAILED" in verdict or "PROVIDER_ERROR" in verdict:
        return "PROVIDER_ERROR"
    return "UNKNOWN"


class _ProviderFetchTimeout(TimeoutError):
    pass


class _provider_fetch_deadline:
    def __init__(self, timeout_seconds: float | None) -> None:
        self.timeout_seconds = timeout_seconds
        self.previous_handler: object | None = None
        self.previous_alarm = 0
        self.enabled = False

    def __enter__(self) -> None:
        if self.timeout_seconds is None or self.timeout_seconds <= 0:
            return
        if not hasattr(signal, "SIGALRM"):
            return
        if signal.getsignal(signal.SIGALRM) == self._handle_timeout:
            return
        self.previous_handler = signal.getsignal(signal.SIGALRM)
        self.previous_alarm = signal.alarm(0)
        signal.signal(signal.SIGALRM, self._handle_timeout)
        signal.setitimer(signal.ITIMER_REAL, float(self.timeout_seconds))
        self.enabled = True

    def __exit__(self, exc_type: object, exc: object, traceback: object) -> bool:
        if self.enabled:
            signal.setitimer(signal.ITIMER_REAL, 0)
            if self.previous_handler is not None:
                signal.signal(signal.SIGALRM, self.previous_handler)  # type: ignore[arg-type]
            if self.previous_alarm > 0:
                signal.alarm(self.previous_alarm)
        return False

    def _handle_timeout(self, _signum: int, _frame: object) -> None:
        raise _ProviderFetchTimeout("Databento runtime candle fetch exceeded monitor provider timeout.")


def _read_json_required(path: Path | None) -> dict[str, Any]:
    if path is None:
        raise ValueError("Required JSON path was not provided.")
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value


def _read_json_optional(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _write_json_file(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(to_jsonable(dict(payload)), indent=2, sort_keys=True), encoding="utf-8")


def _age_seconds_from_payload(payload: Mapping[str, Any] | None, now: datetime) -> float | None:
    if not payload:
        return None
    raw = payload.get("generated_at") or payload.get("completed_at") or payload.get("wall_clock_time")
    if not raw:
        return None
    try:
        generated_at = _parse_time(str(raw))
    except ValueError:
        return None
    return max(0.0, (now.astimezone(UTC) - generated_at.astimezone(UTC)).total_seconds())


def _bars_available(payload: Mapping[str, Any] | None) -> int:
    if not payload:
        return 0
    for key in ("bars_available", "completed_5m_bar_count", "valid_ohlcv_1m_record_count"):
        value = _int_or_none(payload.get(key))
        if value is not None:
            return value
    candles = payload.get("candles") or payload.get("candle_history") or payload.get("bars")
    return len(candles) if isinstance(candles, Sequence) and not isinstance(candles, (str, bytes)) else 0


def _bool_or_none(value: object) -> bool | None:
    if isinstance(value, bool):
        return value
    if value is None:
        return None
    text = str(value).strip().lower()
    if text in {"true", "1", "yes"}:
        return True
    if text in {"false", "0", "no"}:
        return False
    return None


def _first_bool(*values: object) -> bool | None:
    for value in values:
        parsed = _bool_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _float_or_none(value: object) -> float | None:
    try:
        return float(str(value))
    except (TypeError, ValueError):
        return None


def _first_float(*values: object) -> float | None:
    for value in values:
        parsed = _float_or_none(value)
        if parsed is not None:
            return parsed
    return None


def _first_text(*values: object) -> str | None:
    for value in values:
        text = str(value or "").strip()
        if text:
            return text
    return None


def _live_feed_artifact_matches(
    *,
    report: Mapping[str, Any] | None,
    event: Mapping[str, Any] | None,
    instrument: TrackBShadowMonitorInstrumentConfig,
) -> str | None:
    payload = report or event or {}
    if not payload:
        return None
    expected = {
        "contract_key": instrument.contract_key,
        "local_symbol": instrument.local_symbol,
        "databento_continuous_symbol": instrument.databento_continuous_symbol,
        "dataset": instrument.dataset,
    }
    for key, expected_value in expected.items():
        observed = payload.get(key)
        if observed is not None and str(observed) != str(expected_value):
            return f"Databento Live feed artifact {key} mismatch: expected {expected_value}, observed {observed}."
    return None


def _path_from_report(report: Mapping[str, Any], key: str) -> Path | None:
    value = report.get(key)
    return Path(str(value)) if value else None


def _existing_optional_path(path: Path | None) -> Path | None:
    return path if path is not None and path.exists() else None


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _pid_is_alive(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    return True


def _int_or_none(value: object) -> int | None:
    try:
        return int(str(value))
    except (TypeError, ValueError):
        return None


def _lock_held_report(
    *,
    config: TrackBShadowMonitorConfig,
    monitor_id: str,
    lock: TrackBShadowMonitorLock,
    now: datetime,
) -> dict[str, Any]:
    report_json = Path(config.output_root) / f"{monitor_id}_lock_held" / "track_b_shadow_monitor_report.json"
    return {
        "schema_version": "track_b_shadow_monitor_v2",
        "monitor_schema_version": "track_b_shadow_monitor_v2",
        "monitor_id": monitor_id,
        "cycle_id": f"{monitor_id}_lock_held",
        "mode": _monitor_mode(config),
        "monitor_mode": _monitor_mode(config),
        "started_at": now.astimezone(UTC).isoformat(),
        "completed_at": now.astimezone(UTC).isoformat(),
        "cycle_elapsed_seconds": 0,
        "monitor_verdict": TrackBShadowMonitorVerdict.LOCK_HELD.value,
        "primary_blocker": lock.blocker,
        "required_next_action": "Stop the existing monitor or restart with --force-takeover after confirming stale ownership.",
        "lock_owner": lock.owner,
        "lockfile_path": str(lock.lockfile),
        "pidfile_path": str(lock.pidfile),
        "instrument_reports": [],
        "submit_allowed": False,
        "submit_attempted": False,
        "paper_proof_invoked": False,
        "broker_state_mutated": False,
        "live_money_readiness": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_shadow_monitor_report.json"),
        "heartbeat_json_path": str(report_json.parent.parent / "latest_track_b_shadow_monitor_heartbeat.json"),
    }


def _primary_blocker(instrument_reports: Sequence[Mapping[str, Any]]) -> str | None:
    priority = (
        TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG.value,
        TrackBShadowMonitorVerdict.BLOCKED_PROVIDER_ERROR.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_DISCONNECTED.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_STALE.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP.value,
        TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY.value,
        TrackBShadowMonitorVerdict.BLOCKED_PRODUCER_ERROR.value,
        TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT.value,
        TrackBShadowMonitorVerdict.OK_SIGNAL_READY_NO_SUBMIT.value,
    )
    for verdict in priority:
        for report in instrument_reports:
            if report.get("instrument_verdict") == verdict and report.get("primary_blocker"):
                return str(report["primary_blocker"])
    for report in instrument_reports:
        blocker = report.get("primary_blocker")
        if blocker:
            return str(blocker)
    return None


def _required_next_action(verdict: TrackBShadowMonitorVerdict) -> str:
    if verdict == TrackBShadowMonitorVerdict.CRITICAL_UNEXPECTED_MUTATION_FLAG:
        return "Stop SHADOW monitor and inspect Track B safety fields before continuing."
    if verdict == TrackBShadowMonitorVerdict.BLOCKED_PROVIDER_ERROR:
        return "Repair provider/runtime candle capture; monitor may continue with bounded backoff."
    if verdict == TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY:
        return "Start or repair Databento Live runtime feed; do not evaluate strategies from HTTP backfill as live."
    if verdict == TrackBShadowMonitorVerdict.LIVE_FEED_WARMING_UP:
        return "Keep Databento Live feed running until required runtime bars are accumulated."
    if verdict == TrackBShadowMonitorVerdict.LIVE_FEED_STALE:
        return "Reconnect or wait for fresh Databento Live heartbeat before strategy evaluation."
    if verdict == TrackBShadowMonitorVerdict.LIVE_FEED_DISCONNECTED:
        return "Restart Databento Live feed; monitor may retry with bounded backoff."
    if verdict == TrackBShadowMonitorVerdict.NOT_READY_STALE_RUNTIME_CONTEXT:
        return "Wait for fresh runtime candles; do not evaluate strategies on stale context."
    if verdict == TrackBShadowMonitorVerdict.OK_SIGNAL_READY_NO_SUBMIT:
        return "Review signal artifact; SHADOW monitor continues without submit authority."
    if verdict == TrackBShadowMonitorVerdict.PAPER_PROOF_PASSED:
        return "Guarded PAPER lifecycle completed; pause/stop according to monitor paper policy."
    if verdict == TrackBShadowMonitorVerdict.PAPER_PROOF_REVIEW_REQUIRED:
        return "Review guarded PAPER lifecycle classification before re-arming PAPER mode."
    return "Continue Track B SHADOW monitoring."


def _first_nonempty(values: Sequence[object] | Any) -> object:
    for value in values:
        if value:
            return value
    return {}


def _latest_signal_field(instrument_reports: Sequence[Mapping[str, Any]], field: str) -> object:
    for report in instrument_reports:
        chosen = report.get("chosen_signal")
        if isinstance(chosen, Mapping) and chosen.get(field):
            return chosen.get(field)
    for report in instrument_reports:
        candidates = report.get("candidate_signals") or []
        if isinstance(candidates, Sequence) and not isinstance(candidates, (str, bytes)):
            for candidate in candidates:
                if isinstance(candidate, Mapping) and candidate.get(field):
                    return candidate.get(field)
    return None


def _aggregate_tier_counts(instrument_reports: Sequence[Mapping[str, Any]]) -> dict[str, int]:
    totals: dict[str, int] = {}
    for report in instrument_reports:
        counts = report.get("decision_journal_tier_counts") or {}
        if isinstance(counts, Mapping):
            for key, value in counts.items():
                totals[str(key)] = totals.get(str(key), 0) + int(value or 0)
    return totals


def _count_strategy_verdicts(strategy_verdicts: Sequence[Mapping[str, Any]], needle: str) -> int:
    return sum(1 for item in strategy_verdicts if needle in str(item.get("strategy_runtime_verdict") or ""))


def _prune_old_cycles(output_root: Path, *, keep: int, current_cycle_dir: Path) -> None:
    if keep <= 0:
        return
    output_root.mkdir(parents=True, exist_ok=True)
    cycle_dirs = [
        item
        for item in output_root.iterdir()
        if item.is_dir() and item.name.startswith("track_b_shadow_monitor_")
    ]
    cycle_dirs.sort(key=lambda item: item.stat().st_mtime, reverse=True)
    retained = 0
    for item in cycle_dirs:
        if item == current_cycle_dir or retained < keep:
            retained += 1
            continue
        for child in item.iterdir():
            child.unlink()
        item.rmdir()
