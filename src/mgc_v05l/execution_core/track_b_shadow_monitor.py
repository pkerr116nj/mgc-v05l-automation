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
    strategy_ready: bool
    warmup_1m_count: int
    warmup_completed_5m_count: int
    required_1m_count: int
    required_completed_5m_count: int
    blocker: str | None
    report_path: Path | None
    heartbeat_path: Path | None
    event_path: Path | None
    completed_5m_path: Path | None


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
            contract_key="MNQ-NOT_CONFIGURED",
            local_symbol="MNQ",
            databento_continuous_symbol="MNQ.v.0",
            dataset="GLBX.MDP3",
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
            )
            if should_stop:
                break
            if cycle_index < config.max_cycles:
                sleep_seconds = _sleep_seconds(config=config, consecutive_failures=consecutive_failures)
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
        if not live_feed_readiness.strategy_ready:
            report = _instrument_report_base(
                instrument=instrument,
                verdict=live_feed_readiness.verdict or TrackBShadowMonitorVerdict.LIVE_FEED_NOT_READY,
                primary_blocker=live_feed_readiness.blocker or "Databento Live feed is not strategy-ready.",
                required_next_action="Keep the managed Databento Live feed running until strategy_ready=true.",
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
    event_path = root / "latest_live_mgc_1m_candles.json"
    completed_path = root / "latest_live_mgc_completed_5m_candles.json"
    report_path = root / "latest_databento_live_runtime_feed_report.json"
    heartbeat_path = root / "latest_databento_live_runtime_feed_heartbeat.json"
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
    strategy_ready = (
        artifact_matches is None
        and heartbeat_fresh
        and live_connected is True
        and bool((heartbeat or report or {}).get("fresh_for_execution")) is True
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
    elif warmup_1m_count < required_1m_count or warmup_completed_5m_count < required_completed_5m_count:
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
                "Databento Live feed is warming up: "
                f"1m={warmup_1m_count}/{required_1m_count}, "
                f"completed_5m={warmup_completed_5m_count}/{required_completed_5m_count}."
            )
    elif bool((heartbeat or report or {}).get("fresh_for_execution")) is not True:
        status = "LIVE_FEED_STALE"
        verdict = TrackBShadowMonitorVerdict.LIVE_FEED_STALE
        blocker = "Databento Live feed artifacts are present but not fresh_for_execution=true."
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
        strategy_ready=strategy_ready,
        warmup_1m_count=warmup_1m_count,
        warmup_completed_5m_count=warmup_completed_5m_count,
        required_1m_count=required_1m_count,
        required_completed_5m_count=required_completed_5m_count,
        blocker=blocker,
        report_path=report_path if report_path.exists() else None,
        heartbeat_path=heartbeat_path if heartbeat_path.exists() else None,
        event_path=event_path if event_path.exists() else None,
        completed_5m_path=completed_path if completed_path.exists() else None,
    )


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
    stdout = open(root / "track_b_live_feed_stdout.log", "ab", buffering=0)  # noqa: SIM115 - Popen needs file handles.
    stderr = open(root / "track_b_live_feed_stderr.log", "ab", buffering=0)  # noqa: SIM115
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
    live_event_json = Path(config.live_runtime_feed_output_root) / "latest_live_mgc_1m_candles.json"
    live_report_json = Path(config.live_runtime_feed_output_root) / "latest_databento_live_runtime_feed_report.json"
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
    result = capture_track_b_runtime_mgc_1m_candles(
        runtime_candle_payload=payload,
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
    _annotate_live_runtime_result(result, live_report_json=live_report_json, live_event_json=live_event_json, live_report=live_report)
    return result


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
        fallback_candidate_report_json = Path(config.runtime_candle_capture_output_root) / "latest_runtime_candle_capture_report.json"
        fallback_candidate_event_json = Path(config.runtime_candle_capture_output_root) / "latest_runtime_mgc_1m_candles.json"
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
    event_json = Path(config.runtime_candle_capture_output_root) / "latest_runtime_mgc_1m_candles.json"
    report_json = Path(config.runtime_candle_capture_output_root) / "latest_runtime_candle_capture_report.json"
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
    actual_report_json = latest_report_json or Path(config.runtime_candle_capture_output_root) / "latest_runtime_candle_capture_report.json"
    actual_event_json = latest_event_json or Path(config.runtime_candle_capture_output_root) / "latest_runtime_mgc_1m_candles.json"
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
            asian_drift_event_json=_path_from_report(asian.report, "asian_drift_state_snapshot_path"),
            pause_resume_short_event_json=session.asia_early_pause_resume_short_event_json,
            breakout_retest_hold_long_event_json=session.asia_early_normal_breakout_retest_hold_long_event_json,
            first_bull_snap_turn_event_json=snap.first_bull_snap_turn_event_json,
            first_bear_snap_turn_event_json=snap.first_bear_snap_turn_event_json,
            london_late_pause_resume_short_event_json=session.london_late_pause_resume_short_event_json,
            asia_late_flat_pullback_pause_resume_long_event_json=session.asia_late_flat_pullback_pause_resume_long_event_json,
            us_derivative_bear_turn_event_json=session.us_derivative_bear_turn_event_json,
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
            pricing_context_json=Path(config.runtime_candle_capture_output_root) / "latest_runtime_mgc_1m_candles.json",
            live_quote_report_json=Path(config.live_runtime_feed_output_root) / "latest_live_quote_status_report.json",
            max_pricing_context_age_seconds=config.max_latest_1m_age_seconds,
            tick_size=config.tick_size,
            allowlisted_local_symbol=instrument.local_symbol,
            con_id=config.con_id,
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
        "live_feed_strategy_ready": None if live_feed is None else live_feed.strategy_ready,
        "live_feed_warmup_1m_count": None if live_feed is None else live_feed.warmup_1m_count,
        "live_feed_warmup_completed_5m_count": None if live_feed is None else live_feed.warmup_completed_5m_count,
        "live_feed_required_1m_count": None if live_feed is None else live_feed.required_1m_count,
        "live_feed_required_completed_5m_count": None if live_feed is None else live_feed.required_completed_5m_count,
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
            "live_feed_strategy_ready": base.get("live_feed_strategy_ready"),
            "live_feed_warmup_1m_count": base.get("live_feed_warmup_1m_count"),
            "live_feed_warmup_completed_5m_count": base.get("live_feed_warmup_completed_5m_count"),
            "live_feed_required_1m_count": base.get("live_feed_required_1m_count"),
            "live_feed_required_completed_5m_count": base.get("live_feed_required_completed_5m_count"),
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
                "live_feed_strategy_ready": item.get("live_feed_strategy_ready"),
                "live_feed_warmup_1m_count": item.get("live_feed_warmup_1m_count"),
                "live_feed_warmup_completed_5m_count": item.get("live_feed_warmup_completed_5m_count"),
                "live_feed_required_1m_count": item.get("live_feed_required_1m_count"),
                "live_feed_required_completed_5m_count": item.get("live_feed_required_completed_5m_count"),
                "live_feed_blocker": item.get("live_feed_blocker"),
                "fresh_for_execution": item.get("fresh_for_execution"),
                "latest_1m_timestamp": item.get("latest_1m_timestamp"),
                "latest_completed_5m_timestamp": item.get("latest_completed_5m_timestamp"),
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
            return f"{blocker} Instrument={report.get('instrument_family')}."
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
