"""Track B no-submit observation runner.

This is an operator convenience wrapper around existing Track B no-submit
boundaries. It produces artifacts for observation only; it never submits,
connects to IBKR/TWS, calls paper_proof_cli, or creates execution authority.
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from time import sleep
from typing import Any, Mapping

from .databento_candle_observer import (
    DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT,
    DatabentoCandleObserverResult,
    DatabentoCandleObserverVerdict,
    observe_databento_candle_event,
)
from .models import require_aware_datetime, to_jsonable
from .operator_status import DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT, OperatorStatusInputs, OperatorStatusResult, create_operator_status_summary
from .shadow_listener import DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT, ShadowListenerResult, run_shadow_listener_cycle
from .strategy_signal_adapter import (
    DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT,
    StrategySignalAdapterResult,
    StrategySignalAdapterVerdict,
    adapt_demo_candle_direction_signal,
)


DEFAULT_TRACK_B_OBSERVATION_RUNNER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_observation_runner")


class TrackBObservationRunnerVerdict(str, Enum):
    COMPLETED_FOR_REVIEW = "TRACK_B_OBSERVATION_RUNNER_COMPLETED_FOR_REVIEW"
    COMPLETED_WITH_BLOCKERS = "TRACK_B_OBSERVATION_RUNNER_COMPLETED_WITH_BLOCKERS"
    BLOCKED_DATABENTO_OBSERVER = "TRACK_B_OBSERVATION_RUNNER_BLOCKED_DATABENTO_OBSERVER"
    BLOCKED_STRATEGY_ADAPTER = "TRACK_B_OBSERVATION_RUNNER_BLOCKED_STRATEGY_ADAPTER"
    BLOCKED_LISTENER = "TRACK_B_OBSERVATION_RUNNER_BLOCKED_LISTENER"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_OBSERVATION_RUNNER_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class TrackBObservationRunnerResult:
    verdict: TrackBObservationRunnerVerdict
    report_json: Path
    report: dict[str, Any]


@dataclass(frozen=True)
class TrackBObservationRunnerWatchResult:
    verdict: TrackBObservationRunnerVerdict
    report_json: Path
    report: dict[str, Any]
    cycle_results: tuple[TrackBObservationRunnerResult, ...]


def run_track_b_observation_once(
    *,
    market_data_payload: Mapping[str, Any],
    listener_config_payload: Mapping[str, Any],
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    expected_account_id: str,
    strategy_id: str,
    lane_id: str,
    timeframe: str = "quote_snapshot",
    source_id: str | None = None,
    signal_direction: str | None = None,
    output_root: Path = DEFAULT_TRACK_B_OBSERVATION_RUNNER_OUTPUT_ROOT,
    databento_observer_output_root: Path = DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT,
    strategy_adapter_output_root: Path = DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT,
    candle_producer_output_root: Path | None = None,
    writer_output_root: Path | None = None,
    listener_output_root: Path = DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT,
    operator_status_output_root: Path = DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT,
    runner_id: str | None = None,
    now: datetime | None = None,
    market_data_connection_attempted: bool = False,
) -> TrackBObservationRunnerResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_runner_id = runner_id or f"track_b_observation_runner_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_runner_id / "track_b_observation_runner_report.json"
    actual_source_id = source_id or "track_b_observation_runner"

    try:
        observer = observe_databento_candle_event(
            market_data_payload=market_data_payload,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            expected_account_id=expected_account_id,
            strategy_id=strategy_id,
            lane_id=lane_id,
            timeframe=timeframe,
            output_root=databento_observer_output_root,
            source_id=actual_source_id,
            signal_direction=signal_direction,
            market_data_connection_attempted=market_data_connection_attempted,
            observer_id=f"{actual_runner_id}_databento_observer",
            now=actual_now,
        )
        if observer.verdict != DatabentoCandleObserverVerdict.WROTE_EVENT or observer.candle_event is None:
            operator_status = _operator_status(
                databento_observer=observer,
                strategy_adapter=None,
                listener=None,
                output_root=operator_status_output_root,
                status_id=f"{actual_runner_id}_operator_status",
                now=actual_now,
            )
            return _write_runner_report(
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                runner_mode="poll_once",
                cycle_number=1,
                verdict=TrackBObservationRunnerVerdict.BLOCKED_DATABENTO_OBSERVER,
                databento_observer=observer,
                strategy_adapter=None,
                listener=None,
                operator_status=operator_status,
                primary_blocker=str(observer.report.get("primary_blocker") or "Databento observer did not produce an event."),
                required_next_action=str(observer.report.get("required_next_action") or "Fix Databento observer input before retrying."),
            )

        adapter = adapt_demo_candle_direction_signal(
            strategy_event_payload=observer.candle_event,
            inbox_dir=Path(str(listener_config_payload.get("inbox_dir") or "")),
            expected_account_id=expected_account_id,
            source_id=actual_source_id,
            output_root=strategy_adapter_output_root,
            candle_producer_output_root=candle_producer_output_root or Path("outputs/track_b_execution_core/candle_signal_producer"),
            writer_output_root=writer_output_root or Path("outputs/track_b_execution_core/signal_batch_writer"),
            adapter_id=f"{actual_runner_id}_strategy_adapter",
            now=actual_now,
        )
        if adapter.verdict != StrategySignalAdapterVerdict.EMITTED_SIGNAL_BATCH:
            operator_status = _operator_status(
                databento_observer=observer,
                strategy_adapter=adapter,
                listener=None,
                output_root=operator_status_output_root,
                status_id=f"{actual_runner_id}_operator_status",
                now=actual_now,
            )
            return _write_runner_report(
                report_json=report_json,
                now=actual_now,
                runner_id=actual_runner_id,
                runner_mode="poll_once",
                cycle_number=1,
                verdict=TrackBObservationRunnerVerdict.BLOCKED_STRATEGY_ADAPTER,
                databento_observer=observer,
                strategy_adapter=adapter,
                listener=None,
                operator_status=operator_status,
                primary_blocker=str(adapter.report.get("primary_blocker") or "Strategy adapter did not emit a signal batch."),
                required_next_action=str(adapter.report.get("required_next_action") or "Fix strategy adapter input before retrying."),
            )

        listener = run_shadow_listener_cycle(
            config_payload=listener_config_payload,
            output_root=listener_output_root,
            cycle_id=f"{actual_runner_id}_listener_cycle",
            now=actual_now,
        )
        operator_status = _operator_status(
            databento_observer=observer,
            strategy_adapter=adapter,
            listener=listener,
            output_root=operator_status_output_root,
            status_id=f"{actual_runner_id}_operator_status",
            now=actual_now,
        )
        listener_blocked = str(listener.report.get("listener_verdict") or "").endswith(("BLOCKED_INVALID_CONFIG", "BLOCKED_SCHEMA_ERROR"))
        listener_failed_files = int(listener.report.get("files_failed") or 0)
        verdict = (
            TrackBObservationRunnerVerdict.BLOCKED_LISTENER
            if listener_blocked
            else TrackBObservationRunnerVerdict.COMPLETED_WITH_BLOCKERS
            if listener_failed_files
            else TrackBObservationRunnerVerdict.COMPLETED_FOR_REVIEW
        )
        return _write_runner_report(
            report_json=report_json,
            now=actual_now,
            runner_id=actual_runner_id,
            runner_mode="poll_once",
            cycle_number=1,
            verdict=verdict,
            databento_observer=observer,
            strategy_adapter=adapter,
            listener=listener,
            operator_status=operator_status,
            primary_blocker=listener.report.get("primary_blocker") if listener_blocked else None,
            required_next_action=(
                str(listener.report.get("required_next_action") or "Fix listener blocker before retrying.")
                if listener_blocked or listener_failed_files
                else "Review Track B Status UI and latest no-submit artifacts. This runner does not authorize submit."
            ),
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_runner_report(
            report_json=report_json,
            now=actual_now,
            runner_id=actual_runner_id,
            runner_mode="poll_once",
            cycle_number=1,
            verdict=TrackBObservationRunnerVerdict.BLOCKED_SCHEMA_ERROR,
            databento_observer=None,
            strategy_adapter=None,
            listener=None,
            operator_status=None,
            primary_blocker=str(exc),
            required_next_action="Fix Track B observation runner inputs before retrying.",
        )


def run_track_b_observation_watch(
    *,
    market_data_payload_reader: Callable[[], Mapping[str, Any]],
    listener_config_payload: Mapping[str, Any],
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    expected_account_id: str,
    strategy_id: str,
    lane_id: str,
    timeframe: str = "quote_snapshot",
    source_id: str | None = None,
    signal_direction: str | None = None,
    output_root: Path = DEFAULT_TRACK_B_OBSERVATION_RUNNER_OUTPUT_ROOT,
    databento_observer_output_root: Path = DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT,
    strategy_adapter_output_root: Path = DEFAULT_STRATEGY_SIGNAL_ADAPTER_OUTPUT_ROOT,
    candle_producer_output_root: Path | None = None,
    writer_output_root: Path | None = None,
    listener_output_root: Path = DEFAULT_SHADOW_LISTENER_OUTPUT_ROOT,
    operator_status_output_root: Path = DEFAULT_OPERATOR_STATUS_OUTPUT_ROOT,
    max_cycles: int = 1,
    poll_seconds: float = 0.0,
    runner_id: str | None = None,
    now_func: Callable[[], datetime] | None = None,
    sleep_func: Callable[[float], None] | None = None,
    market_data_connection_attempted: bool = False,
) -> TrackBObservationRunnerWatchResult:
    if max_cycles <= 0:
        raise ValueError("max_cycles must be positive for Track B observation runner watch mode.")
    if poll_seconds < 0:
        raise ValueError("poll_seconds must be non-negative.")
    actual_runner_id = runner_id or f"track_b_observation_runner_watch_{uuid.uuid4().hex}"
    clock = now_func or (lambda: datetime.now(UTC))
    sleeper = sleep_func or sleep
    cycle_results: list[TrackBObservationRunnerResult] = []
    for cycle_number in range(1, max_cycles + 1):
        cycle_now = clock()
        payload = market_data_payload_reader()
        result = run_track_b_observation_once(
            market_data_payload=payload,
            listener_config_payload=listener_config_payload,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            expected_account_id=expected_account_id,
            strategy_id=strategy_id,
            lane_id=lane_id,
            timeframe=timeframe,
            source_id=source_id,
            signal_direction=signal_direction,
            output_root=output_root,
            databento_observer_output_root=databento_observer_output_root,
            strategy_adapter_output_root=strategy_adapter_output_root,
            candle_producer_output_root=candle_producer_output_root,
            writer_output_root=writer_output_root,
            listener_output_root=listener_output_root,
            operator_status_output_root=operator_status_output_root,
            runner_id=f"{actual_runner_id}_cycle_{cycle_number:04d}",
            now=cycle_now,
            market_data_connection_attempted=market_data_connection_attempted,
        )
        cycle_results.append(result)
        if cycle_number < max_cycles and poll_seconds > 0:
            sleeper(poll_seconds)
    final = cycle_results[-1]
    watch_report = dict(final.report)
    watch_report.update(
        {
            "runner_mode": "watch",
            "mode": "watch",
            "current_cycle": len(cycle_results),
            "max_cycles": max_cycles,
            "watch_exited_normally": len(cycle_results) == max_cycles,
            "cycle_report_paths": [str(result.report_json) for result in cycle_results],
        }
    )
    report_json = Path(output_root) / actual_runner_id / "track_b_observation_runner_report.json"
    return _write_watch_report(
        report_json=report_json,
        verdict=TrackBObservationRunnerVerdict(str(final.report["runner_verdict"])),
        report=watch_report,
        cycle_results=tuple(cycle_results),
    )


def _operator_status(
    *,
    databento_observer: DatabentoCandleObserverResult | None,
    strategy_adapter: StrategySignalAdapterResult | None,
    listener: ShadowListenerResult | None,
    output_root: Path,
    status_id: str,
    now: datetime,
) -> OperatorStatusResult:
    listener_report = {} if listener is None else listener.report
    runner_paths = listener_report.get("runner_summary_paths") or []
    return create_operator_status_summary(
        inputs=OperatorStatusInputs(
            listener_health_json=Path(listener_report["health_report_path"]) if listener_report.get("health_report_path") else None,
            listener_cycle_json=listener.report_json if listener is not None else None,
            shadow_runner_summary_json=Path(str(runner_paths[0])) if runner_paths else None,
            databento_candle_observer_report_json=databento_observer.report_json if databento_observer is not None else None,
            strategy_signal_adapter_report_json=strategy_adapter.report_json if strategy_adapter is not None else None,
            candle_signal_producer_report_json=strategy_adapter.candle_producer_report_json if strategy_adapter is not None else None,
            signal_batch_writer_report_json=strategy_adapter.writer_report_json if strategy_adapter is not None else None,
            output_root=output_root,
        ),
        status_id=status_id,
        now=now,
    )


def _write_runner_report(
    *,
    report_json: Path,
    now: datetime,
    runner_id: str,
    runner_mode: str,
    cycle_number: int,
    verdict: TrackBObservationRunnerVerdict,
    databento_observer: DatabentoCandleObserverResult | None,
    strategy_adapter: StrategySignalAdapterResult | None,
    listener: ShadowListenerResult | None,
    operator_status: OperatorStatusResult | None,
    primary_blocker: object | None,
    required_next_action: str,
) -> TrackBObservationRunnerResult:
    listener_report = {} if listener is None else listener.report
    report = {
        "schema_version": "track_b_observation_runner_v1",
        "generated_at": now.isoformat(),
        "track_b_observation_runner_id": runner_id,
        "runner_verdict": verdict.value,
        "mode": runner_mode,
        "current_cycle": cycle_number,
        "databento_observer_verdict": None if databento_observer is None else databento_observer.report.get("observer_verdict"),
        "strategy_adapter_verdict": None if strategy_adapter is None else strategy_adapter.report.get("adapter_verdict"),
        "candle_producer_verdict": None if strategy_adapter is None else strategy_adapter.report.get("candle_signal_producer_verdict"),
        "signal_batch_writer_verdict": _writer_verdict(strategy_adapter),
        "listener_verdict": listener_report.get("listener_verdict"),
        "listener_health_verdict": _listener_health_verdict(listener_report),
        "operator_status_verdict": None if operator_status is None else operator_status.report.get("status_verdict"),
        "latest_operator_status_path": None if operator_status is None else str(operator_status.report_json.parent.parent / "latest_operator_status_summary.json"),
        "databento_observer_report_path": None if databento_observer is None else str(databento_observer.report_json),
        "databento_candle_event_path": None if databento_observer is None else databento_observer.report.get("output_candle_event_path"),
        "strategy_adapter_report_path": None if strategy_adapter is None else str(strategy_adapter.report_json),
        "candle_producer_report_path": None if strategy_adapter is None or strategy_adapter.candle_producer_report_json is None else str(strategy_adapter.candle_producer_report_json),
        "signal_batch_writer_report_path": None if strategy_adapter is None or strategy_adapter.writer_report_json is None else str(strategy_adapter.writer_report_json),
        "listener_cycle_summary_path": None if listener is None else str(listener.report_json),
        "listener_runner_summary_paths": listener_report.get("runner_summary_paths") or [],
        "primary_blocker": primary_blocker,
        "secondary_blockers": _secondary_blockers(databento_observer, strategy_adapter, listener, operator_status),
        "required_next_action": required_next_action,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "tws_connection_attempted": False,
        "ibkr_connection_attempted": False,
        "paper_proof_cli_called": False,
        "paper_proof_cli_wired": False,
        "place_order_called": False,
        "cancel_called": False,
        "dashboard_is_not_authority": True,
        "observer_status_only": True,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_track_b_observation_runner_report.json"),
    }
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    return TrackBObservationRunnerResult(verdict=verdict, report_json=report_json, report=report)


def _write_watch_report(
    *,
    report_json: Path,
    verdict: TrackBObservationRunnerVerdict,
    report: dict[str, Any],
    cycle_results: tuple[TrackBObservationRunnerResult, ...],
) -> TrackBObservationRunnerWatchResult:
    report["report_json_path"] = str(report_json)
    report["latest_report_json_path"] = str(report_json.parent.parent / "latest_track_b_observation_runner_report.json")
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    return TrackBObservationRunnerWatchResult(verdict=verdict, report_json=report_json, report=report, cycle_results=cycle_results)


def _writer_verdict(strategy_adapter: StrategySignalAdapterResult | None) -> str | None:
    if strategy_adapter is None or strategy_adapter.writer_report_json is None:
        return None
    try:
        report = _read_json(strategy_adapter.writer_report_json)
    except (OSError, json.JSONDecodeError, ValueError):
        return None
    return str(report.get("signal_batch_writer_verdict") or "")


def _listener_health_verdict(listener_report: Mapping[str, Any]) -> str | None:
    path = listener_report.get("health_report_path")
    if not path:
        return None
    try:
        return str(_read_json(Path(str(path))).get("health_verdict") or "")
    except (OSError, json.JSONDecodeError, ValueError):
        return None


def _secondary_blockers(
    databento_observer: DatabentoCandleObserverResult | None,
    strategy_adapter: StrategySignalAdapterResult | None,
    listener: ShadowListenerResult | None,
    operator_status: OperatorStatusResult | None,
) -> list[str]:
    reports = []
    if databento_observer is not None:
        reports.append(databento_observer.report)
    if strategy_adapter is not None:
        reports.append(strategy_adapter.report)
    if listener is not None:
        reports.append(listener.report)
    if operator_status is not None:
        reports.append(operator_status.report)
    blockers: list[str] = []
    seen: set[str] = set()
    for report in reports:
        for blocker in (report.get("primary_blocker"), *tuple(report.get("secondary_blockers") or ())):
            if not blocker:
                continue
            text = str(blocker)
            if text not in seen:
                seen.add(text)
                blockers.append(text)
    return blockers


def _read_json(path: Path) -> dict[str, Any]:
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value
