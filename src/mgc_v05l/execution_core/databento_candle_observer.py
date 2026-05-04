"""Track B no-submit Databento market-data to candle/event observer.

This boundary turns a supplied Databento quote/candle artifact into a Track B
candle/event JSON file for later explicit strategy adapter processing. It does
not connect to Databento, infer execution authority, invoke the listener, or
touch broker/submit paths.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping

from .models import require_aware_datetime, to_jsonable


DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT = Path("outputs/track_b_execution_core/databento_candle_observer")


class DatabentoCandleObserverVerdict(str, Enum):
    WROTE_EVENT = "DATABENTO_CANDLE_OBSERVER_WROTE_EVENT"
    BLOCKED_NO_MARKET_DATA = "DATABENTO_CANDLE_OBSERVER_BLOCKED_NO_MARKET_DATA"
    BLOCKED_INVALID_INPUT = "DATABENTO_CANDLE_OBSERVER_BLOCKED_INVALID_INPUT"
    BLOCKED_SCHEMA_ERROR = "DATABENTO_CANDLE_OBSERVER_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class DatabentoCandleObserverResult:
    verdict: DatabentoCandleObserverVerdict
    report_json: Path
    report: dict[str, Any]
    candle_event_json: Path | None
    candle_event: dict[str, Any] | None


@dataclass(frozen=True)
class DatabentoCandleObserverWatchResult:
    heartbeat_json: Path
    heartbeat: dict[str, Any]
    cycle_results: tuple[DatabentoCandleObserverResult, ...]


@dataclass(frozen=True)
class DatabentoCandleObserverWaitResult:
    heartbeat_json: Path
    heartbeat: dict[str, Any]
    cycle_results: tuple[DatabentoCandleObserverResult, ...]


def observe_databento_candle_event(
    *,
    market_data_payload: Mapping[str, Any],
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    expected_account_id: str,
    strategy_id: str,
    lane_id: str,
    timeframe: str,
    output_root: Path = DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT,
    source_id: str | None = None,
    signal_direction: str | None = None,
    market_data_connection_attempted: bool = False,
    observer_id: str | None = None,
    now: datetime | None = None,
) -> DatabentoCandleObserverResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_observer_id = observer_id or f"databento_candle_observer_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_observer_id / "databento_candle_observer_report.json"
    event_json = Path(output_root) / actual_observer_id / "databento_candle_event.json"
    actual_source_id = source_id or _optional_text(market_data_payload.get("source_id")) or "databento_candle_observer"

    try:
        _require_text(contract_key, "contract_key")
        _require_text(databento_continuous_symbol, "databento_continuous_symbol")
        _require_text(dataset, "dataset")
        _require_text(expected_account_id, "expected_account_id")
        _require_text(strategy_id, "strategy_id")
        _require_text(lane_id, "lane_id")
        _require_text(timeframe, "timeframe")
        if _is_no_record_payload(market_data_payload):
            return _write_report(
                report_json=report_json,
                event_json=event_json,
                verdict=DatabentoCandleObserverVerdict.BLOCKED_NO_MARKET_DATA,
                now=actual_now,
                observer_id=actual_observer_id,
                source_id=actual_source_id,
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                timeframe=timeframe,
                candle_event=None,
                market_data_connection_attempted=market_data_connection_attempted,
                primary_blocker=_market_data_blocker(market_data_payload),
                required_next_action=_market_data_required_next_action(market_data_payload),
                market_data_payload=market_data_payload,
            )

        candle = _extract_candle(market_data_payload)
        event_timestamp = _event_timestamp(market_data_payload)
        candle_event = {
            "source_id": actual_source_id,
            "batch_id": _optional_text(market_data_payload.get("batch_id")) or f"databento_candle_batch_{uuid.uuid4().hex}",
            "account_id": expected_account_id,
            "contract_key": contract_key,
            "instrument_family": _optional_text(market_data_payload.get("instrument_family")) or _instrument_family(contract_key),
            "strategy_id": strategy_id,
            "lane_id": lane_id,
            "signal_type": "databento_market_data_observation",
            "candle_timestamp": event_timestamp,
            "observed_at": _optional_text(market_data_payload.get("observed_at")) or event_timestamp,
            "timeframe": timeframe,
            "open": candle["open"],
            "high": candle["high"],
            "low": candle["low"],
            "close": candle["close"],
            "volume": candle.get("volume"),
            "reason": "Databento market-data observer produced no-submit candle/event evidence for Track B review.",
            "metadata": {
                "databento_candle_observer_boundary": "databento_candle_observer",
                "market_data_provider": "DATABENTO",
                "market_data_role": "EVIDENCE_ONLY",
                "dataset": dataset,
                "databento_continuous_symbol": databento_continuous_symbol,
                "databento_symbol": market_data_payload.get("databento_symbol") or market_data_payload.get("raw_symbol"),
                "provider_symbol": market_data_payload.get("provider_symbol") or market_data_payload.get("symbol_selector"),
                "source_report_path": market_data_payload.get("report_json_path"),
                "source_schema_version": market_data_payload.get("schema_version"),
                "ohlc_source": candle["ohlc_source"],
                "databento_is_execution_authority": False,
                "local_execution_contract_key_remains_authority": True,
            },
        }
        if _optional_text(signal_direction):
            candle_event["signal_direction"] = str(signal_direction).strip().upper()
            candle_event["decision_style"] = "BINARY"
        return _write_report(
            report_json=report_json,
            event_json=event_json,
            verdict=DatabentoCandleObserverVerdict.WROTE_EVENT,
            now=actual_now,
            observer_id=actual_observer_id,
            source_id=actual_source_id,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            timeframe=timeframe,
            candle_event=candle_event,
            market_data_connection_attempted=market_data_connection_attempted,
            primary_blocker=None,
            required_next_action="Run strategy_signal_adapter_cli explicitly if this no-submit market-data event should enter the listener inbox.",
            market_data_payload=market_data_payload,
        )
    except (TypeError, ValueError, OSError) as exc:
        return _write_report(
            report_json=report_json,
            event_json=event_json,
            verdict=DatabentoCandleObserverVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            observer_id=actual_observer_id,
            source_id=actual_source_id,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            timeframe=timeframe,
            candle_event=None,
            market_data_connection_attempted=market_data_connection_attempted,
            primary_blocker=str(exc),
            required_next_action="Fix Databento candle observer input before retrying.",
            market_data_payload=market_data_payload,
        )


def write_databento_candle_observer_blocked_report(
    *,
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    timeframe: str,
    output_root: Path = DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT,
    source_id: str | None = None,
    observer_id: str | None = None,
    verdict: DatabentoCandleObserverVerdict = DatabentoCandleObserverVerdict.BLOCKED_SCHEMA_ERROR,
    primary_blocker: str,
    required_next_action: str,
    market_data_connection_attempted: bool = False,
    now: datetime | None = None,
) -> DatabentoCandleObserverResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_observer_id = observer_id or f"databento_candle_observer_{uuid.uuid4().hex}"
    actual_source_id = source_id or "databento_candle_observer"
    return _write_report(
        report_json=Path(output_root) / actual_observer_id / "databento_candle_observer_report.json",
        event_json=Path(output_root) / actual_observer_id / "databento_candle_event.json",
        verdict=verdict,
        now=actual_now,
        observer_id=actual_observer_id,
        source_id=actual_source_id,
        contract_key=contract_key,
        databento_continuous_symbol=databento_continuous_symbol,
        dataset=dataset,
        timeframe=timeframe,
        candle_event=None,
        market_data_connection_attempted=market_data_connection_attempted,
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
        market_data_payload=None,
    )


def _is_no_record_payload(payload: Mapping[str, Any]) -> bool:
    quote_observed = payload.get("quote_observed")
    if quote_observed is False:
        return True
    if str(payload.get("schema_version") or "").strip() == "track_b_databento_current_quote_v1":
        if payload.get("current_quote_available") is False:
            return True
    if str(payload.get("classification") or "").strip().upper().endswith("NO_RECORDS"):
        return True
    return False


def _market_data_blocker(payload: Mapping[str, Any]) -> str:
    provider_error = _optional_text(payload.get("provider_error"))
    no_records_reason = _optional_text(payload.get("no_records_reason"))
    classification = _optional_text(payload.get("classification") or payload.get("quote_status"))
    requested_end = _optional_text(payload.get("requested_quote_end"))
    available_end = _optional_text(payload.get("provider_available_end") or payload.get("provider_available_end_final"))
    if provider_error:
        if requested_end and available_end:
            return (
                f"Databento market-data provider error: {provider_error}; "
                f"requested_quote_end={requested_end}; provider_available_end={available_end}"
            )
        return f"Databento market-data provider error: {provider_error}"
    if requested_end and available_end and payload.get("current_quote_available") is False:
        freshness_verdict = _optional_text(payload.get("quote_freshness_verdict"))
        quote_age = _optional_text(payload.get("quote_age_seconds"))
        max_age = _optional_text(payload.get("max_current_quote_age_seconds"))
        if freshness_verdict:
            return (
                "Databento current quote is not currently available under the explicit freshness policy; "
                f"requested_quote_end={requested_end}; provider_available_end={available_end}; "
                f"quote_freshness_verdict={freshness_verdict}; quote_age_seconds={quote_age}; "
                f"max_current_quote_age_seconds={max_age}"
            )
        return (
            "Databento current quote is not currently available for the requested window; "
            f"requested_quote_end={requested_end}; provider_available_end={available_end}"
        )
    if no_records_reason:
        return f"Databento market-data payload contained no records: {no_records_reason}"
    if classification:
        return f"Databento market-data payload did not contain an observed quote/candle: {classification}"
    return "Databento market-data payload did not contain an observed quote/candle."


def _market_data_required_next_action(payload: Mapping[str, Any]) -> str:
    requested_end = _optional_text(payload.get("requested_quote_end"))
    available_end = _optional_text(payload.get("provider_available_end") or payload.get("provider_available_end_final"))
    if requested_end and available_end:
        return (
            "Databento available_end is behind the requested current quote window. Wait for fresh market data, "
            "or rerun the no-submit observer with explicit --allow-available-end-fallback for historical evidence only; "
            "do not treat the fallback as readiness or submit authority."
        )
    return "Provide a Databento quote/candle artifact with observed current market data before producing a Track B event."


def _extract_candle(payload: Mapping[str, Any]) -> dict[str, Any]:
    explicit_close = payload.get("close")
    close = explicit_close if explicit_close is not None else payload.get("last")
    if close in {None, ""}:
        raise ValueError("close or last is required to produce a candle/event.")
    ohlc_source = "explicit_ohlc" if explicit_close is not None else "quote_last"
    return {
        "open": payload.get("open") if payload.get("open") not in {None, ""} else close,
        "high": payload.get("high") if payload.get("high") not in {None, ""} else close,
        "low": payload.get("low") if payload.get("low") not in {None, ""} else close,
        "close": close,
        "volume": payload.get("volume"),
        "ohlc_source": ohlc_source,
    }


def _event_timestamp(payload: Mapping[str, Any]) -> str:
    timestamp = _optional_text(
        payload.get("candle_timestamp")
        or payload.get("event_timestamp")
        or payload.get("timestamp")
        or payload.get("actual_quote_end")
        or payload.get("requested_quote_end")
    )
    if timestamp is None:
        raise ValueError("timestamp/candle_timestamp is required to produce a candle/event.")
    return timestamp


def _instrument_family(contract_key: str) -> str | None:
    text = _optional_text(contract_key)
    if text is None:
        return None
    return text.split("-", maxsplit=1)[0]


def _require_text(value: object, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def _write_report(
    *,
    report_json: Path,
    event_json: Path,
    verdict: DatabentoCandleObserverVerdict,
    now: datetime,
    observer_id: str,
    source_id: str,
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    timeframe: str,
    candle_event: dict[str, Any] | None,
    market_data_connection_attempted: bool,
    primary_blocker: str | None,
    required_next_action: str,
    market_data_payload: Mapping[str, Any] | None = None,
) -> DatabentoCandleObserverResult:
    market_data_payload = market_data_payload or {}
    if candle_event is not None:
        event_json.parent.mkdir(parents=True, exist_ok=True)
        event_payload = json.dumps(to_jsonable(candle_event), indent=2, sort_keys=True)
        event_json.write_text(event_payload, encoding="utf-8")
        latest_event_json = event_json.parent.parent / "latest_databento_candle_event.json"
        latest_event_json.write_text(event_payload, encoding="utf-8")
    else:
        latest_event_json = event_json.parent.parent / "latest_databento_candle_event.json"

    report = {
        "schema_version": "track_b_databento_candle_observer_v1",
        "generated_at": now.isoformat(),
        "databento_candle_observer_id": observer_id,
        "observer_mode": "one_shot",
        "observer_verdict": verdict.value,
        "source_id": source_id,
        "contract_key": contract_key,
        "local_execution_contract_key": contract_key,
        "databento_continuous_symbol": databento_continuous_symbol,
        "databento_symbol": None if candle_event is None else candle_event["metadata"].get("databento_symbol"),
        "dataset": dataset,
        "timeframe": timeframe,
        "event_timestamp": None if candle_event is None else candle_event.get("candle_timestamp"),
        "candle_timestamp": None if candle_event is None else candle_event.get("candle_timestamp"),
        "current_quote_available": market_data_payload.get("current_quote_available"),
        "max_current_quote_age_seconds": market_data_payload.get("max_current_quote_age_seconds"),
        "quote_age_seconds": market_data_payload.get("quote_age_seconds"),
        "quote_freshness_verdict": market_data_payload.get("quote_freshness_verdict"),
        "requested_quote_end": market_data_payload.get("requested_quote_end"),
        "provider_available_end": market_data_payload.get("provider_available_end"),
        "provider_available_end_final": market_data_payload.get("provider_available_end_final"),
        "available_end_fallback_used": market_data_payload.get("available_end_fallback_used"),
        "open": None if candle_event is None else candle_event.get("open"),
        "high": None if candle_event is None else candle_event.get("high"),
        "low": None if candle_event is None else candle_event.get("low"),
        "close": None if candle_event is None else candle_event.get("close"),
        "volume": None if candle_event is None else candle_event.get("volume"),
        "quote_candle_source_metadata": {} if candle_event is None else candle_event.get("metadata"),
        "output_candle_event_path": None if candle_event is None else str(event_json),
        "latest_candle_event_path": str(latest_event_json),
        "listener_invoked": False,
        "runner_invoked": False,
        "operator_status_invoked": False,
        "strategy_adapter_invoked": False,
        "candle_signal_producer_invoked": False,
        "signal_batch_writer_invoked": False,
        "lane_registry_invoked": False,
        "order_plan_created": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "tws_connection_attempted": False,
        "ibkr_connection_attempted": False,
        "market_data_connection_attempted": market_data_connection_attempted,
        "databento_connection_attempted": market_data_connection_attempted,
        "paper_proof_cli_wired": False,
        "place_order_called": False,
        "cancel_called": False,
        "primary_blocker": primary_blocker,
        "secondary_blockers": [],
        "required_next_action": required_next_action,
        "databento_is_execution_authority": False,
        "databento_continuous_symbol_is_execution_authority": False,
        "ibkr_allowlist_remains_execution_authority": True,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(report_json.parent.parent / "latest_databento_candle_observer_report.json"),
    }
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json = Path(str(report["latest_report_json_path"]))
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    return DatabentoCandleObserverResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        candle_event_json=None if candle_event is None else event_json,
        candle_event=candle_event,
    )


def watch_databento_candle_observer(
    *,
    market_data_payload_reader: Callable[[], Mapping[str, Any]],
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    expected_account_id: str,
    strategy_id: str,
    lane_id: str,
    timeframe: str,
    output_root: Path = DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT,
    source_id: str | None = None,
    signal_direction: str | None = None,
    market_data_connection_attempted: bool = False,
    max_cycles: int,
    poll_seconds: float = 0.0,
    sleep_func: Callable[[float], None] | None = None,
    watch_id: str | None = None,
    now_func: Callable[[], datetime] | None = None,
) -> DatabentoCandleObserverWatchResult:
    if max_cycles <= 0:
        raise ValueError("max_cycles must be positive for Databento observer watch mode.")
    if poll_seconds < 0:
        raise ValueError("poll_seconds must be non-negative.")
    actual_watch_id = watch_id or f"databento_candle_observer_watch_{uuid.uuid4().hex}"
    actual_now_func = now_func or (lambda: datetime.now(UTC))
    actual_sleep = sleep_func or (lambda seconds: None)
    cycle_results: list[DatabentoCandleObserverResult] = []
    processed_cycles = 0
    no_data_cycles = 0
    error_cycles = 0
    last_started_at: str | None = None
    last_ended_at: str | None = None
    heartbeat: dict[str, Any] = {}

    for cycle_number in range(1, max_cycles + 1):
        started_at = actual_now_func()
        require_aware_datetime(started_at, "started_at")
        last_started_at = started_at.isoformat()
        try:
            payload = market_data_payload_reader()
            if not isinstance(payload, Mapping):
                raise ValueError("Databento observer watch payload reader must return a JSON object.")
            result = observe_databento_candle_event(
                market_data_payload=payload,
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                expected_account_id=expected_account_id,
                strategy_id=strategy_id,
                lane_id=lane_id,
                timeframe=timeframe,
                output_root=output_root,
                source_id=source_id,
                signal_direction=signal_direction,
                market_data_connection_attempted=market_data_connection_attempted,
                observer_id=f"{actual_watch_id}_cycle_{cycle_number}",
                now=started_at,
            )
        except (TypeError, ValueError, OSError) as exc:
            result = _write_report(
                report_json=Path(output_root) / f"{actual_watch_id}_cycle_{cycle_number}" / "databento_candle_observer_report.json",
                event_json=Path(output_root) / f"{actual_watch_id}_cycle_{cycle_number}" / "databento_candle_event.json",
                verdict=DatabentoCandleObserverVerdict.BLOCKED_SCHEMA_ERROR,
                now=started_at,
                observer_id=f"{actual_watch_id}_cycle_{cycle_number}",
                source_id=source_id or "databento_candle_observer",
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                timeframe=timeframe,
                candle_event=None,
                market_data_connection_attempted=market_data_connection_attempted,
                primary_blocker=str(exc),
                required_next_action="Fix Databento observer watch input before retrying.",
            )
        cycle_results.append(result)
        if result.verdict == DatabentoCandleObserverVerdict.WROTE_EVENT:
            processed_cycles += 1
        elif result.verdict == DatabentoCandleObserverVerdict.BLOCKED_NO_MARKET_DATA:
            no_data_cycles += 1
        else:
            error_cycles += 1
        ended_at = actual_now_func()
        require_aware_datetime(ended_at, "ended_at")
        last_ended_at = ended_at.isoformat()
        heartbeat = _write_heartbeat(
            output_root=Path(output_root),
            watch_id=actual_watch_id,
            generated_at=ended_at,
            current_cycle_number=cycle_number,
            max_cycles=max_cycles,
            last_cycle_start=last_started_at,
            last_cycle_end=last_ended_at,
            processed_cycles=processed_cycles,
            no_data_cycles=no_data_cycles,
            error_cycles=error_cycles,
            last_result=result,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            watch_exited_normally=cycle_number == max_cycles,
        )
        if cycle_number < max_cycles and poll_seconds > 0:
            actual_sleep(poll_seconds)
    heartbeat_json = Path(heartbeat["latest_heartbeat_json_path"])
    return DatabentoCandleObserverWatchResult(
        heartbeat_json=heartbeat_json,
        heartbeat=heartbeat,
        cycle_results=tuple(cycle_results),
    )


def wait_for_current_databento_quote(
    *,
    market_data_payload_reader: Callable[[], Mapping[str, Any]],
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    expected_account_id: str,
    strategy_id: str,
    lane_id: str,
    timeframe: str,
    output_root: Path = DEFAULT_DATABENTO_CANDLE_OBSERVER_OUTPUT_ROOT,
    source_id: str | None = None,
    signal_direction: str | None = None,
    market_data_connection_attempted: bool = False,
    max_wait_cycles: int,
    wait_poll_seconds: float = 0.0,
    sleep_func: Callable[[float], None] | None = None,
    wait_id: str | None = None,
    now_func: Callable[[], datetime] | None = None,
) -> DatabentoCandleObserverWaitResult:
    if max_wait_cycles <= 0:
        raise ValueError("max_wait_cycles must be positive for Databento current quote wait mode.")
    if wait_poll_seconds < 0:
        raise ValueError("wait_poll_seconds must be non-negative.")
    actual_wait_id = wait_id or f"databento_current_quote_wait_{uuid.uuid4().hex}"
    actual_now_func = now_func or (lambda: datetime.now(UTC))
    actual_sleep = sleep_func or (lambda seconds: None)
    cycle_results: list[DatabentoCandleObserverResult] = []
    successful_current_quote_cycles = 0
    available_end_lag_cycles = 0
    no_data_cycles = 0
    error_cycles = 0
    heartbeat: dict[str, Any] = {}
    wait_succeeded = False

    for cycle_number in range(1, max_wait_cycles + 1):
        started_at = actual_now_func()
        require_aware_datetime(started_at, "started_at")
        payload: Mapping[str, Any] | None = None
        try:
            payload = market_data_payload_reader()
            if not isinstance(payload, Mapping):
                raise ValueError("Databento current quote wait payload reader must return a JSON object.")
            result = observe_databento_candle_event(
                market_data_payload=payload,
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                expected_account_id=expected_account_id,
                strategy_id=strategy_id,
                lane_id=lane_id,
                timeframe=timeframe,
                output_root=output_root,
                source_id=source_id,
                signal_direction=signal_direction,
                market_data_connection_attempted=market_data_connection_attempted,
                observer_id=f"{actual_wait_id}_cycle_{cycle_number}",
                now=started_at,
            )
        except (TypeError, ValueError, OSError) as exc:
            result = _write_report(
                report_json=Path(output_root) / f"{actual_wait_id}_cycle_{cycle_number}" / "databento_candle_observer_report.json",
                event_json=Path(output_root) / f"{actual_wait_id}_cycle_{cycle_number}" / "databento_candle_event.json",
                verdict=DatabentoCandleObserverVerdict.BLOCKED_SCHEMA_ERROR,
                now=started_at,
                observer_id=f"{actual_wait_id}_cycle_{cycle_number}",
                source_id=source_id or "databento_current_quote_wait",
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                timeframe=timeframe,
                candle_event=None,
                market_data_connection_attempted=market_data_connection_attempted,
                primary_blocker=str(exc),
                required_next_action="Fix Databento current quote wait input before retrying.",
            )
        cycle_results.append(result)
        if result.verdict == DatabentoCandleObserverVerdict.WROTE_EVENT:
            successful_current_quote_cycles += 1
            wait_succeeded = True
        elif _is_available_end_lag_payload(payload or {}):
            available_end_lag_cycles += 1
            no_data_cycles += 1
        elif result.verdict == DatabentoCandleObserverVerdict.BLOCKED_NO_MARKET_DATA:
            no_data_cycles += 1
        else:
            error_cycles += 1
        fatal_provider_error = bool(_optional_text((payload or {}).get("provider_error"))) and not _is_available_end_lag_payload(payload or {})
        fatal_schema_error = result.verdict != DatabentoCandleObserverVerdict.BLOCKED_NO_MARKET_DATA and not wait_succeeded

        ended_at = actual_now_func()
        require_aware_datetime(ended_at, "ended_at")
        heartbeat = _write_wait_heartbeat(
            output_root=Path(output_root),
            wait_id=actual_wait_id,
            generated_at=ended_at,
            current_cycle_number=cycle_number,
            max_wait_cycles=max_wait_cycles,
            wait_poll_seconds=wait_poll_seconds,
            successful_current_quote_cycles=successful_current_quote_cycles,
            available_end_lag_cycles=available_end_lag_cycles,
            no_data_cycles=no_data_cycles,
            error_cycles=error_cycles,
            last_result=result,
            last_payload=payload or {},
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            wait_exited_normally=(wait_succeeded or cycle_number == max_wait_cycles) and not fatal_provider_error and not fatal_schema_error,
            wait_succeeded=wait_succeeded,
        )
        if wait_succeeded:
            break
        if fatal_schema_error:
            break
        if fatal_provider_error:
            break
        if cycle_number < max_wait_cycles and wait_poll_seconds > 0:
            actual_sleep(wait_poll_seconds)
    heartbeat_json = Path(heartbeat["latest_heartbeat_json_path"])
    return DatabentoCandleObserverWaitResult(
        heartbeat_json=heartbeat_json,
        heartbeat=heartbeat,
        cycle_results=tuple(cycle_results),
    )


def _write_heartbeat(
    *,
    output_root: Path,
    watch_id: str,
    generated_at: datetime,
    current_cycle_number: int,
    max_cycles: int,
    last_cycle_start: str | None,
    last_cycle_end: str | None,
    processed_cycles: int,
    no_data_cycles: int,
    error_cycles: int,
    last_result: DatabentoCandleObserverResult,
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    watch_exited_normally: bool,
) -> dict[str, Any]:
    heartbeat_json = output_root / watch_id / "databento_candle_observer_heartbeat.json"
    latest_heartbeat_json = output_root / "latest_databento_candle_observer_heartbeat.json"
    heartbeat = {
        "schema_version": "track_b_databento_candle_observer_heartbeat_v1",
        "generated_at": generated_at.isoformat(),
        "observer_mode": "watch",
        "watch_id": watch_id,
        "current_cycle_number": current_cycle_number,
        "max_cycles": max_cycles,
        "last_cycle_start": last_cycle_start,
        "last_cycle_end": last_cycle_end,
        "processed_cycles": processed_cycles,
        "no_data_cycles": no_data_cycles,
        "error_cycles": error_cycles,
        "last_observer_verdict": last_result.verdict.value,
        "last_event_timestamp": last_result.report.get("event_timestamp"),
        "contract_key": contract_key,
        "local_execution_contract_key": contract_key,
        "databento_continuous_symbol": databento_continuous_symbol,
        "dataset": dataset,
        "output_event_path": last_result.report.get("output_candle_event_path"),
        "latest_event_path": last_result.report.get("latest_candle_event_path"),
        "latest_observer_report_path": last_result.report.get("latest_report_json_path"),
        "last_observer_report_path": str(last_result.report_json),
        "watch_exited_normally": watch_exited_normally,
        "listener_invoked": False,
        "runner_invoked": False,
        "operator_status_invoked": False,
        "strategy_adapter_invoked": False,
        "candle_signal_producer_invoked": False,
        "signal_batch_writer_invoked": False,
        "lane_registry_invoked": False,
        "order_plan_created": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "tws_connection_attempted": False,
        "ibkr_connection_attempted": False,
        "market_data_connection_attempted": bool(last_result.report.get("market_data_connection_attempted")),
        "databento_connection_attempted": bool(last_result.report.get("databento_connection_attempted")),
        "paper_proof_cli_wired": False,
        "place_order_called": False,
        "cancel_called": False,
        "primary_blocker": last_result.report.get("primary_blocker"),
        "secondary_blockers": [],
        "required_next_action": "Continue explicit no-submit downstream steps only when operator review requires them.",
        "heartbeat_json_path": str(heartbeat_json),
        "latest_heartbeat_json_path": str(latest_heartbeat_json),
    }
    payload = json.dumps(to_jsonable(heartbeat), indent=2, sort_keys=True)
    heartbeat_json.parent.mkdir(parents=True, exist_ok=True)
    heartbeat_json.write_text(payload, encoding="utf-8")
    latest_heartbeat_json.parent.mkdir(parents=True, exist_ok=True)
    latest_heartbeat_json.write_text(payload, encoding="utf-8")
    return heartbeat


def _write_wait_heartbeat(
    *,
    output_root: Path,
    wait_id: str,
    generated_at: datetime,
    current_cycle_number: int,
    max_wait_cycles: int,
    wait_poll_seconds: float,
    successful_current_quote_cycles: int,
    available_end_lag_cycles: int,
    no_data_cycles: int,
    error_cycles: int,
    last_result: DatabentoCandleObserverResult,
    last_payload: Mapping[str, Any],
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    wait_exited_normally: bool,
    wait_succeeded: bool,
) -> dict[str, Any]:
    heartbeat_json = output_root / wait_id / "databento_candle_observer_heartbeat.json"
    latest_heartbeat_json = output_root / "latest_databento_candle_observer_heartbeat.json"
    current_quote_available = last_payload.get("current_quote_available")
    max_current_quote_age_seconds = last_payload.get("max_current_quote_age_seconds")
    quote_age_seconds = last_payload.get("quote_age_seconds")
    quote_freshness_verdict = last_payload.get("quote_freshness_verdict")
    requested_quote_end = _optional_text(last_payload.get("requested_quote_end"))
    provider_available_end = _optional_text(last_payload.get("provider_available_end") or last_payload.get("provider_available_end_final"))
    required_next_action = (
        "Current Databento quote artifact is available for no-submit market-data review."
        if wait_succeeded
        else (
            "Databento available_end is still behind the requested current quote window. Continue waiting or rerun later; "
            "do not use fallback/historical data as readiness."
            if available_end_lag_cycles > 0
            else last_result.report.get("required_next_action")
        )
    )
    heartbeat = {
        "schema_version": "track_b_databento_candle_observer_heartbeat_v1",
        "generated_at": generated_at.isoformat(),
        "observer_mode": "wait_for_current_quote",
        "wait_id": wait_id,
        "watch_id": wait_id,
        "current_cycle_number": current_cycle_number,
        "max_cycles": max_wait_cycles,
        "max_wait_cycles": max_wait_cycles,
        "poll_seconds": wait_poll_seconds,
        "wait_poll_seconds": wait_poll_seconds,
        "successful_current_quote_cycles": successful_current_quote_cycles,
        "available_end_lag_cycles": available_end_lag_cycles,
        "no_data_cycles": no_data_cycles,
        "error_cycles": error_cycles,
        "last_requested_quote_end": requested_quote_end,
        "last_provider_available_end": provider_available_end,
        "max_current_quote_age_seconds": max_current_quote_age_seconds,
        "quote_age_seconds": quote_age_seconds,
        "quote_freshness_verdict": quote_freshness_verdict,
        "last_observer_verdict": last_result.verdict.value,
        "last_event_timestamp": last_result.report.get("event_timestamp"),
        "current_quote_available": current_quote_available,
        "wait_exited_normally": wait_exited_normally,
        "watch_exited_normally": wait_exited_normally,
        "wait_succeeded": wait_succeeded,
        "contract_key": contract_key,
        "local_execution_contract_key": contract_key,
        "databento_continuous_symbol": databento_continuous_symbol,
        "dataset": dataset,
        "output_event_path": last_result.report.get("output_candle_event_path"),
        "latest_event_path": last_result.report.get("latest_candle_event_path"),
        "latest_observer_report_path": last_result.report.get("latest_report_json_path"),
        "last_observer_report_path": str(last_result.report_json),
        "listener_invoked": False,
        "runner_invoked": False,
        "operator_status_invoked": False,
        "strategy_adapter_invoked": False,
        "candle_signal_producer_invoked": False,
        "signal_batch_writer_invoked": False,
        "lane_registry_invoked": False,
        "order_plan_created": False,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "broker_connection_attempted": False,
        "tws_connection_attempted": False,
        "ibkr_connection_attempted": False,
        "market_data_connection_attempted": bool(last_result.report.get("market_data_connection_attempted")),
        "databento_connection_attempted": bool(last_result.report.get("databento_connection_attempted")),
        "paper_proof_cli_wired": False,
        "place_order_called": False,
        "cancel_called": False,
        "primary_blocker": last_result.report.get("primary_blocker"),
        "secondary_blockers": [],
        "required_next_action": required_next_action,
        "heartbeat_json_path": str(heartbeat_json),
        "latest_heartbeat_json_path": str(latest_heartbeat_json),
    }
    payload = json.dumps(to_jsonable(heartbeat), indent=2, sort_keys=True)
    heartbeat_json.parent.mkdir(parents=True, exist_ok=True)
    heartbeat_json.write_text(payload, encoding="utf-8")
    latest_heartbeat_json.parent.mkdir(parents=True, exist_ok=True)
    latest_heartbeat_json.write_text(payload, encoding="utf-8")
    return heartbeat


def _is_available_end_lag_payload(payload: Mapping[str, Any]) -> bool:
    if _optional_text(payload.get("requested_quote_end")) is None:
        return False
    if _optional_text(payload.get("provider_available_end") or payload.get("provider_available_end_final")) is None:
        return False
    if payload.get("current_quote_available") is False:
        return True
    error_code = str(payload.get("native_databento_error_code") or "").strip().lower()
    return "available_end" in error_code
