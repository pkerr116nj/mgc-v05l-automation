"""Track B MGC candle-history producer.

This boundary produces the bounded candle-history JSON input consumed by
``track_b_market_history``. It may normalize supplied Databento-like OHLCV
history or use an injected Databento historical records transport, but it
requires explicit realtime current-quote evidence before writing a signal-ready
history input. It does not call broker, listener, strategy, readiness, or paper
proof paths.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Protocol, Sequence

from .databento_quote_provider import DatabentoAvailableEndError, DatabentoQuoteProviderError, DatabentoQuoteTransport
from .models import require_aware_datetime, to_jsonable


DEFAULT_TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_OUTPUT_ROOT = Path(
    "outputs/track_b_execution_core/track_b_mgc_candle_history_producer"
)
MGC_CONTRACT_KEY = "MGC-202606"
MGC_LOCAL_SYMBOL = "MGCM6"
MGC_DATABENTO_CONTINUOUS_SYMBOL = "MGC.v.0"
MGC_DATASET = "GLBX.MDP3"


class TrackBMgcCandleHistoryProducerVerdict(str, Enum):
    WROTE_HISTORY_INPUT = "TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_WROTE_HISTORY_INPUT"
    BLOCKED_INSUFFICIENT_CANDLES = "TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_BLOCKED_INSUFFICIENT_CANDLES"
    BLOCKED_NON_REALTIME_CURRENT_EVIDENCE = "TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_BLOCKED_NON_REALTIME_CURRENT_EVIDENCE"
    BLOCKED_PROVIDER_ERROR = "TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_BLOCKED_PROVIDER_ERROR"
    BLOCKED_INVALID_INPUT = "TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_BLOCKED_INVALID_INPUT"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class TrackBMgcCandleHistoryProducerResult:
    verdict: TrackBMgcCandleHistoryProducerVerdict
    report_json: Path
    report: dict[str, Any]
    history_input_json: Path | None
    history_input: dict[str, Any] | None


class DatabentoCandleHistoryTransport(Protocol):
    def request_records(
        self,
        *,
        base_url: str,
        api_key: str,
        dataset: str,
        symbol: str,
        schema: str,
        start: datetime,
        end: datetime,
        stype_in: str,
        limit: int,
    ) -> Sequence[Mapping[str, Any]]: ...


def produce_track_b_mgc_candle_history_input(
    *,
    history_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    current_quote_report_payload: Mapping[str, Any],
    history_payload_path: Path | None,
    current_quote_report_path: Path | None,
    expected_account_id: str,
    strategy_id: str,
    lane_id: str,
    contract_key: str = MGC_CONTRACT_KEY,
    databento_continuous_symbol: str = MGC_DATABENTO_CONTINUOUS_SYMBOL,
    allowlisted_local_symbol: str = MGC_LOCAL_SYMBOL,
    dataset: str = MGC_DATASET,
    timeframe: str = "1m",
    max_candles: int = 50,
    min_candles: int = 3,
    source_id: str | None = None,
    output_root: Path = DEFAULT_TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_OUTPUT_ROOT,
    producer_id: str | None = None,
    history_provider_mode: str = "DATABENTO_HISTORICAL_BOUNDED_WITH_REALTIME_CURRENT",
    now: datetime | None = None,
) -> TrackBMgcCandleHistoryProducerResult:
    actual_now = now or datetime.now(timezone.utc)
    require_aware_datetime(actual_now, "now")
    actual_producer_id = producer_id or f"track_b_mgc_candle_history_producer_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_producer_id / "track_b_mgc_candle_history_producer_report.json"
    history_input_json = Path(output_root) / actual_producer_id / "track_b_mgc_candle_history_input.json"
    actual_source_id = source_id or _source_id(history_payload) or "track_b_mgc_candle_history_producer"

    try:
        quote_blocker = _current_quote_blocker(current_quote_report_payload)
        raw_candles = _raw_candles(history_payload)
        candles = [_normalize_candle(item, index=index) for index, item in enumerate(raw_candles, start=1)]
        bounded_candles = candles[-max_candles:]
        input_blocker = _input_blocker(
            history_payload=history_payload,
            expected_account_id=expected_account_id,
            contract_key=contract_key,
        )
        if input_blocker:
            return _write_report(
                report_json=report_json,
                history_input_json=history_input_json,
                verdict=TrackBMgcCandleHistoryProducerVerdict.BLOCKED_INVALID_INPUT,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=actual_source_id,
                history_payload_path=history_payload_path,
                current_quote_report_path=current_quote_report_path,
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                allowlisted_local_symbol=allowlisted_local_symbol,
                dataset=dataset,
                timeframe=timeframe,
                max_candles=max_candles,
                min_candles=min_candles,
                candles=bounded_candles,
                current_quote=current_quote_report_payload,
                history_input=None,
                history_provider_mode=history_provider_mode,
                primary_blocker=input_blocker,
                required_next_action="Fix the explicit MGC candle-history input before producing Track B market-history work.",
            )
        if quote_blocker:
            return _write_report(
                report_json=report_json,
                history_input_json=history_input_json,
                verdict=TrackBMgcCandleHistoryProducerVerdict.BLOCKED_NON_REALTIME_CURRENT_EVIDENCE,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=actual_source_id,
                history_payload_path=history_payload_path,
                current_quote_report_path=current_quote_report_path,
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                allowlisted_local_symbol=allowlisted_local_symbol,
                dataset=dataset,
                timeframe=timeframe,
                max_candles=max_candles,
                min_candles=min_candles,
                candles=bounded_candles,
                current_quote=current_quote_report_payload,
                history_input=None,
                history_provider_mode=history_provider_mode,
                primary_blocker=quote_blocker,
                required_next_action="Run the Track B realtime Databento quote path and provide a current quote report before producing strategy history.",
            )
        if len(bounded_candles) < min_candles:
            return _write_report(
                report_json=report_json,
                history_input_json=history_input_json,
                verdict=TrackBMgcCandleHistoryProducerVerdict.BLOCKED_INSUFFICIENT_CANDLES,
                now=actual_now,
                producer_id=actual_producer_id,
                source_id=actual_source_id,
                history_payload_path=history_payload_path,
                current_quote_report_path=current_quote_report_path,
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                allowlisted_local_symbol=allowlisted_local_symbol,
                dataset=dataset,
                timeframe=timeframe,
                max_candles=max_candles,
                min_candles=min_candles,
                candles=bounded_candles,
                current_quote=current_quote_report_payload,
                history_input=None,
                history_provider_mode=history_provider_mode,
                primary_blocker=f"At least {min_candles} candles are required; received {len(bounded_candles)}.",
                required_next_action="Collect more bounded 1m MGC candles before running the market-history collector.",
            )
        history_input = _history_input(
            candles=bounded_candles,
            current_quote=current_quote_report_payload,
            producer_id=actual_producer_id,
            source_id=actual_source_id,
            history_payload_path=history_payload_path,
            current_quote_report_path=current_quote_report_path,
            expected_account_id=expected_account_id,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            allowlisted_local_symbol=allowlisted_local_symbol,
            dataset=dataset,
            timeframe=timeframe,
            strategy_id=strategy_id,
            lane_id=lane_id,
            history_provider_mode=history_provider_mode,
            generated_at=actual_now,
        )
        return _write_report(
            report_json=report_json,
            history_input_json=history_input_json,
            verdict=TrackBMgcCandleHistoryProducerVerdict.WROTE_HISTORY_INPUT,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=actual_source_id,
            history_payload_path=history_payload_path,
            current_quote_report_path=current_quote_report_path,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            allowlisted_local_symbol=allowlisted_local_symbol,
            dataset=dataset,
            timeframe=timeframe,
            max_candles=max_candles,
            min_candles=min_candles,
            candles=bounded_candles,
            current_quote=current_quote_report_payload,
            history_input=history_input,
            history_provider_mode=history_provider_mode,
            primary_blocker=None,
            required_next_action="Run track_b_market_history_cli on the produced bounded candle-history input.",
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_report(
            report_json=report_json,
            history_input_json=history_input_json,
            verdict=TrackBMgcCandleHistoryProducerVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            producer_id=actual_producer_id,
            source_id=actual_source_id,
            history_payload_path=history_payload_path,
            current_quote_report_path=current_quote_report_path,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            allowlisted_local_symbol=allowlisted_local_symbol,
            dataset=dataset,
            timeframe=timeframe,
            max_candles=max_candles,
            min_candles=min_candles,
            candles=[],
            current_quote=current_quote_report_payload,
            history_input=None,
            history_provider_mode=history_provider_mode,
            primary_blocker=str(exc),
            required_next_action="Fix Track B MGC candle-history producer input schema before retrying.",
        )


def fetch_databento_ohlcv_1m_records(
    *,
    transport: DatabentoCandleHistoryTransport,
    api_key: str,
    dataset: str,
    symbol: str,
    stype_in: str,
    start: datetime,
    end: datetime,
    max_candles: int,
    base_url: str = "https://hist.databento.com/v0",
    schema: str = "ohlcv-1m",
) -> tuple[Mapping[str, Any], ...]:
    require_aware_datetime(start, "start")
    require_aware_datetime(end, "end")
    if end <= start:
        raise ValueError("Databento candle-history end must be after start.")
    if max_candles <= 0:
        raise ValueError("max_candles must be positive.")
    return tuple(
        transport.request_records(
            base_url=base_url,
            api_key=api_key,
            dataset=dataset,
            symbol=symbol,
            schema=schema,
            start=start.astimezone(timezone.utc),
            end=end.astimezone(timezone.utc),
            stype_in=stype_in,
            limit=max_candles,
        )
    )


def write_provider_error_report(
    *,
    primary_blocker: str,
    required_next_action: str,
    expected_account_id: str,
    strategy_id: str,
    lane_id: str,
    contract_key: str = MGC_CONTRACT_KEY,
    databento_continuous_symbol: str = MGC_DATABENTO_CONTINUOUS_SYMBOL,
    allowlisted_local_symbol: str = MGC_LOCAL_SYMBOL,
    dataset: str = MGC_DATASET,
    timeframe: str = "1m",
    output_root: Path = DEFAULT_TRACK_B_MGC_CANDLE_HISTORY_PRODUCER_OUTPUT_ROOT,
    source_id: str | None = None,
    producer_id: str | None = None,
    now: datetime | None = None,
) -> TrackBMgcCandleHistoryProducerResult:
    del expected_account_id, strategy_id, lane_id
    actual_now = now or datetime.now(timezone.utc)
    require_aware_datetime(actual_now, "now")
    actual_producer_id = producer_id or f"track_b_mgc_candle_history_producer_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_producer_id / "track_b_mgc_candle_history_producer_report.json"
    history_input_json = Path(output_root) / actual_producer_id / "track_b_mgc_candle_history_input.json"
    return _write_report(
        report_json=report_json,
        history_input_json=history_input_json,
        verdict=TrackBMgcCandleHistoryProducerVerdict.BLOCKED_PROVIDER_ERROR,
        now=actual_now,
        producer_id=actual_producer_id,
        source_id=source_id or "track_b_mgc_candle_history_producer",
        history_payload_path=None,
        current_quote_report_path=None,
        contract_key=contract_key,
        databento_continuous_symbol=databento_continuous_symbol,
        allowlisted_local_symbol=allowlisted_local_symbol,
        dataset=dataset,
        timeframe=timeframe,
        max_candles=0,
        min_candles=0,
        candles=[],
        current_quote={},
        history_input=None,
        history_provider_mode="DATABENTO_HISTORICAL_BOUNDED_WITH_REALTIME_CURRENT",
        primary_blocker=primary_blocker,
        required_next_action=required_next_action,
    )


def _raw_candles(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> Sequence[Mapping[str, Any]]:
    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, Mapping)):
        raw = payload
    elif isinstance(payload, Mapping):
        raw = payload.get("candles") or payload.get("candle_history") or payload.get("bars") or payload.get("ohlcv") or payload.get("records")
        if raw is None:
            raw = [payload] if payload.get("close") is not None or payload.get("last") is not None else []
    else:
        raise ValueError("history_payload must be a JSON object or list of candle objects.")
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("candles/candle_history/bars/ohlcv/records must be a list of objects.")
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, Mapping):
            raise ValueError(f"candles[{index}] must be an object.")
    return raw


def _normalize_candle(item: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    close = _decimal(_first_value(item, "close", "last", "price"), f"candles[{index}].close")
    open_price = _decimal(_first_value(item, "open", "open_price", default=close), f"candles[{index}].open")
    high = _decimal(_first_value(item, "high", "high_price", default=close), f"candles[{index}].high")
    low = _decimal(_first_value(item, "low", "low_price", default=close), f"candles[{index}].low")
    timestamp = _required_text(
        _first_value(item, "candle_timestamp", "timestamp", "ts_event", "bar_timestamp", "observed_at"),
        f"candles[{index}].timestamp",
    )
    volume_value = _first_value(item, "volume", "size", default=None)
    return {
        "candle_timestamp": timestamp,
        "timestamp": timestamp,
        "observed_at": _optional_text(item.get("observed_at")) or timestamp,
        "open": _decimal_text(open_price),
        "high": _decimal_text(high),
        "low": _decimal_text(low),
        "close": _decimal_text(close),
        "volume": None if volume_value in {None, ""} else _decimal_text(_decimal(volume_value, f"candles[{index}].volume")),
        "provider_symbol": item.get("provider_symbol") or item.get("symbol") or item.get("raw_symbol"),
        "record_type": item.get("record_type"),
    }


def _current_quote_blocker(quote: Mapping[str, Any]) -> str | None:
    if not quote:
        return "Current quote report is required."
    if _optional_text(quote.get("quote_provider_mode")) != "REALTIME":
        return "Current quote report is not quote_provider_mode=REALTIME."
    if quote.get("realtime_quote_received") is not True:
        return "Current quote report realtime_quote_received is not true."
    if quote.get("current_quote_available") is not True:
        return "Current quote report current_quote_available is not true."
    if _optional_text(quote.get("classification")) not in {None, "CURRENT_QUOTE_AVAILABLE"}:
        return f"Current quote report classification is not available: {quote.get('classification')}"
    return None


def _input_blocker(
    *,
    history_payload: Mapping[str, Any] | Sequence[Mapping[str, Any]],
    expected_account_id: str,
    contract_key: str,
) -> str | None:
    if not isinstance(history_payload, Mapping):
        return None
    payload_account_id = _optional_text(history_payload.get("account_id") or history_payload.get("expected_account_id"))
    if payload_account_id and payload_account_id != expected_account_id:
        return f"History payload account_id {payload_account_id} does not match expected_account_id {expected_account_id}."
    payload_contract_key = _optional_text(history_payload.get("contract_key") or history_payload.get("local_execution_contract_key"))
    if payload_contract_key and payload_contract_key != contract_key:
        return f"History payload contract_key {payload_contract_key} does not match {contract_key}."
    provider_mode = _optional_text(history_payload.get("quote_provider_mode") or history_payload.get("provider_mode"))
    if provider_mode in {"HISTORICAL_AVAILABLE_END", "FIXTURE_STALE", "STALE"}:
        return f"History payload provider mode {provider_mode} is diagnostic-only for Track B strategy features."
    return None


def _history_input(
    *,
    candles: Sequence[Mapping[str, Any]],
    current_quote: Mapping[str, Any],
    producer_id: str,
    source_id: str,
    history_payload_path: Path | None,
    current_quote_report_path: Path | None,
    expected_account_id: str,
    contract_key: str,
    databento_continuous_symbol: str,
    allowlisted_local_symbol: str,
    dataset: str,
    timeframe: str,
    strategy_id: str,
    lane_id: str,
    history_provider_mode: str,
    generated_at: datetime,
) -> dict[str, Any]:
    latest = candles[-1]
    quote_report_path = _optional_text(current_quote.get("report_json_path")) or (
        None if current_quote_report_path is None else str(current_quote_report_path)
    )
    return {
        "schema_version": "track_b_mgc_candle_history_input_v1",
        "generated_at": generated_at.isoformat(),
        "source_id": source_id,
        "batch_id": f"track_b_mgc_candle_history_batch_{uuid.uuid4().hex}",
        "track_b_mgc_candle_history_producer_id": producer_id,
        "account_id": expected_account_id,
        "contract_key": contract_key,
        "local_execution_contract_key": contract_key,
        "instrument_family": "MGC",
        "symbol": allowlisted_local_symbol,
        "allowlisted_local_symbol": allowlisted_local_symbol,
        "databento_continuous_symbol": databento_continuous_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "strategy_id": strategy_id,
        "lane_id": lane_id,
        "candle_timestamp": latest.get("candle_timestamp"),
        "observed_at": latest.get("observed_at"),
        "open": latest.get("open"),
        "high": latest.get("high"),
        "low": latest.get("low"),
        "close": latest.get("close"),
        "volume": latest.get("volume"),
        "candles": list(candles),
        "candle_history": list(candles),
        "history_provider_mode": history_provider_mode,
        "history_schema": "ohlcv-1m",
        "quote_provider_mode": "REALTIME",
        "realtime_quote_received": True,
        "current_quote_available": True,
        "quote_freshness_verdict": current_quote.get("quote_freshness_verdict"),
        "metadata": {
            "track_b_mgc_candle_history_producer_boundary": "track_b_mgc_candle_history_producer",
            "track_b_mgc_candle_history_producer_id": producer_id,
            "market_data_provider": "DATABENTO",
            "market_data_role": "EVIDENCE_ONLY",
            "history_provider_mode": history_provider_mode,
            "history_payload_path": None if history_payload_path is None else str(history_payload_path),
            "source_report_path": quote_report_path,
            "current_quote_report_path": quote_report_path,
            "quote_provider_mode": "REALTIME",
            "realtime_quote_received": True,
            "current_quote_available": True,
            "quote_freshness_verdict": current_quote.get("quote_freshness_verdict"),
            "quote_timestamp": current_quote.get("timestamp"),
            "quote_age_seconds": current_quote.get("quote_age_seconds"),
            "databento_is_execution_authority": False,
            "local_execution_contract_key_remains_authority": True,
            "history_is_bounded": True,
            "max_candle_count": len(candles),
        },
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _write_report(
    *,
    report_json: Path,
    history_input_json: Path,
    verdict: TrackBMgcCandleHistoryProducerVerdict,
    now: datetime,
    producer_id: str,
    source_id: str,
    history_payload_path: Path | None,
    current_quote_report_path: Path | None,
    contract_key: str,
    databento_continuous_symbol: str,
    allowlisted_local_symbol: str,
    dataset: str,
    timeframe: str,
    max_candles: int,
    min_candles: int,
    candles: Sequence[Mapping[str, Any]],
    current_quote: Mapping[str, Any],
    history_input: dict[str, Any] | None,
    history_provider_mode: str,
    primary_blocker: str | None,
    required_next_action: str,
) -> TrackBMgcCandleHistoryProducerResult:
    latest_report_json = report_json.parent.parent / "latest_track_b_mgc_candle_history_producer_report.json"
    latest_input_json = report_json.parent.parent / "latest_track_b_mgc_candle_history_input.json"
    wrote_input = history_input is not None and verdict == TrackBMgcCandleHistoryProducerVerdict.WROTE_HISTORY_INPUT
    report = {
        "schema_version": "track_b_mgc_candle_history_producer_report_v1",
        "generated_at": now.isoformat(),
        "track_b_mgc_candle_history_producer_id": producer_id,
        "candle_history_producer_verdict": verdict.value,
        "source_id": source_id,
        "contract_key": contract_key,
        "symbol": allowlisted_local_symbol,
        "allowlisted_local_symbol": allowlisted_local_symbol,
        "databento_continuous_symbol": databento_continuous_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "history_provider_mode": history_provider_mode,
        "quote_provider_mode": current_quote.get("quote_provider_mode") or "NOT_PROVIDED",
        "realtime_quote_received": current_quote.get("realtime_quote_received") if current_quote else False,
        "current_quote_available": current_quote.get("current_quote_available") if current_quote else False,
        "quote_freshness_verdict": current_quote.get("quote_freshness_verdict") or "NOT_PROVIDED",
        "current_quote_report_path": None if current_quote_report_path is None else str(current_quote_report_path),
        "history_payload_path": None if history_payload_path is None else str(history_payload_path),
        "requested_max_candles": max_candles,
        "min_candles": min_candles,
        "candles_produced": len(candles),
        "latest_candle_timestamp": candles[-1].get("candle_timestamp") if candles else None,
        "output_history_input_path": str(history_input_json) if wrote_input else None,
        "latest_history_input_path": str(latest_input_json) if wrote_input else None,
        "primary_blocker": primary_blocker,
        "secondary_blockers": [],
        "required_next_action": required_next_action,
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
        "paper_proof_cli_called": False,
        "listener_invoked": False,
        "runner_invoked": False,
        "broker_connection_attempted": False,
        "tws_connection_attempted": False,
        "ibkr_connection_attempted": False,
        "place_order_called": False,
        "cancel_called": False,
        "report_json_path": str(report_json),
        "latest_report_json_path": str(latest_report_json),
    }
    payload = json.dumps(to_jsonable(report), indent=2, sort_keys=True)
    report_json.parent.mkdir(parents=True, exist_ok=True)
    report_json.write_text(payload, encoding="utf-8")
    latest_report_json.parent.mkdir(parents=True, exist_ok=True)
    latest_report_json.write_text(payload, encoding="utf-8")
    if history_input is not None:
        input_payload = json.dumps(to_jsonable(history_input), indent=2, sort_keys=True)
        history_input_json.write_text(input_payload, encoding="utf-8")
        latest_input_json.write_text(input_payload, encoding="utf-8")
    return TrackBMgcCandleHistoryProducerResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        history_input_json=history_input_json if history_input is not None else None,
        history_input=history_input,
    )


def _source_id(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]]) -> str | None:
    if isinstance(payload, Mapping):
        return _optional_text(payload.get("source_id"))
    return None


def _first_value(record: Mapping[str, Any], *keys: str, default: object | None = None) -> object | None:
    for key in keys:
        value = record.get(key)
        if value not in {None, ""}:
            return value
    return default


def _decimal(value: object, field_name: str) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible.") from exc


def _decimal_text(value: Decimal) -> str:
    return format(value.normalize(), "f")


def _required_text(value: object, field_name: str) -> str:
    text = _optional_text(value)
    if text is None:
        raise ValueError(f"{field_name} is required.")
    return text


def _optional_text(value: object) -> str | None:
    text = str(value or "").strip()
    return text or None


def provider_error_message(exc: Exception) -> str:
    if isinstance(exc, DatabentoAvailableEndError):
        available_end = exc.provider_available_end.isoformat() if exc.provider_available_end is not None else "UNKNOWN"
        return f"Databento candle-history request was after available_end={available_end}: {exc}"
    if isinstance(exc, DatabentoQuoteProviderError):
        return f"Databento candle-history provider error: {exc}"
    return f"Databento candle-history producer error: {exc}"
