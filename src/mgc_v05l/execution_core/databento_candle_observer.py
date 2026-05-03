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
from typing import Any, Mapping

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
                primary_blocker="Databento market-data payload did not contain an observed quote/candle.",
                required_next_action="Provide a Databento quote/candle artifact with observed market data before producing a Track B event.",
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
            primary_blocker=None,
            required_next_action="Run strategy_signal_adapter_cli explicitly if this no-submit market-data event should enter the listener inbox.",
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
            primary_blocker=str(exc),
            required_next_action="Fix Databento candle observer input before retrying.",
        )


def _is_no_record_payload(payload: Mapping[str, Any]) -> bool:
    quote_observed = payload.get("quote_observed")
    if quote_observed is False:
        return True
    if str(payload.get("classification") or "").strip().upper().endswith("NO_RECORDS"):
        return True
    return False


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
    primary_blocker: str | None,
    required_next_action: str,
) -> DatabentoCandleObserverResult:
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
        "market_data_connection_attempted": False,
        "databento_connection_attempted": False,
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
