"""Track B bounded MGC market-history collector.

This boundary normalizes explicit MGC quote/candle history into the bounded
history event consumed by ``track_b_feature_builder``. It is market-data
evidence only: no broker access, no strategy execution, and no submit path.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import require_aware_datetime, to_jsonable


DEFAULT_TRACK_B_MARKET_HISTORY_OUTPUT_ROOT = Path("outputs/track_b_execution_core/track_b_market_history")
MGC_CONTRACT_KEY = "MGC-202606"
MGC_INSTRUMENT_FAMILY = "MGC"


class TrackBMarketHistoryVerdict(str, Enum):
    WROTE_HISTORY_EVENT = "TRACK_B_MARKET_HISTORY_WROTE_HISTORY_EVENT"
    BLOCKED_INSUFFICIENT_HISTORY = "TRACK_B_MARKET_HISTORY_BLOCKED_INSUFFICIENT_HISTORY"
    BLOCKED_NON_REALTIME_INPUT = "TRACK_B_MARKET_HISTORY_BLOCKED_NON_REALTIME_INPUT"
    BLOCKED_INVALID_INPUT = "TRACK_B_MARKET_HISTORY_BLOCKED_INVALID_INPUT"
    BLOCKED_SCHEMA_ERROR = "TRACK_B_MARKET_HISTORY_BLOCKED_SCHEMA_ERROR"


@dataclass(frozen=True)
class TrackBMarketHistoryResult:
    verdict: TrackBMarketHistoryVerdict
    report_json: Path
    report: dict[str, Any]
    history_event_json: Path | None
    history_event: dict[str, Any] | None


def collect_track_b_mgc_market_history(
    *,
    market_history_payload: Mapping[str, Any],
    source_payload_path: Path | None,
    expected_account_id: str | None = None,
    contract_key: str = MGC_CONTRACT_KEY,
    databento_continuous_symbol: str = "MGC.v.0",
    dataset: str = "GLBX.MDP3",
    allowlisted_local_symbol: str | None = "MGCM6",
    timeframe: str = "1m",
    max_candles: int = 50,
    min_candles: int = 3,
    output_root: Path = DEFAULT_TRACK_B_MARKET_HISTORY_OUTPUT_ROOT,
    collector_id: str | None = None,
    source_id: str | None = None,
    strategy_id: str | None = None,
    lane_id: str | None = None,
    now: datetime | None = None,
) -> TrackBMarketHistoryResult:
    actual_now = now or datetime.now(UTC)
    require_aware_datetime(actual_now, "now")
    actual_collector_id = collector_id or f"track_b_market_history_{uuid.uuid4().hex}"
    report_json = Path(output_root) / actual_collector_id / "track_b_market_history_report.json"
    event_json = Path(output_root) / actual_collector_id / "track_b_market_history_event.json"
    actual_source_id = source_id or _optional_text(market_history_payload.get("source_id")) or "track_b_market_history"

    try:
        if max_candles <= 0:
            raise ValueError("max_candles must be positive.")
        if min_candles <= 0:
            raise ValueError("min_candles must be positive.")
        quote_evidence = _quote_evidence(market_history_payload)
        raw_candles = _raw_candles(market_history_payload)
        candles = [_normalize_candle(item, index=index) for index, item in enumerate(raw_candles, start=1)]
        bounded_candles = candles[-max_candles:]
        validation_blocker = _validation_blocker(
            payload=market_history_payload,
            candles=bounded_candles,
            expected_account_id=expected_account_id,
            expected_contract_key=contract_key,
            quote_evidence=quote_evidence,
        )
        if validation_blocker:
            verdict = (
                TrackBMarketHistoryVerdict.BLOCKED_NON_REALTIME_INPUT
                if "REALTIME" in validation_blocker or "realtime" in validation_blocker or "current_quote_available" in validation_blocker
                else TrackBMarketHistoryVerdict.BLOCKED_INVALID_INPUT
            )
            return _write_report(
                report_json=report_json,
                event_json=event_json,
                verdict=verdict,
                now=actual_now,
                collector_id=actual_collector_id,
                source_id=actual_source_id,
                source_payload_path=source_payload_path,
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                allowlisted_local_symbol=allowlisted_local_symbol,
                timeframe=timeframe,
                max_candles=max_candles,
                min_candles=min_candles,
                candles=bounded_candles,
                quote_evidence=quote_evidence,
                history_event=None,
                primary_blocker=validation_blocker,
                required_next_action="Provide current realtime MGC market-history evidence before feature building.",
            )
        if len(bounded_candles) < min_candles:
            return _write_report(
                report_json=report_json,
                event_json=event_json,
                verdict=TrackBMarketHistoryVerdict.BLOCKED_INSUFFICIENT_HISTORY,
                now=actual_now,
                collector_id=actual_collector_id,
                source_id=actual_source_id,
                source_payload_path=source_payload_path,
                contract_key=contract_key,
                databento_continuous_symbol=databento_continuous_symbol,
                dataset=dataset,
                allowlisted_local_symbol=allowlisted_local_symbol,
                timeframe=timeframe,
                max_candles=max_candles,
                min_candles=min_candles,
                candles=bounded_candles,
                quote_evidence=quote_evidence,
                history_event=None,
                primary_blocker=f"At least {min_candles} candles are required; received {len(bounded_candles)}.",
                required_next_action="Collect more bounded MGC realtime/history candles before feature building.",
            )
        history_event = _history_event(
            payload=market_history_payload,
            candles=bounded_candles,
            quote_evidence=quote_evidence,
            collector_id=actual_collector_id,
            source_id=actual_source_id,
            source_payload_path=source_payload_path,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            allowlisted_local_symbol=allowlisted_local_symbol,
            timeframe=timeframe,
            strategy_id=strategy_id,
            lane_id=lane_id,
        )
        return _write_report(
            report_json=report_json,
            event_json=event_json,
            verdict=TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT,
            now=actual_now,
            collector_id=actual_collector_id,
            source_id=actual_source_id,
            source_payload_path=source_payload_path,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            allowlisted_local_symbol=allowlisted_local_symbol,
            timeframe=timeframe,
            max_candles=max_candles,
            min_candles=min_candles,
            candles=bounded_candles,
            quote_evidence=quote_evidence,
            history_event=history_event,
            primary_blocker=None,
            required_next_action="Run track_b_feature_builder_cli on this bounded market-history event.",
        )
    except (TypeError, ValueError, OSError, json.JSONDecodeError) as exc:
        return _write_report(
            report_json=report_json,
            event_json=event_json,
            verdict=TrackBMarketHistoryVerdict.BLOCKED_SCHEMA_ERROR,
            now=actual_now,
            collector_id=actual_collector_id,
            source_id=actual_source_id,
            source_payload_path=source_payload_path,
            contract_key=contract_key,
            databento_continuous_symbol=databento_continuous_symbol,
            dataset=dataset,
            allowlisted_local_symbol=allowlisted_local_symbol,
            timeframe=timeframe,
            max_candles=max_candles,
            min_candles=min_candles,
            candles=[],
            quote_evidence={},
            history_event=None,
            primary_blocker=str(exc),
            required_next_action="Fix Track B market-history input schema before retrying.",
        )


def _raw_candles(payload: Mapping[str, Any]) -> Sequence[Mapping[str, Any]]:
    raw = payload.get("candles") or payload.get("candle_history") or payload.get("candle_items") or payload.get("bars") or payload.get("ohlcv")
    if raw is None:
        raw = [payload] if payload.get("close") is not None or payload.get("last") is not None else []
    if not isinstance(raw, Sequence) or isinstance(raw, (str, bytes)):
        raise ValueError("candles/candle_history must be a list of objects.")
    for index, item in enumerate(raw, start=1):
        if not isinstance(item, Mapping):
            raise ValueError(f"candles[{index}] must be an object.")
    return raw


def _normalize_candle(item: Mapping[str, Any], *, index: int) -> dict[str, Any]:
    close = _decimal(item.get("close") if item.get("close") is not None else item.get("last"), f"candles[{index}].close")
    open_price = _decimal(item.get("open") if item.get("open") is not None else close, f"candles[{index}].open")
    high = _decimal(item.get("high") if item.get("high") is not None else close, f"candles[{index}].high")
    low = _decimal(item.get("low") if item.get("low") is not None else close, f"candles[{index}].low")
    timestamp = _required_text(item.get("candle_timestamp") or item.get("timestamp") or item.get("observed_at") or item.get("ts_event"), f"candles[{index}].timestamp")
    return {
        "candle_timestamp": timestamp,
        "observed_at": _optional_text(item.get("observed_at")) or timestamp,
        "open": _decimal_text(open_price),
        "high": _decimal_text(high),
        "low": _decimal_text(low),
        "close": _decimal_text(close),
        "volume": None if item.get("volume") in {None, ""} else _decimal_text(_decimal(item.get("volume"), f"candles[{index}].volume")),
        "raw_symbol": item.get("raw_symbol"),
        "provider_symbol": item.get("provider_symbol") or item.get("symbol"),
    }


def _quote_evidence(payload: Mapping[str, Any]) -> dict[str, Any]:
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    quote_report_path = metadata.get("source_report_path") or payload.get("source_report_path") or payload.get("quote_report_path")
    quote_report = _read_optional_json(quote_report_path)
    return {
        "quote_provider_mode": _first_text(
            metadata.get("quote_provider_mode"),
            payload.get("quote_provider_mode"),
            None if quote_report is None else quote_report.get("quote_provider_mode"),
            None if quote_report is None else quote_report.get("market_data_mode"),
        ),
        "realtime_quote_received": _first_bool(
            metadata.get("realtime_quote_received"),
            payload.get("realtime_quote_received"),
            None if quote_report is None else quote_report.get("realtime_quote_received"),
        ),
        "current_quote_available": _first_bool(
            metadata.get("current_quote_available"),
            payload.get("current_quote_available"),
            None if quote_report is None else quote_report.get("current_quote_available"),
        ),
        "quote_freshness_verdict": _first_text(
            metadata.get("quote_freshness_verdict"),
            payload.get("quote_freshness_verdict"),
            None if quote_report is None else quote_report.get("quote_freshness_verdict"),
        ),
        "quote_report_path": _optional_text(quote_report_path),
    }


def _validation_blocker(
    *,
    payload: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    expected_account_id: str | None,
    expected_contract_key: str,
    quote_evidence: Mapping[str, Any],
) -> str | None:
    if not candles:
        return "Market history input contains no candles."
    account_id = _optional_text(payload.get("account_id") or payload.get("expected_account_id"))
    if expected_account_id and account_id and account_id != expected_account_id:
        return f"Market history account_id {account_id} does not match expected_account_id {expected_account_id}."
    contract_key = _optional_text(payload.get("contract_key") or payload.get("local_execution_contract_key"))
    if contract_key != expected_contract_key:
        return f"Only {expected_contract_key} is supported by this Track B market-history collector."
    instrument_family = _optional_text(payload.get("instrument_family"))
    if instrument_family and instrument_family != MGC_INSTRUMENT_FAMILY:
        return f"Only instrument_family={MGC_INSTRUMENT_FAMILY} is supported by this Track B market-history collector."
    if quote_evidence.get("quote_provider_mode") != "REALTIME":
        return "Market history input is not explicitly REALTIME."
    if quote_evidence.get("realtime_quote_received") is not True:
        return "Market history input realtime_quote_received is not true."
    if quote_evidence.get("current_quote_available") is not True:
        return "Market history input current_quote_available is not true."
    return None


def _history_event(
    *,
    payload: Mapping[str, Any],
    candles: Sequence[Mapping[str, Any]],
    quote_evidence: Mapping[str, Any],
    collector_id: str,
    source_id: str,
    source_payload_path: Path | None,
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    allowlisted_local_symbol: str | None,
    timeframe: str,
    strategy_id: str | None,
    lane_id: str | None,
) -> dict[str, Any]:
    metadata = payload.get("metadata") if isinstance(payload.get("metadata"), Mapping) else {}
    latest = candles[-1]
    return {
        "schema_version": "track_b_market_history_event_v1",
        "source_id": source_id,
        "batch_id": _optional_text(payload.get("batch_id")) or f"track_b_market_history_batch_{uuid.uuid4().hex}",
        "account_id": _required_text(payload.get("account_id") or payload.get("expected_account_id"), "account_id"),
        "contract_key": contract_key,
        "instrument_family": _optional_text(payload.get("instrument_family")) or MGC_INSTRUMENT_FAMILY,
        "symbol": allowlisted_local_symbol or MGC_INSTRUMENT_FAMILY,
        "databento_continuous_symbol": databento_continuous_symbol,
        "dataset": dataset,
        "allowlisted_local_symbol": allowlisted_local_symbol,
        "strategy_id": _required_text(strategy_id or payload.get("strategy_id"), "strategy_id"),
        "lane_id": _required_text(lane_id or payload.get("lane_id"), "lane_id"),
        "timeframe": timeframe,
        "candle_timestamp": latest.get("candle_timestamp"),
        "observed_at": latest.get("observed_at"),
        "open": latest.get("open"),
        "high": latest.get("high"),
        "low": latest.get("low"),
        "close": latest.get("close"),
        "volume": latest.get("volume"),
        "candles": list(candles),
        "candle_history": list(candles),
        "quote_provider_mode": quote_evidence.get("quote_provider_mode"),
        "realtime_quote_received": quote_evidence.get("realtime_quote_received"),
        "current_quote_available": quote_evidence.get("current_quote_available"),
        "quote_freshness_verdict": quote_evidence.get("quote_freshness_verdict"),
        "metadata": {
            **dict(metadata),
            "track_b_market_history_boundary": "track_b_market_history",
            "track_b_market_history_id": collector_id,
            "market_data_role": "EVIDENCE_ONLY",
            "dataset": dataset,
            "databento_continuous_symbol": databento_continuous_symbol,
            "allowlisted_local_symbol": allowlisted_local_symbol,
            "source_payload_path": None if source_payload_path is None else str(source_payload_path),
            "source_report_path": quote_evidence.get("quote_report_path") or metadata.get("source_report_path"),
            "quote_provider_mode": quote_evidence.get("quote_provider_mode"),
            "realtime_quote_received": quote_evidence.get("realtime_quote_received"),
            "current_quote_available": quote_evidence.get("current_quote_available"),
            "quote_freshness_verdict": quote_evidence.get("quote_freshness_verdict"),
            "databento_is_execution_authority": False,
            "local_execution_contract_key_remains_authority": True,
        },
        "submit_allowed": False,
        "submit_attempted": False,
        "live_money_readiness": False,
    }


def _write_report(
    *,
    report_json: Path,
    event_json: Path,
    verdict: TrackBMarketHistoryVerdict,
    now: datetime,
    collector_id: str,
    source_id: str,
    source_payload_path: Path | None,
    contract_key: str,
    databento_continuous_symbol: str,
    dataset: str,
    allowlisted_local_symbol: str | None,
    timeframe: str,
    max_candles: int,
    min_candles: int,
    candles: Sequence[Mapping[str, Any]],
    quote_evidence: Mapping[str, Any],
    history_event: dict[str, Any] | None,
    primary_blocker: str | None,
    required_next_action: str,
) -> TrackBMarketHistoryResult:
    latest_report_json = report_json.parent.parent / "latest_track_b_market_history_report.json"
    latest_event_json = report_json.parent.parent / "latest_track_b_market_history_event.json"
    wrote_event = history_event is not None and verdict == TrackBMarketHistoryVerdict.WROTE_HISTORY_EVENT
    report = {
        "schema_version": "track_b_market_history_report_v1",
        "generated_at": now.isoformat(),
        "track_b_market_history_id": collector_id,
        "market_history_verdict": verdict.value,
        "source_id": source_id,
        "source_payload_path": None if source_payload_path is None else str(source_payload_path),
        "contract_key": contract_key,
        "databento_continuous_symbol": databento_continuous_symbol,
        "allowlisted_local_symbol": allowlisted_local_symbol,
        "dataset": dataset,
        "timeframe": timeframe,
        "requested_max_candles": max_candles,
        "min_candles": min_candles,
        "candles_collected": len(candles),
        "latest_candle_timestamp": candles[-1].get("candle_timestamp") if candles else None,
        "quote_provider_mode": quote_evidence.get("quote_provider_mode") or "NOT_PROVIDED",
        "realtime_quote_received": quote_evidence.get("realtime_quote_received") if quote_evidence else False,
        "current_quote_available": quote_evidence.get("current_quote_available") if quote_evidence else False,
        "quote_freshness_verdict": quote_evidence.get("quote_freshness_verdict") or "NOT_PROVIDED",
        "quote_report_path": quote_evidence.get("quote_report_path"),
        "output_history_event_path": str(event_json) if wrote_event else None,
        "latest_history_event_path": str(latest_event_json) if wrote_event else None,
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
    if history_event is not None:
        event_payload = json.dumps(to_jsonable(history_event), indent=2, sort_keys=True)
        event_json.write_text(event_payload, encoding="utf-8")
        latest_event_json.write_text(event_payload, encoding="utf-8")
    return TrackBMarketHistoryResult(
        verdict=verdict,
        report_json=report_json,
        report=report,
        history_event_json=event_json if history_event is not None else None,
        history_event=history_event,
    )


def _read_optional_json(path_value: object) -> dict[str, Any] | None:
    path_text = _optional_text(path_value)
    if not path_text:
        return None
    path = Path(path_text)
    if not path.exists():
        return None
    value = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(value, dict):
        raise ValueError(f"{path} must contain a JSON object.")
    return value


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


def _first_text(*values: object) -> str | None:
    for value in values:
        text = _optional_text(value)
        if text is not None:
            return text
    return None


def _first_bool(*values: object) -> bool | None:
    for value in values:
        if isinstance(value, bool):
            return value
    return None
