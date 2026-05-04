"""Track B Databento current quote boundary.

This module is intentionally transport-injected. Tests use fakes; real Databento
network access must be added behind an explicit operator path.
"""

from __future__ import annotations

import json
import uuid
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from enum import Enum
from pathlib import Path
from typing import Any, Mapping, Protocol

from .databento_quote_provider import DatabentoQuoteProvider, DatabentoQuoteProviderConfig
from .models import require_aware_datetime, to_jsonable
from .pricing import MarketDataMode, MarketDataRole
from .quote_provider import QuoteSnapshot


class CurrentQuoteClassification(str, Enum):
    AVAILABLE = "CURRENT_QUOTE_AVAILABLE"
    UNAVAILABLE = "CURRENT_QUOTE_UNAVAILABLE"
    STALE = "CURRENT_QUOTE_STALE"
    PROVIDER_ERROR = "CURRENT_QUOTE_PROVIDER_ERROR"
    MARKET_CLOSED_OR_NO_RECORDS = "CURRENT_QUOTE_MARKET_CLOSED_OR_NO_RECORDS"


DEFAULT_CURRENT_QUOTE_OUTPUT_ROOT = Path("outputs/track_b_execution_core/current_quotes")


class DatabentoCurrentQuoteTransport(Protocol):
    def get_current_quote(
        self,
        *,
        dataset: str,
        symbol: str,
        stype_in: str,
        schema: str,
        contract_key: str,
    ) -> Mapping[str, Any] | None: ...


@dataclass(frozen=True)
class DatabentoCurrentQuoteConfig:
    contract_key: str
    dataset: str = "GLBX.MDP3"
    databento_continuous_symbol: str | None = None
    databento_symbol: str | None = None
    stype_in: str = "continuous"
    schema: str = "mbp-1"
    allowlisted_local_symbol: str | None = None
    tick_size: str = "0.1"
    exchange: str = "COMEX"
    currency: str = "USD"
    max_age_seconds: int = 15
    output_root: Path = DEFAULT_CURRENT_QUOTE_OUTPUT_ROOT

    def selector_symbol(self) -> str:
        symbol = str(self.databento_continuous_symbol or self.databento_symbol or "").strip()
        if not symbol:
            raise ValueError("databento_continuous_symbol or databento_symbol is required")
        return symbol

    def selector_source(self) -> str:
        return "DATABENTO_CONTINUOUS_SYMBOL" if self.databento_continuous_symbol else "DATABENTO_RAW_SYMBOL_OVERRIDE"


@dataclass(frozen=True)
class CurrentQuoteResult:
    classification: CurrentQuoteClassification
    report_json: Path
    report: dict[str, Any]
    quote: QuoteSnapshot | None = None


class DatabentoCurrentQuoteProvider:
    provider_name = "DATABENTO"

    def __init__(self, *, config: DatabentoCurrentQuoteConfig, transport: DatabentoCurrentQuoteTransport) -> None:
        self.config = config
        self.transport = transport

    def fetch_current_quote(
        self,
        *,
        run_id: str | None = None,
        now: datetime | None = None,
    ) -> CurrentQuoteResult:
        actual_now = now or datetime.now(UTC)
        require_aware_datetime(actual_now, "now")
        actual_run_id = run_id or f"databento_current_quote_{uuid.uuid4().hex}"
        report_json = Path(self.config.output_root) / actual_run_id / "current_quote_report.json"
        symbol = self.config.selector_symbol()
        try:
            raw_quote = self.transport.get_current_quote(
                dataset=self.config.dataset,
                symbol=symbol,
                stype_in=self.config.stype_in,
                schema=self.config.schema,
                contract_key=self.config.contract_key,
            )
        except Exception as exc:  # noqa: BLE001 - provider errors must become operator reports.
            return self._write_report(
                report_json=report_json,
                classification=CurrentQuoteClassification.PROVIDER_ERROR,
                now=actual_now,
                raw_quote=None,
                quote=None,
                provider_error=str(exc),
                provider_diagnostics=_provider_diagnostics_from_exception(exc),
                no_records_reason=None,
            )
        if raw_quote is None:
            return self._write_report(
                report_json=report_json,
                classification=CurrentQuoteClassification.MARKET_CLOSED_OR_NO_RECORDS,
                now=actual_now,
                raw_quote=None,
                quote=None,
                provider_error=None,
                provider_diagnostics=None,
                no_records_reason="MARKET_CLOSED_OR_NO_RECORDS",
            )
        try:
            quote = self._quote_from_raw(raw_quote)
        except ValueError as exc:
            return self._write_report(
                report_json=report_json,
                classification=CurrentQuoteClassification.UNAVAILABLE,
                now=actual_now,
                raw_quote=raw_quote,
                quote=None,
                provider_error=str(exc),
                provider_diagnostics=_provider_diagnostics_from_raw(raw_quote),
                no_records_reason="UNAVAILABLE_OR_INCOMPLETE_QUOTE",
            )
        age_seconds = quote.age_seconds(actual_now)
        classification = (
            CurrentQuoteClassification.AVAILABLE
            if Decimal("0") <= age_seconds <= Decimal(str(self.config.max_age_seconds))
            else CurrentQuoteClassification.STALE
        )
        return self._write_report(
            report_json=report_json,
            classification=classification,
            now=actual_now,
            raw_quote=raw_quote,
            quote=quote,
            provider_error=None,
            provider_diagnostics=_provider_diagnostics_from_raw(raw_quote),
            no_records_reason=None,
        )

    def _quote_from_raw(self, raw_quote: Mapping[str, Any]) -> QuoteSnapshot:
        timestamp = _parse_timestamp(raw_quote.get("timestamp") or raw_quote.get("ts_event"))
        bid = _required_decimal(raw_quote.get("bid"), "bid")
        ask = _required_decimal(raw_quote.get("ask"), "ask")
        last = raw_quote.get("last")
        return QuoteSnapshot(
            provider=self.provider_name,
            mode=MarketDataMode.REALTIME,
            role=MarketDataRole.PRIMARY,
            contract_key=self.config.contract_key,
            provider_symbol=str(raw_quote.get("provider_symbol") or self.config.selector_symbol()),
            bid=bid,
            ask=ask,
            last=_required_decimal(last, "last") if last is not None else None,
            timestamp=timestamp,
            tick_size=self.config.tick_size,
            exchange=self.config.exchange,
            currency=self.config.currency,
            provider_warnings=tuple(raw_quote.get("provider_warnings") or ()),
            delayed_data_warning_seen=False,
            raw={
                "dataset": self.config.dataset,
                "schema": self.config.schema,
                "symbol_selector": self.config.selector_symbol(),
                "symbol_selector_source": self.config.selector_source(),
                "stype_in": self.config.stype_in,
                "local_execution_contract_key": self.config.contract_key,
                "allowlisted_local_symbol": self.config.allowlisted_local_symbol,
                "databento_continuous_symbol_is_execution_authority": False,
                "ibkr_allowlist_remains_execution_authority": True,
            },
        )

    def _write_report(
        self,
        *,
        report_json: Path,
        classification: CurrentQuoteClassification,
        now: datetime,
        raw_quote: Mapping[str, Any] | None,
        quote: QuoteSnapshot | None,
        provider_error: str | None,
        provider_diagnostics: Mapping[str, Any] | None,
        no_records_reason: str | None,
    ) -> CurrentQuoteResult:
        diagnostics = dict(provider_diagnostics or {})
        quote_age_seconds = str(quote.age_seconds(now)) if quote is not None else None
        quote_available = classification == CurrentQuoteClassification.AVAILABLE
        report = {
            "schema_version": "track_b_databento_current_quote_v1",
            "classification": classification.value,
            "quote_status": classification.value,
            "generated_at": now.isoformat(),
            "market_data_provider": self.provider_name,
            "market_data_mode": MarketDataMode.REALTIME if quote is not None else MarketDataMode.UNKNOWN,
            "market_data_role": MarketDataRole.PRIMARY,
            "dataset": self.config.dataset,
            "schema": self.config.schema,
            "contract_key": self.config.contract_key,
            "local_execution_contract_key": self.config.contract_key,
            "ibkr_allowlist_remains_execution_authority": True,
            "allowlisted_local_symbol": self.config.allowlisted_local_symbol,
            "databento_continuous_symbol": self.config.databento_continuous_symbol,
            "databento_symbol": self.config.databento_symbol,
            "symbol_selector": self.config.selector_symbol(),
            "symbol_selector_source": self.config.selector_source(),
            "databento_continuous_symbol_is_execution_authority": False,
            "quote_observed": quote is not None,
            "bid": str(quote.bid) if quote is not None else None,
            "ask": str(quote.ask) if quote is not None else None,
            "last": str(quote.last) if quote is not None and quote.last is not None else None,
            "timestamp": quote.timestamp.isoformat() if quote is not None else None,
            "quote_age_seconds": quote_age_seconds,
            "max_age_seconds": int(self.config.max_age_seconds),
            "current_quote_available": quote_available,
            "current_executable_quote": quote_available,
            "quote_usable_for_paper_pricing": quote_available,
            "quote_usable_for_live_money_readiness": False,
            "production_live_money_readiness": False,
            "provider_error": provider_error,
            "no_records_reason": no_records_reason,
            "requested_quote_start": diagnostics.get("requested_quote_start"),
            "requested_quote_end": diagnostics.get("requested_quote_end"),
            "actual_quote_start": diagnostics.get("actual_quote_start"),
            "actual_quote_end": diagnostics.get("actual_quote_end"),
            "provider_available_end": diagnostics.get("provider_available_end"),
            "provider_available_end_initial": diagnostics.get("provider_available_end_initial"),
            "provider_available_end_final": diagnostics.get("provider_available_end_final"),
            "available_end_fallback_used": diagnostics.get("available_end_fallback_used", False),
            "available_end_buffer_seconds": diagnostics.get("available_end_buffer_seconds"),
            "allow_available_end_fallback_requested": diagnostics.get("allow_available_end_fallback_requested"),
            "allow_available_end_fallback_effective": diagnostics.get("allow_available_end_fallback_effective"),
            "available_end_retry_attempted": diagnostics.get("available_end_retry_attempted", False),
            "available_end_retry_count": diagnostics.get("available_end_retry_count", 0),
            "available_end_retry_reason": diagnostics.get("available_end_retry_reason"),
            "quote_temporal_scope": diagnostics.get("quote_temporal_scope"),
            "active_session_quote": diagnostics.get("active_session_quote"),
            "native_databento_error_code": diagnostics.get("native_databento_error_code"),
            "native_databento_error_message": diagnostics.get("native_databento_error_message"),
            "raw_quote_keys": sorted(str(key) for key in raw_quote.keys()) if raw_quote is not None else [],
            "submit_enabled": False,
            "place_order_called": False,
            "cancel_called": False,
        }
        report_json.parent.mkdir(parents=True, exist_ok=True)
        report["report_json_path"] = str(report_json)
        report_json.write_text(json.dumps(to_jsonable(report), indent=2, sort_keys=True), encoding="utf-8")
        return CurrentQuoteResult(classification=classification, report_json=report_json, report=report, quote=quote)


class DatabentoQuoteProviderCurrentQuoteTransport:
    """Current-quote transport backed by the existing Track B Databento quote provider."""

    def __init__(
        self,
        *,
        api_key: str,
        allowlisted_local_symbol: str | None,
        tick_size: str,
        exchange: str,
        currency: str,
        max_age_seconds: int,
        lookback_seconds: int = 300,
        allow_available_end_fallback: bool = False,
        available_end_buffer_seconds: int = 300,
        base_url: str = "https://hist.databento.com/v0",
        quote_provider_factory: Any | None = None,
    ) -> None:
        self.api_key = api_key
        self.allowlisted_local_symbol = allowlisted_local_symbol
        self.tick_size = tick_size
        self.exchange = exchange
        self.currency = currency
        self.max_age_seconds = int(max_age_seconds)
        self.lookback_seconds = int(lookback_seconds)
        self.allow_available_end_fallback = bool(allow_available_end_fallback)
        self.available_end_buffer_seconds = int(available_end_buffer_seconds)
        self.base_url = base_url
        self.quote_provider_factory = quote_provider_factory

    def get_current_quote(
        self,
        *,
        dataset: str,
        symbol: str,
        stype_in: str,
        schema: str,
        contract_key: str,
    ) -> Mapping[str, Any] | None:
        raw_symbol = symbol if stype_in == "raw_symbol" else None
        continuous_symbol = symbol if stype_in != "raw_symbol" else None
        config = DatabentoQuoteProviderConfig(
            contract_key=contract_key,
            tick_size=self.tick_size,
            exchange=self.exchange,
            currency=self.currency,
            api_key=self.api_key,
            databento_symbol=raw_symbol,
            databento_continuous_symbol=continuous_symbol,
            allowlisted_local_symbol=self.allowlisted_local_symbol,
            dataset=dataset,
            stype_in=stype_in,
            bbo_schema=schema,
            lookback_seconds=self.lookback_seconds,
            allow_available_end_fallback=self.allow_available_end_fallback,
            available_end_buffer_seconds=self.available_end_buffer_seconds,
            base_url=self.base_url,
            realtime_max_age_seconds=self.max_age_seconds,
        )
        factory = self.quote_provider_factory or (lambda cfg: DatabentoQuoteProvider(config=cfg))
        quote = factory(config).get_quote(contract_key)
        return {
            "provider_symbol": quote.provider_symbol,
            "bid": str(quote.bid),
            "ask": str(quote.ask),
            "last": str(quote.last) if quote.last is not None else None,
            "timestamp": quote.timestamp.isoformat(),
            "provider_warnings": list(quote.provider_warnings),
            "raw": dict(quote.raw),
        }


def _provider_diagnostics_from_exception(exc: Exception) -> dict[str, Any]:
    diagnostics = dict(getattr(exc, "diagnostics", {}) or {})
    provider_available_end = getattr(exc, "provider_available_end", None)
    if provider_available_end is not None and "provider_available_end" not in diagnostics:
        diagnostics["provider_available_end"] = provider_available_end.isoformat()
    return diagnostics


def _provider_diagnostics_from_raw(raw_quote: Mapping[str, Any] | None) -> dict[str, Any]:
    if raw_quote is None:
        return {}
    raw = raw_quote.get("raw")
    return dict(raw) if isinstance(raw, Mapping) else {}


def _parse_timestamp(value: Any) -> datetime:
    if value is None:
        raise ValueError("timestamp is required")
    if isinstance(value, datetime):
        return require_aware_datetime(value, "timestamp")
    parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return require_aware_datetime(parsed, "timestamp")


def _required_decimal(value: Any, field_name: str) -> Decimal:
    if value is None:
        raise ValueError(f"{field_name} is required")
    try:
        parsed = Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise ValueError(f"{field_name} must be decimal-compatible") from exc
    if parsed <= 0:
        raise ValueError(f"{field_name} must be positive")
    return parsed
