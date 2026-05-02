"""Track B-local Databento quote provider.

This module is intentionally independent of the legacy market-data runtime.
It uses an injectable Databento transport and normalizes provider records into
Track B QuoteSnapshot objects.
"""

from __future__ import annotations

import base64
import json
import re
import sys
from dataclasses import dataclass
from datetime import UTC, date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Callable, Mapping, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .pricing import MarketDataMode, MarketDataRole
from .quote_provider import QuoteProviderError, QuoteSnapshot


class DatabentoQuoteProviderError(QuoteProviderError):
    """Raised when Databento quote retrieval cannot produce a snapshot."""


class DatabentoAvailableEndError(DatabentoQuoteProviderError):
    """Raised when a requested Databento records window is beyond available_end."""

    def __init__(
        self,
        message: str,
        *,
        provider_available_end: datetime | None,
        detail: str,
        diagnostics: Mapping[str, Any] | None = None,
    ) -> None:
        super().__init__(message)
        self.provider_available_end = provider_available_end
        self.detail = detail
        self.diagnostics = dict(diagnostics or {})


class DatabentoQuoteParseError(DatabentoQuoteProviderError):
    """Raised when Databento records were returned but cannot form a quote."""

    def __init__(self, message: str, *, diagnostics: Mapping[str, Any]) -> None:
        super().__init__(message)
        self.diagnostics = dict(diagnostics)


class DatabentoQuoteTransport(Protocol):
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


class DatabentoSymbolResolver(Protocol):
    def resolve(self, *, request: "DatabentoSymbolResolutionRequest") -> "DatabentoSymbolResolution": ...


class DatabentoResolutionStatus(str):
    RESOLVED = "RESOLVED"
    NOT_FOUND = "NOT_FOUND"
    PARTIAL = "PARTIAL"
    AMBIGUOUS = "AMBIGUOUS"


@dataclass(frozen=True)
class DatabentoSymbolResolutionRequest:
    requested_symbol: str
    dataset: str
    stype_in: str = "continuous"
    stype_out: str = "raw_symbol"
    resolution_date: date | None = None
    resolution_start: date | None = None
    resolution_end: date | None = None


@dataclass(frozen=True)
class DatabentoSymbolResolution:
    requested_symbol: str
    dataset: str
    stype_in: str
    stype_out: str
    resolved_instrument_id: str | None
    raw_symbol: str | None
    effective_start: datetime | None = None
    effective_end: datetime | None = None
    resolution_date: date | None = None
    resolution_start: date | None = None
    resolution_end: date | None = None
    mapping_intervals: tuple[Mapping[str, Any], ...] = ()
    active_mapping: Mapping[str, Any] | None = None
    resolution_status: str = DatabentoResolutionStatus.RESOLVED
    warnings: tuple[str, ...] = ()


@dataclass(frozen=True)
class DatabentoQuoteProviderConfig:
    contract_key: str
    tick_size: str
    exchange: str
    currency: str
    api_key: str
    databento_symbol: str | None = None
    databento_continuous_symbol: str | None = None
    allowlisted_local_symbol: str | None = None
    dataset: str = "GLBX.MDP3"
    stype_in: str = "raw_symbol"
    resolver_stype_in: str = "continuous"
    resolver_stype_out: str = "instrument_id"
    resolution_date: str | None = None
    resolution_start: str | None = None
    resolution_end: str | None = None
    allow_prior_session_resolution: bool = False
    prior_session_resolution_lookback_days: int = 3
    raw_symbol_lookup_enabled: bool = True
    raw_symbol_lookup_stype_in: str = "instrument_id"
    raw_symbol_lookup_stype_out: str = "raw_symbol"
    base_url: str = "https://hist.databento.com/v0"
    bbo_schema: str = "mbp-1"
    trades_schema: str = "trades"
    lookback_seconds: int = 300
    quote_end_timestamp: str | None = None
    allow_available_end_fallback: bool = False
    record_limit: int = 1000
    realtime_max_age_seconds: int = 15


@dataclass(frozen=True)
class DatabentoRecordDiagnosticRequest:
    api_key: str
    dataset: str
    symbol: str
    stype_in: str
    schema: str
    start: datetime
    end: datetime
    base_url: str = "https://hist.databento.com/v0"
    record_limit: int = 1000


@dataclass(frozen=True)
class DatabentoRecordDiagnosticResult:
    classification: str
    request: Mapping[str, Any]
    records_returned: int
    first_record_timestamp: str | None
    last_record_timestamp: str | None
    first_raw_record_keys_or_shape: Mapping[str, Any] | None
    provider_available_end: str | None
    provider_error: str | None
    failure_assessment: str | None

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "request": dict(self.request),
            "records_returned": self.records_returned,
            "first_record_timestamp": self.first_record_timestamp,
            "last_record_timestamp": self.last_record_timestamp,
            "first_raw_record_keys_or_shape": dict(self.first_raw_record_keys_or_shape) if self.first_raw_record_keys_or_shape else None,
            "provider_available_end": self.provider_available_end,
            "provider_error": self.provider_error,
            "failure_assessment": self.failure_assessment,
        }


class UrllibDatabentoQuoteTransport:
    def __init__(self, *, timeout_seconds: float = 10.0) -> None:
        self.timeout_seconds = float(timeout_seconds)

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
    ) -> Sequence[Mapping[str, Any]]:
        form = {
            "dataset": dataset,
            "symbols": symbol,
            "schema": schema,
            "start": start.astimezone(UTC).isoformat(),
            "end": end.astimezone(UTC).isoformat(),
            "stype_in": stype_in,
            "stype_out": "raw_symbol",
            "encoding": "json",
            "compression": "none",
            "pretty_px": "true",
            "pretty_ts": "true",
            "limit": str(int(limit)),
        }
        request = Request(
            url=f"{base_url.rstrip('/')}/timeseries.get_range",
            method="POST",
            headers={
                "Accept": "application/x-ndjson",
                "Authorization": _basic_auth_header(api_key),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data=urlencode(form).encode("utf-8"),
        )
        try:
            with urlopen(request, timeout=self.timeout_seconds) as response:
                text = response.read().decode("utf-8")
        except HTTPError as exc:  # pragma: no cover - exercised only by real operator command.
            detail = exc.read().decode("utf-8", errors="replace")
            if exc.code == 422:
                available_end = _extract_available_end(detail)
                if available_end is not None or "available_end" in detail or "available end" in detail:
                    message = (
                        "requested quote window is after Databento available_end; rerun with "
                        "--allow-available-end-fallback or earlier --quote-end-timestamp"
                    )
                    raise DatabentoAvailableEndError(message, provider_available_end=available_end, detail=detail) from exc
            raise DatabentoQuoteProviderError(f"Databento HTTP error {exc.code}: {detail}") from exc
        except URLError as exc:  # pragma: no cover - exercised only by real operator command.
            raise DatabentoQuoteProviderError(f"Databento transport error: {exc}") from exc
        return tuple(json.loads(line) for line in text.splitlines() if line.strip())


class NativeDatabentoQuoteTransport:
    """Read-only records transport backed by Databento's native Python client."""

    def __init__(self, *, client_factory: Callable[[str], Any] | None = None) -> None:
        self.client_factory = client_factory

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
    ) -> Sequence[Mapping[str, Any]]:
        del base_url, limit
        client = self.client_factory(api_key) if self.client_factory else _native_historical_client(api_key)
        try:
            store = client.timeseries.get_range(
                dataset=dataset,
                symbols=[symbol],
                schema=schema,
                start=start.astimezone(UTC).isoformat(),
                end=end.astimezone(UTC).isoformat(),
                stype_in=stype_in,
            )
        except Exception as exc:  # noqa: BLE001 - provider errors must be reported cleanly.
            detail = _sanitize_provider_error(exc)
            available_end = _extract_available_end(detail)
            if available_end is not None or "data_start_after_available_end" in detail or "available_end" in detail or "available end" in detail:
                raise DatabentoAvailableEndError(
                    "requested quote window is after Databento available_end; rerun with --allow-available-end-fallback or earlier --quote-end-timestamp",
                    provider_available_end=available_end,
                    detail=detail,
                    diagnostics={
                        "native_exception_class": type(exc).__name__,
                        "native_exception_message_sanitized": detail,
                    },
                ) from exc
            raise DatabentoQuoteProviderError(f"Databento native get_range error: {detail}") from exc
        try:
            frame = store.to_df()
        except Exception as exc:  # noqa: BLE001 - DBNStore parse errors should remain provider-scoped.
            raise DatabentoQuoteProviderError(f"Databento native DBNStore to_df error: {_sanitize_provider_error(exc)}") from exc
        return _records_from_dataframe(frame)


def run_databento_record_diagnostic(
    *,
    request: DatabentoRecordDiagnosticRequest,
    transport: DatabentoQuoteTransport | None = None,
) -> DatabentoRecordDiagnosticResult:
    transport = transport or NativeDatabentoQuoteTransport()
    request_details = _record_request_details(
        base_url=request.base_url,
        dataset=request.dataset,
        symbol=request.symbol,
        schema=request.schema,
        start=request.start,
        end=request.end,
        stype_in=request.stype_in,
        limit=request.record_limit,
    )
    try:
        records = tuple(
            transport.request_records(
                base_url=request.base_url,
                api_key=request.api_key,
                dataset=request.dataset,
                symbol=request.symbol,
                schema=request.schema,
                start=request.start,
                end=request.end,
                stype_in=request.stype_in,
                limit=request.record_limit,
            )
        )
    except DatabentoAvailableEndError as exc:
        return DatabentoRecordDiagnosticResult(
            classification="PROVIDER_ERROR",
            request=request_details,
            records_returned=0,
            first_record_timestamp=None,
            last_record_timestamp=None,
            first_raw_record_keys_or_shape=None,
            provider_available_end=exc.provider_available_end.isoformat() if exc.provider_available_end is not None else None,
            provider_error=_sanitize_provider_error(exc.detail or str(exc)),
            failure_assessment="BAD_WINDOW_AFTER_AVAILABLE_END",
        )
    except DatabentoQuoteProviderError as exc:
        return DatabentoRecordDiagnosticResult(
            classification="PROVIDER_ERROR",
            request=request_details,
            records_returned=0,
            first_record_timestamp=None,
            last_record_timestamp=None,
            first_raw_record_keys_or_shape=None,
            provider_available_end=getattr(exc, "provider_available_end", None).isoformat()
            if getattr(exc, "provider_available_end", None) is not None
            else None,
            provider_error=_sanitize_provider_error(str(exc)),
            failure_assessment="PROVIDER_ERROR_OR_ENTITLEMENT",
        )
    if not records:
        return DatabentoRecordDiagnosticResult(
            classification="ZERO_RECORDS",
            request=request_details,
            records_returned=0,
            first_record_timestamp=None,
            last_record_timestamp=None,
            first_raw_record_keys_or_shape=None,
            provider_available_end=None,
            provider_error=None,
            failure_assessment="NO_DATA_BAD_SYMBOL_STYPE_BAD_SCHEMA_BAD_WINDOW_OR_ENTITLEMENT",
        )
    timestamps = tuple(_record_timestamp(record) for record in records)
    return DatabentoRecordDiagnosticResult(
        classification="RECORDS_FOUND",
        request=request_details,
        records_returned=len(records),
        first_record_timestamp=min(timestamps).isoformat(),
        last_record_timestamp=max(timestamps).isoformat(),
        first_raw_record_keys_or_shape=_record_shape(records[0]),
        provider_available_end=None,
        provider_error=None,
        failure_assessment=None,
    )


class UrllibDatabentoSymbolResolver:
    def __init__(self, *, api_key: str, base_url: str = "https://hist.databento.com/v0", timeout_seconds: float = 10.0) -> None:
        self.api_key = api_key
        self.base_url = base_url
        self.timeout_seconds = float(timeout_seconds)

    def resolve(self, *, request: DatabentoSymbolResolutionRequest) -> DatabentoSymbolResolution:
        resolution_date = request.resolution_date or datetime.now(UTC).date()
        resolution_start = request.resolution_start or resolution_date
        resolution_end = request.resolution_end or (resolution_date + timedelta(days=1))
        form = {
            "dataset": request.dataset,
            "symbols": request.requested_symbol,
            "stype_in": request.stype_in,
            "stype_out": request.stype_out,
            "start_date": resolution_start.isoformat(),
            "end_date": resolution_end.isoformat(),
        }
        http_request = Request(
            url=f"{self.base_url.rstrip('/')}/symbology.resolve",
            method="POST",
            headers={
                "Accept": "application/json",
                "Authorization": _basic_auth_header(self.api_key),
                "Content-Type": "application/x-www-form-urlencoded",
            },
            data=urlencode(form).encode("utf-8"),
        )
        try:
            with urlopen(http_request, timeout=self.timeout_seconds) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except HTTPError as exc:  # pragma: no cover - exercised only by real operator command.
            detail = exc.read().decode("utf-8", errors="replace")
            if exc.code == 422 and request.stype_in == "continuous" and request.stype_out == "raw_symbol":
                detail = (
                    f"{detail} Track B corrective action: resolve continuous symbols to instrument_id first, "
                    "then optionally resolve instrument_id to raw_symbol for reporting."
                )
            raise DatabentoQuoteProviderError(f"Databento symbology HTTP error {exc.code}: {detail}") from exc
        except URLError as exc:  # pragma: no cover - exercised only by real operator command.
            raise DatabentoQuoteProviderError(f"Databento symbology transport error: {exc}") from exc
        return _resolution_from_payload(request=request, payload=payload)


class DatabentoQuoteProvider:
    provider_name = "DATABENTO"

    def __init__(
        self,
        *,
        config: DatabentoQuoteProviderConfig,
        transport: DatabentoQuoteTransport | None = None,
        resolver: DatabentoSymbolResolver | None = None,
        now: datetime | None = None,
    ) -> None:
        self.config = config
        self.transport = transport or NativeDatabentoQuoteTransport()
        self.resolver = resolver or UrllibDatabentoSymbolResolver(api_key=config.api_key, base_url=config.base_url)
        self._now = now
        _require_config(config)

    def get_quote(self, contract_key: str) -> QuoteSnapshot:
        if str(contract_key or "").strip() != self.config.contract_key:
            raise DatabentoQuoteProviderError("contract_key must match explicit Databento quote provider mapping")
        now = self._now or datetime.now(UTC)
        quote_end = _optional_datetime(self.config.quote_end_timestamp) or now
        quote_lookback = max(int(self.config.lookback_seconds), 1)
        requested_start = quote_end - timedelta(seconds=quote_lookback)
        requested_end = quote_end
        actual_start = requested_start
        actual_end = requested_end
        provider_available_end: datetime | None = None
        available_end_fallback_used = False
        available_end_retry_attempted = False
        available_end_retry_reason: str | None = None
        native_databento_path_used = isinstance(self.transport, NativeDatabentoQuoteTransport)
        resolved_symbol = self._resolve_quote_symbol()
        try:
            bbo_records, trade_records = self._request_quote_records(resolved_symbol=resolved_symbol, start=actual_start, end=actual_end)
        except DatabentoAvailableEndError as exc:
            provider_available_end = exc.provider_available_end
            if not self.config.allow_available_end_fallback:
                exc.diagnostics = _quote_request_failure_diagnostics(
                    config=self.config,
                    resolved_symbol=resolved_symbol,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    actual_start=actual_start,
                    actual_end=actual_end,
                    provider_available_end=provider_available_end,
                    available_end_fallback_used=False,
                    available_end_retry_attempted=False,
                    available_end_retry_reason="fallback_not_enabled",
                    native_databento_path_used=native_databento_path_used,
                    exception_diagnostics=getattr(exc, "diagnostics", {}),
                    raw_provider_error=exc.detail,
                )
                raise
            if provider_available_end is None:
                error = DatabentoQuoteProviderError(
                    "Databento available_end fallback was requested, but provider_available_end was not parseable"
                )
                error.diagnostics = _quote_request_failure_diagnostics(
                    config=self.config,
                    resolved_symbol=resolved_symbol,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    actual_start=actual_start,
                    actual_end=actual_end,
                    provider_available_end=None,
                    available_end_fallback_used=False,
                    available_end_retry_attempted=False,
                    available_end_retry_reason="provider_available_end_unparseable",
                    native_databento_path_used=native_databento_path_used,
                    exception_diagnostics=getattr(exc, "diagnostics", {}),
                    raw_provider_error=exc.detail,
                )
                raise error from exc
            actual_end = provider_available_end.astimezone(UTC)
            actual_start = actual_end - timedelta(seconds=quote_lookback)
            available_end_fallback_used = True
            available_end_retry_attempted = True
            available_end_retry_reason = "initial_data_start_after_available_end"
            try:
                bbo_records, trade_records = self._request_quote_records(resolved_symbol=resolved_symbol, start=actual_start, end=actual_end)
            except DatabentoAvailableEndError as retry_exc:
                retry_provider_available_end = retry_exc.provider_available_end or provider_available_end
                retry_exc.diagnostics = _quote_request_failure_diagnostics(
                    config=self.config,
                    resolved_symbol=resolved_symbol,
                    requested_start=requested_start,
                    requested_end=requested_end,
                    actual_start=actual_start,
                    actual_end=actual_end,
                    provider_available_end=retry_provider_available_end,
                    available_end_fallback_used=True,
                    available_end_retry_attempted=True,
                    available_end_retry_reason=available_end_retry_reason,
                    native_databento_path_used=native_databento_path_used,
                    exception_diagnostics=getattr(retry_exc, "diagnostics", {}),
                    raw_provider_error=retry_exc.detail,
                )
                raise
        latest_bbo = _latest_record_with_bid_ask(bbo_records)
        latest_trade = _latest_record_with_price(trade_records)
        parser_diagnostics = _quote_parser_diagnostics(
            config=self.config,
            resolved_symbol=resolved_symbol,
            start=actual_start,
            end=actual_end,
            requested_start=requested_start,
            requested_end=requested_end,
            provider_available_end=provider_available_end,
            available_end_fallback_used=available_end_fallback_used,
            available_end_retry_attempted=available_end_retry_attempted,
            available_end_retry_reason=available_end_retry_reason,
            native_databento_path_used=native_databento_path_used,
            bbo_records=bbo_records,
            trade_records=trade_records,
            latest_bbo=latest_bbo,
            latest_trade=latest_trade,
        )
        if latest_bbo is None:
            raise DatabentoQuoteParseError(
                f"Databento returned no bid/ask quote records for schema {self.config.bbo_schema}",
                diagnostics=parser_diagnostics,
            )
        bid_parse = _first_decimal_with_source(latest_bbo, _BID_KEYS)
        ask_parse = _first_decimal_with_source(latest_bbo, _ASK_KEYS)
        if bid_parse.value is None or ask_parse.value is None:
            raise DatabentoQuoteParseError(
                f"Databento {self.config.bbo_schema} records did not contain parseable bid/ask fields",
                diagnostics=parser_diagnostics,
            )
        bid = bid_parse.value
        ask = ask_parse.value
        last_parse = _first_decimal_with_source(latest_trade or {}, _LAST_KEYS) if latest_trade is not None else _ParsedDecimal(None, None)
        last = last_parse.value
        timestamp = max(_record_timestamp(latest_bbo), _record_timestamp(latest_trade) if latest_trade is not None else _record_timestamp(latest_bbo))
        warnings: list[str] = []
        if latest_trade is None:
            warnings.append("Databento returned no trade record for last price in lookback window.")
        if available_end_fallback_used:
            warnings.append("Databento available_end quote-window fallback was used; live-money readiness must remain false.")
        warnings.extend(resolved_symbol.warnings)
        mode = (
            MarketDataMode.REALTIME
            if not available_end_fallback_used and (now - timestamp).total_seconds() <= int(self.config.realtime_max_age_seconds)
            else MarketDataMode.UNKNOWN
        )
        return QuoteSnapshot(
            provider=self.provider_name,
            mode=mode,
            role=MarketDataRole.PRIMARY,
            contract_key=self.config.contract_key,
            provider_symbol=resolved_symbol.report_symbol,
            bid=bid,
            ask=ask,
            last=last,
            timestamp=timestamp,
            tick_size=self.config.tick_size,
            exchange=self.config.exchange,
            currency=self.config.currency,
            provider_warnings=tuple(warnings),
            delayed_data_warning_seen=False,
            raw={
                "dataset": self.config.dataset,
                "stype_in": resolved_symbol.stype_in,
                "bbo_schema": self.config.bbo_schema,
                "trades_schema": self.config.trades_schema,
                "bbo_record_count": len(bbo_records),
                "trade_record_count": len(trade_records),
                **parser_diagnostics,
                "requested_quote_start": requested_start.isoformat(),
                "requested_quote_end": requested_end.isoformat(),
                "actual_quote_start": actual_start.isoformat(),
                "actual_quote_end": actual_end.isoformat(),
                "provider_available_end": provider_available_end.isoformat() if provider_available_end is not None else None,
                "available_end_fallback_used": available_end_fallback_used,
                "quote_age_seconds": str((actual_end - timestamp).total_seconds()),
                "usable_for_paper_pricing": mode == MarketDataMode.REALTIME,
                "usable_for_live_money_readiness": mode == MarketDataMode.REALTIME,
                "symbol_source": resolved_symbol.symbol_source,
                "requested_continuous_symbol": self.config.databento_continuous_symbol,
                "manual_provider_symbol_override": self.config.databento_symbol,
                "resolution_path": resolved_symbol.resolution_path,
                "requested_resolution_date": resolved_symbol.requested_resolution_date.isoformat()
                if resolved_symbol.requested_resolution_date is not None
                else None,
                "actual_resolution_date_used": resolved_symbol.actual_resolution_date_used.isoformat()
                if resolved_symbol.actual_resolution_date_used is not None
                else None,
                "prior_session_fallback_used": resolved_symbol.prior_session_fallback_used,
                "fallback_lookback_days": resolved_symbol.fallback_lookback_days,
                "resolution_session_type": resolved_symbol.resolution_session_type,
                "resolution_date": resolved_symbol.resolution_date.isoformat() if resolved_symbol.resolution_date is not None else None,
                "resolution_start": resolved_symbol.resolution_start.isoformat() if resolved_symbol.resolution_start is not None else None,
                "resolution_end": resolved_symbol.resolution_end.isoformat() if resolved_symbol.resolution_end is not None else None,
                "mapping_intervals": tuple(dict(row) for row in resolved_symbol.mapping_intervals),
                "active_mapping": dict(resolved_symbol.active_mapping) if resolved_symbol.active_mapping is not None else None,
                "raw_symbol_lookup_path": resolved_symbol.raw_symbol_lookup_path,
                "raw_symbol_match_status": resolved_symbol.raw_symbol_match_status,
                "quote_request_symbol": resolved_symbol.symbol,
                "quote_request_stype_in": resolved_symbol.stype_in,
                "resolved_instrument_id": resolved_symbol.resolved_instrument_id,
                "resolved_raw_symbol": resolved_symbol.resolved_raw_symbol,
                "resolution_status": resolved_symbol.resolution_status,
                "raw_symbol_resolution_status": resolved_symbol.raw_symbol_resolution_status,
                "execution_contract_validation_status": resolved_symbol.execution_validation_status,
            },
        )

    def _request_quote_records(
        self,
        *,
        resolved_symbol: "_ResolvedQuoteSymbol",
        start: datetime,
        end: datetime,
    ) -> tuple[tuple[Mapping[str, Any], ...], tuple[Mapping[str, Any], ...]]:
        bbo_records = tuple(
            self.transport.request_records(
                base_url=self.config.base_url,
                api_key=self.config.api_key,
                dataset=self.config.dataset,
                symbol=resolved_symbol.symbol,
                schema=self.config.bbo_schema,
                start=start,
                end=end,
                stype_in=resolved_symbol.stype_in,
                limit=self.config.record_limit,
            )
        )
        trade_records = tuple(
            self.transport.request_records(
                base_url=self.config.base_url,
                api_key=self.config.api_key,
                dataset=self.config.dataset,
                symbol=resolved_symbol.symbol,
                schema=self.config.trades_schema,
                start=start,
                end=end,
                stype_in=resolved_symbol.stype_in,
                limit=self.config.record_limit,
            )
        )
        return bbo_records, trade_records

    def _resolve_quote_symbol(self) -> "_ResolvedQuoteSymbol":
        if self.config.databento_symbol:
            return _ResolvedQuoteSymbol(
                symbol=str(self.config.databento_symbol),
                stype_in=self.config.stype_in,
                report_symbol=str(self.config.databento_symbol),
                symbol_source="MANUAL_PROVIDER_SYMBOL_OVERRIDE",
                execution_validation_status="MANUAL_OVERRIDE_OPERATOR_REVIEW",
                raw_symbol_match_status="MANUAL_OVERRIDE_OPERATOR_REVIEW",
                resolution_path=None,
                requested_resolution_date=None,
                actual_resolution_date_used=None,
                prior_session_fallback_used=False,
                fallback_lookback_days=None,
                resolution_session_type=None,
                resolution_date=None,
                resolution_start=None,
                resolution_end=None,
                mapping_intervals=(),
                active_mapping=None,
                raw_symbol_lookup_path=None,
                resolved_instrument_id=None,
                resolved_raw_symbol=None,
                resolution_status=None,
                raw_symbol_resolution_status=None,
                warnings=("Manual Databento provider symbol override was used for market data only.",),
            )
        requested = str(self.config.databento_continuous_symbol or "").strip()
        resolution_date = _optional_date(self.config.resolution_date) or (self._now or datetime.now(UTC)).astimezone(UTC).date()
        resolution_start = _optional_date(self.config.resolution_start)
        resolution_end = _optional_date(self.config.resolution_end)
        primary_resolution, prior_session_fallback_used = self._resolve_primary_symbol(
            requested_symbol=requested,
            requested_resolution_date=resolution_date,
            resolution_start=resolution_start,
            resolution_end=resolution_end,
        )
        if primary_resolution.resolution_status != DatabentoResolutionStatus.RESOLVED:
            raise DatabentoQuoteProviderError(f"Databento symbol resolution did not resolve cleanly: {primary_resolution.resolution_status}")
        if not primary_resolution.resolved_instrument_id:
            raise DatabentoQuoteProviderError("Databento continuous symbol resolution returned no instrument_id")

        warnings = list(primary_resolution.warnings)
        if prior_session_fallback_used:
            warnings.append(
                "Databento prior-session resolution fallback was used; this must not be treated as silent current-session resolution."
            )
        resolved_raw_symbol: str | None = primary_resolution.raw_symbol
        raw_symbol_resolution_status: str | None = None
        raw_symbol_lookup_path: str | None = None
        if not resolved_raw_symbol and self.config.raw_symbol_lookup_enabled:
            raw_symbol_lookup_path = f"{self.config.raw_symbol_lookup_stype_in}->{self.config.raw_symbol_lookup_stype_out}"
            try:
                raw_resolution = self.resolver.resolve(
                    request=DatabentoSymbolResolutionRequest(
                        requested_symbol=primary_resolution.resolved_instrument_id,
                        dataset=self.config.dataset,
                        stype_in=self.config.raw_symbol_lookup_stype_in,
                        stype_out=self.config.raw_symbol_lookup_stype_out,
                        resolution_date=primary_resolution.resolution_date or resolution_date,
                        resolution_start=resolution_start,
                        resolution_end=resolution_end,
                    )
                )
            except DatabentoQuoteProviderError as exc:
                raw_symbol_resolution_status = "UNAVAILABLE"
                warnings.append(f"Optional Databento raw-symbol lookup failed: {exc}")
            else:
                raw_symbol_resolution_status = raw_resolution.resolution_status
                warnings.extend(raw_resolution.warnings)
                if raw_resolution.resolution_status == DatabentoResolutionStatus.RESOLVED and raw_resolution.raw_symbol:
                    resolved_raw_symbol = raw_resolution.raw_symbol
                else:
                    warnings.append("Optional Databento raw-symbol lookup did not return a raw symbol.")

        raw_symbol_match_status = "UNAVAILABLE"
        execution_validation_status = "INCOMPLETE_NO_RAW_SYMBOL"
        if resolved_raw_symbol and self.config.allowlisted_local_symbol and resolved_raw_symbol != self.config.allowlisted_local_symbol:
            raw_symbol_match_status = "MISMATCH"
            raise DatabentoQuoteProviderError(
                "Databento resolved raw symbol conflicts with IBKR allowlisted local symbol; operator review required"
            )
        if resolved_raw_symbol and self.config.allowlisted_local_symbol:
            raw_symbol_match_status = "MATCH"
            execution_validation_status = "MATCHED_ALLOWLISTED_LOCAL_SYMBOL"
        elif resolved_raw_symbol:
            raw_symbol_match_status = "UNAVAILABLE"
            execution_validation_status = "RAW_SYMBOL_AVAILABLE_NO_ALLOWLIST_COMPARISON"
        else:
            warnings.append("Databento raw symbol is unavailable; execution contract exact-match validation is incomplete.")
        return _ResolvedQuoteSymbol(
            symbol=primary_resolution.resolved_instrument_id,
            stype_in="instrument_id",
            report_symbol=resolved_raw_symbol or primary_resolution.resolved_instrument_id,
            symbol_source="CONTINUOUS_SYMBOL_RESOLUTION",
            execution_validation_status=execution_validation_status,
            raw_symbol_match_status=raw_symbol_match_status,
            resolution_path=f"{self.config.resolver_stype_in}->{self.config.resolver_stype_out}",
            requested_resolution_date=resolution_date,
            actual_resolution_date_used=primary_resolution.resolution_date,
            prior_session_fallback_used=prior_session_fallback_used,
            fallback_lookback_days=int(self.config.prior_session_resolution_lookback_days),
            resolution_session_type="PRIOR_SESSION_RESOLUTION_FALLBACK" if prior_session_fallback_used else "CURRENT_SESSION",
            resolution_date=primary_resolution.resolution_date,
            resolution_start=primary_resolution.resolution_start,
            resolution_end=primary_resolution.resolution_end,
            mapping_intervals=primary_resolution.mapping_intervals,
            active_mapping=primary_resolution.active_mapping,
            raw_symbol_lookup_path=raw_symbol_lookup_path,
            resolved_instrument_id=primary_resolution.resolved_instrument_id,
            resolved_raw_symbol=resolved_raw_symbol,
            resolution_status=primary_resolution.resolution_status,
            raw_symbol_resolution_status=raw_symbol_resolution_status,
            warnings=tuple(warnings),
        )

    def _resolve_primary_symbol(
        self,
        *,
        requested_symbol: str,
        requested_resolution_date: date,
        resolution_start: date | None,
        resolution_end: date | None,
    ) -> tuple[DatabentoSymbolResolution, bool]:
        first = self._resolve_primary_for_date(
            requested_symbol=requested_symbol,
            resolution_date=requested_resolution_date,
            resolution_start=resolution_start,
            resolution_end=resolution_end,
        )
        if first.resolution_status == DatabentoResolutionStatus.RESOLVED or not self.config.allow_prior_session_resolution:
            return first, False
        lookback_days = int(self.config.prior_session_resolution_lookback_days)
        if lookback_days < 1:
            return first, False
        for offset in range(1, lookback_days + 1):
            fallback_date = requested_resolution_date - timedelta(days=offset)
            candidate = self._resolve_primary_for_date(
                requested_symbol=requested_symbol,
                resolution_date=fallback_date,
                resolution_start=fallback_date,
                resolution_end=fallback_date + timedelta(days=1),
            )
            if candidate.resolution_status == DatabentoResolutionStatus.RESOLVED:
                return candidate, True
        return first, False

    def _resolve_primary_for_date(
        self,
        *,
        requested_symbol: str,
        resolution_date: date,
        resolution_start: date | None,
        resolution_end: date | None,
    ) -> DatabentoSymbolResolution:
        return self.resolver.resolve(
            request=DatabentoSymbolResolutionRequest(
                requested_symbol=requested_symbol,
                dataset=self.config.dataset,
                stype_in=self.config.resolver_stype_in,
                stype_out=self.config.resolver_stype_out,
                resolution_date=resolution_date,
                resolution_start=resolution_start,
                resolution_end=resolution_end,
            )
        )


@dataclass(frozen=True)
class _ResolvedQuoteSymbol:
    symbol: str
    stype_in: str
    report_symbol: str
    symbol_source: str
    execution_validation_status: str
    raw_symbol_match_status: str
    resolution_path: str | None
    requested_resolution_date: date | None
    actual_resolution_date_used: date | None
    prior_session_fallback_used: bool
    fallback_lookback_days: int | None
    resolution_session_type: str | None
    resolution_date: date | None
    resolution_start: date | None
    resolution_end: date | None
    mapping_intervals: tuple[Mapping[str, Any], ...]
    active_mapping: Mapping[str, Any] | None
    raw_symbol_lookup_path: str | None
    resolved_instrument_id: str | None
    resolved_raw_symbol: str | None
    resolution_status: str | None
    raw_symbol_resolution_status: str | None
    warnings: tuple[str, ...]


@dataclass(frozen=True)
class _ParsedDecimal:
    value: Decimal | None
    source: str | None


_BID_KEYS = ("bid_px", "bid_price", "bid", "bid_px_00", "bid_price_00", "bid_0")
_ASK_KEYS = ("ask_px", "ask_price", "ask", "ask_px_00", "ask_price_00", "ask_0")
_LAST_KEYS = ("price", "last", "last_px")


def _require_config(config: DatabentoQuoteProviderConfig) -> None:
    required = {
        "contract_key": config.contract_key,
        "tick_size": config.tick_size,
        "exchange": config.exchange,
        "currency": config.currency,
        "api_key": config.api_key,
    }
    missing = [key for key, value in required.items() if not str(value or "").strip()]
    if missing:
        raise DatabentoQuoteProviderError(f"Databento quote provider missing required field(s): {', '.join(missing)}")
    if bool(str(config.databento_symbol or "").strip()) == bool(str(config.databento_continuous_symbol or "").strip()):
        raise DatabentoQuoteProviderError("configure exactly one of databento_symbol or databento_continuous_symbol")


def _latest_record_with_bid_ask(records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    candidates = [record for record in records if _first_decimal(record, _BID_KEYS) is not None and _first_decimal(record, _ASK_KEYS) is not None]
    return max(candidates, key=_record_timestamp) if candidates else None


def _latest_record_with_price(records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    candidates = [record for record in records if _first_decimal(record, _LAST_KEYS) is not None]
    return max(candidates, key=_record_timestamp) if candidates else None


def _first_decimal(record: Mapping[str, Any], keys: tuple[str, ...]) -> Decimal | None:
    return _first_decimal_with_source(record, keys).value


def _first_decimal_with_source(record: Mapping[str, Any], keys: tuple[str, ...]) -> _ParsedDecimal:
    for key in keys:
        value, source = _nested_value_with_source(record, key)
        if value is None or value == "":
            continue
        try:
            return _ParsedDecimal(Decimal(str(value)), source)
        except (InvalidOperation, ValueError):
            continue
    return _ParsedDecimal(None, None)


def _nested_value(record: Mapping[str, Any], key: str) -> Any:
    return _nested_value_with_source(record, key)[0]


def _nested_value_with_source(record: Mapping[str, Any], key: str) -> tuple[Any, str | None]:
    if key in record:
        return record[key], key
    levels = record.get("levels")
    if isinstance(levels, Sequence) and not isinstance(levels, (str, bytes)) and levels:
        first_level = levels[0]
        if isinstance(first_level, Mapping) and key in first_level:
            return first_level[key], f"levels[0].{key}"
    return None, None


def _quote_parser_diagnostics(
    *,
    config: DatabentoQuoteProviderConfig,
    resolved_symbol: _ResolvedQuoteSymbol,
    start: datetime,
    end: datetime,
    requested_start: datetime | None = None,
    requested_end: datetime | None = None,
    provider_available_end: datetime | None,
    available_end_fallback_used: bool | None = None,
    available_end_retry_attempted: bool = False,
    available_end_retry_reason: str | None = None,
    native_databento_path_used: bool = False,
    bbo_records: Sequence[Mapping[str, Any]],
    trade_records: Sequence[Mapping[str, Any]],
    latest_bbo: Mapping[str, Any] | None,
    latest_trade: Mapping[str, Any] | None,
) -> dict[str, Any]:
    requested_start = requested_start or start
    requested_end = requested_end or end
    fallback_used = bool(provider_available_end is not None) if available_end_fallback_used is None else bool(available_end_fallback_used)
    bid_parse = _first_decimal_with_source(latest_bbo or {}, _BID_KEYS)
    ask_parse = _first_decimal_with_source(latest_bbo or {}, _ASK_KEYS)
    last_parse = _first_decimal_with_source(latest_trade or {}, _LAST_KEYS)
    no_quote_records_reason: str | None = None
    if not bbo_records:
        no_quote_records_reason = f"no records returned for schema {config.bbo_schema}"
    elif latest_bbo is None:
        no_quote_records_reason = f"records returned for schema {config.bbo_schema}, but none contained parseable bid and ask fields"
    bbo_request = _record_request_details(
        base_url=config.base_url,
        dataset=config.dataset,
        symbol=resolved_symbol.symbol,
        schema=config.bbo_schema,
        start=start,
        end=end,
        stype_in=resolved_symbol.stype_in,
        limit=config.record_limit,
    )
    trade_request = _record_request_details(
        base_url=config.base_url,
        dataset=config.dataset,
        symbol=resolved_symbol.symbol,
        schema=config.trades_schema,
        start=start,
        end=end,
        stype_in=resolved_symbol.stype_in,
        limit=config.record_limit,
    )
    return {
        "resolved_instrument_id": resolved_symbol.resolved_instrument_id,
        "resolved_symbol_stype": resolved_symbol.stype_in,
        "quote_request_symbol": resolved_symbol.symbol,
        "quote_request_stype_in": resolved_symbol.stype_in,
        "dataset": config.dataset,
        "schema": {"bid_ask": config.bbo_schema, "last": config.trades_schema},
        "schemas_attempted": [config.bbo_schema, config.trades_schema],
        "start": start.isoformat(),
        "end": end.isoformat(),
        "requested_quote_start": requested_start.isoformat(),
        "requested_quote_end": requested_end.isoformat(),
        "actual_quote_start": start.isoformat(),
        "actual_quote_end": end.isoformat(),
        "encoding": "json",
        "provider_available_end": provider_available_end.isoformat() if provider_available_end is not None else None,
        "allow_available_end_fallback_requested": bool(config.allow_available_end_fallback),
        "allow_available_end_fallback_effective": bool(config.allow_available_end_fallback and provider_available_end is not None),
        "native_databento_path_used": bool(native_databento_path_used),
        "available_end_fallback_used": fallback_used,
        "available_end_retry_attempted": bool(available_end_retry_attempted),
        "available_end_retry_reason": available_end_retry_reason,
        "raw_provider_error": None,
        "native_databento_error_code": None,
        "native_databento_error_message": None,
        "native_exception_class": None,
        "native_exception_message_sanitized": None,
        "request_details": {"bid_ask": bbo_request, "trades": trade_request},
        "databento_schema": {"bid_ask": config.bbo_schema, "last": config.trades_schema},
        "records_returned": {"bid_ask": len(bbo_records), "trades": len(trade_records)},
        "first_raw_record_keys_or_shape": {
            "bid_ask": _record_shape(bbo_records[0]) if bbo_records else None,
            "trades": _record_shape(trade_records[0]) if trade_records else None,
        },
        "parser_bid_field_source": bid_parse.source,
        "parser_ask_field_source": ask_parse.source,
        "parser_last_field_source": last_parse.source,
        "no_quote_records_reason": no_quote_records_reason,
    }


def _quote_request_failure_diagnostics(
    *,
    config: DatabentoQuoteProviderConfig,
    resolved_symbol: _ResolvedQuoteSymbol,
    requested_start: datetime,
    requested_end: datetime,
    actual_start: datetime,
    actual_end: datetime,
    provider_available_end: datetime | None,
    available_end_fallback_used: bool,
    available_end_retry_attempted: bool,
    available_end_retry_reason: str | None,
    native_databento_path_used: bool,
    exception_diagnostics: Mapping[str, Any] | None,
    raw_provider_error: str | None,
) -> dict[str, Any]:
    base = _quote_parser_diagnostics(
        config=config,
        resolved_symbol=resolved_symbol,
        start=actual_start,
        end=actual_end,
        requested_start=requested_start,
        requested_end=requested_end,
        provider_available_end=provider_available_end,
        available_end_fallback_used=available_end_fallback_used,
        available_end_retry_attempted=available_end_retry_attempted,
        available_end_retry_reason=available_end_retry_reason,
        native_databento_path_used=native_databento_path_used,
        bbo_records=(),
        trade_records=(),
        latest_bbo=None,
        latest_trade=None,
    )
    exception_diagnostics = dict(exception_diagnostics or {})
    code, message = _native_error_code_message(raw_provider_error)
    base.update(
        {
            "requested_quote_start": requested_start.isoformat(),
            "requested_quote_end": requested_end.isoformat(),
            "actual_quote_start": actual_start.isoformat(),
            "actual_quote_end": actual_end.isoformat(),
            "available_end_fallback_used": available_end_fallback_used,
            "available_end_retry_attempted": available_end_retry_attempted,
            "available_end_retry_reason": available_end_retry_reason,
            "raw_provider_error": raw_provider_error,
            "native_databento_error_code": code,
            "native_databento_error_message": message,
            "native_exception_class": exception_diagnostics.get("native_exception_class"),
            "native_exception_message_sanitized": exception_diagnostics.get("native_exception_message_sanitized"),
        }
    )
    return base


def _record_request_details(
    *,
    base_url: str,
    dataset: str,
    symbol: str,
    schema: str,
    start: datetime,
    end: datetime,
    stype_in: str,
    limit: int,
) -> dict[str, Any]:
    return {
        "endpoint": f"{base_url.rstrip('/')}/timeseries.get_range",
        "dataset": dataset,
        "schema": schema,
        "symbol": symbol,
        "stype_in": stype_in,
        "start": start.astimezone(UTC).isoformat(),
        "end": end.astimezone(UTC).isoformat(),
        "encoding": "json",
        "compression": "none",
        "limit": int(limit),
    }


def _record_shape(record: Mapping[str, Any]) -> dict[str, Any]:
    shape: dict[str, Any] = {"root_keys": sorted(str(key) for key in record.keys())}
    header = record.get("hd")
    if isinstance(header, Mapping):
        shape["hd_keys"] = sorted(str(key) for key in header.keys())
    levels = record.get("levels")
    if isinstance(levels, Sequence) and not isinstance(levels, (str, bytes)):
        shape["levels_count"] = len(levels)
        if levels and isinstance(levels[0], Mapping):
            shape["levels[0]_keys"] = sorted(str(key) for key in levels[0].keys())
    return shape


def _native_historical_client(api_key: str) -> Any:
    original_sys_path = list(sys.path)
    try:
        sys.path = [path for path in sys.path if not str(path).endswith("/src")]
        import databento as db  # type: ignore[import-not-found]
    except Exception as exc:  # noqa: BLE001 - optional dependency should fail only for real provider path.
        raise DatabentoQuoteProviderError(
            "Databento native client is unavailable; install the optional dependency with ./.venv/bin/python -m pip install -e \".[databento]\""
        ) from exc
    finally:
        sys.path = original_sys_path
    try:
        return db.Historical(api_key)
    except TypeError:
        return db.Historical(key=api_key)


def _records_from_dataframe(frame: Any) -> tuple[Mapping[str, Any], ...]:
    if frame is None:
        return ()
    empty = getattr(frame, "empty", None)
    if empty is True:
        return ()
    if hasattr(frame, "reset_index"):
        try:
            frame = frame.reset_index()
        except Exception:
            pass
    if hasattr(frame, "to_dict"):
        try:
            records = frame.to_dict(orient="records")
        except TypeError:
            records = frame.to_dict("records")
        if isinstance(records, Sequence) and not isinstance(records, (str, bytes)):
            return tuple(_normalize_dataframe_record(record) for record in records if isinstance(record, Mapping))
    if isinstance(frame, Sequence) and not isinstance(frame, (str, bytes)):
        return tuple(_normalize_dataframe_record(record) for record in frame if isinstance(record, Mapping))
    raise DatabentoQuoteProviderError("Databento native to_df returned an unsupported dataframe shape")


def _normalize_dataframe_record(record: Mapping[str, Any]) -> Mapping[str, Any]:
    normalized: dict[str, Any] = {}
    for key, value in record.items():
        key_text = _column_name(key)
        normalized[key_text] = _json_safe_value(value)
    return normalized


def _column_name(key: Any) -> str:
    if isinstance(key, tuple):
        return ".".join(str(item) for item in key if str(item))
    return str(key)


def _json_safe_value(value: Any) -> Any:
    if value is None:
        return None
    if hasattr(value, "isoformat"):
        try:
            return value.isoformat()
        except TypeError:
            pass
    if hasattr(value, "item"):
        try:
            return value.item()
        except Exception:
            pass
    return value


def _sanitize_provider_error(value: Any) -> str:
    text = str(value or "")
    text = re.sub(r"Basic\s+[A-Za-z0-9+/=]+", "Basic <redacted>", text)
    text = re.sub(r"(?i)(api[_-]?key['\"\s:=]+)[^'\"\s,}]+", r"\1<redacted>", text)
    return text


def _native_error_code_message(value: Any) -> tuple[str | None, str | None]:
    text = _sanitize_provider_error(value)
    if not text:
        return None, None
    match = re.search(r"\b(\d{3})\s+([A-Za-z0-9_]+)", text)
    if match:
        return match.group(2), text
    return None, text


def _record_timestamp(record: Mapping[str, Any] | None) -> datetime:
    if not record:
        return datetime.fromtimestamp(0, tz=UTC)
    raw = record.get("ts_event") or (record.get("hd") if isinstance(record.get("hd"), Mapping) else {}).get("ts_event")
    if raw is None:
        return datetime.fromtimestamp(0, tz=UTC)
    if isinstance(raw, (int, float)):
        return datetime.fromtimestamp(float(raw) / 1_000_000_000, tz=UTC)
    normalized = str(raw).replace("Z", "+00:00")
    parsed = datetime.fromisoformat(normalized)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _resolution_from_payload(*, request: DatabentoSymbolResolutionRequest, payload: Mapping[str, Any]) -> DatabentoSymbolResolution:
    resolution_date = request.resolution_date or datetime.now(UTC).date()
    resolution_start = request.resolution_start or resolution_date
    resolution_end = request.resolution_end or (resolution_date + timedelta(days=1))
    mappings = payload.get("result") or payload.get("mappings") or payload.get("symbols") or {}
    rows: Any = None
    if isinstance(mappings, Mapping):
        rows = mappings.get(request.requested_symbol) or next(iter(mappings.values()), None)
    elif isinstance(mappings, Sequence) and mappings:
        rows = mappings
    mapping_rows = _mapping_rows(rows)
    row = _active_mapping_row(mapping_rows, resolution_date=resolution_date)
    if row is None:
        return DatabentoSymbolResolution(
            requested_symbol=request.requested_symbol,
            dataset=request.dataset,
            stype_in=request.stype_in,
            stype_out=request.stype_out,
            resolved_instrument_id=None,
            raw_symbol=None,
            resolution_date=resolution_date,
            resolution_start=resolution_start,
            resolution_end=resolution_end,
            mapping_intervals=tuple(dict(item) for item in mapping_rows),
            resolution_status=DatabentoResolutionStatus.NOT_FOUND,
            warnings=(f"Databento symbology response contained no active mapping for {resolution_date.isoformat()}.",),
        )
    start = _mapping_date(row.get("d0") or row.get("start_date") or row.get("start") or row.get("effective_start"))
    end = _mapping_date(row.get("d1") or row.get("end_date") or row.get("end") or row.get("effective_end"))
    resolved_symbol = _optional_str(row.get("s") or row.get("symbol") or row.get("raw_symbol") or row.get("instrument_id") or row.get("d_symbol"))
    resolved_instrument_id = _optional_str(row.get("instrument_id"))
    raw_symbol = _optional_str(row.get("raw_symbol") or row.get("symbol") or row.get("d_symbol"))
    if request.stype_out == "instrument_id":
        resolved_instrument_id = resolved_symbol or resolved_instrument_id
    elif request.stype_out == "raw_symbol":
        raw_symbol = resolved_symbol or raw_symbol
    return DatabentoSymbolResolution(
        requested_symbol=request.requested_symbol,
        dataset=request.dataset,
        stype_in=request.stype_in,
        stype_out=request.stype_out,
        resolved_instrument_id=resolved_instrument_id,
        raw_symbol=raw_symbol,
        effective_start=_date_to_datetime(start),
        effective_end=_date_to_datetime(end),
        resolution_date=resolution_date,
        resolution_start=resolution_start,
        resolution_end=resolution_end,
        mapping_intervals=tuple(dict(item) for item in mapping_rows),
        active_mapping=dict(row),
        resolution_status=DatabentoResolutionStatus.RESOLVED,
    )


def _mapping_rows(rows: Any) -> tuple[Mapping[str, Any], ...]:
    if isinstance(rows, Mapping):
        intervals = rows.get("intervals")
        if isinstance(intervals, Sequence) and intervals:
            return tuple(dict(item) for item in intervals if isinstance(item, Mapping))
        return (rows,)
    if isinstance(rows, Sequence) and not isinstance(rows, (str, bytes)):
        return tuple(dict(item) for item in rows if isinstance(item, Mapping))
    return ()


def _active_mapping_row(rows: Sequence[Mapping[str, Any]], *, resolution_date: date) -> Mapping[str, Any] | None:
    undated: Mapping[str, Any] | None = None
    for row in rows:
        start = _mapping_date(row.get("d0") or row.get("start_date") or row.get("start") or row.get("effective_start"))
        end = _mapping_date(row.get("d1") or row.get("end_date") or row.get("end") or row.get("effective_end"))
        if start is None and end is None and undated is None:
            undated = row
            continue
        if start is not None and resolution_date < start:
            continue
        if end is not None and resolution_date >= end:
            continue
        return row
    return undated


def _optional_str(value: Any) -> str | None:
    normalized = str(value or "").strip()
    return normalized or None


def _optional_date(value: Any) -> date | None:
    if value is None:
        return None
    normalized = str(value or "").strip()
    if not normalized:
        return None
    parsed = _mapping_date(normalized)
    if parsed is None:
        raise DatabentoQuoteProviderError(f"invalid resolution date: {normalized}")
    return parsed


def _mapping_date(value: Any) -> date | None:
    normalized = str(value or "").strip()
    if not normalized:
        return None
    if len(normalized) == 8 and normalized.isdigit():
        try:
            return date(int(normalized[0:4]), int(normalized[4:6]), int(normalized[6:8]))
        except ValueError:
            return None
    try:
        return date.fromisoformat(normalized[:10])
    except ValueError:
        return None


def _date_to_datetime(value: date | None) -> datetime | None:
    return datetime(value.year, value.month, value.day, tzinfo=UTC) if value is not None else None


def _extract_available_end(detail: str) -> datetime | None:
    payload: Any = None
    try:
        payload = json.loads(detail)
    except json.JSONDecodeError:
        payload = None
    if isinstance(payload, Mapping):
        for candidate in _walk_available_end_values(payload):
            parsed = _provider_timestamp(candidate)
            if parsed is not None:
                return parsed
    for match in re.findall(r"available end of dataset[^\(]*\(([^\)]+)\)", detail, flags=re.IGNORECASE):
        parsed = _provider_timestamp(match)
        if parsed is not None:
            return parsed
    for match in re.findall(r"available[_ ]end[^\(]*\(([^\)]+)\)", detail, flags=re.IGNORECASE):
        parsed = _provider_timestamp(match)
        if parsed is not None:
            return parsed
    for match in re.findall(r"available[_ ]end['\"\s:=]+([0-9T:\.\-+Z]+)", detail, flags=re.IGNORECASE):
        parsed = _provider_timestamp(match)
        if parsed is not None:
            return parsed
    return None


def _walk_available_end_values(value: Any) -> tuple[Any, ...]:
    if isinstance(value, Mapping):
        values: list[Any] = []
        for key, item in value.items():
            if str(key).lower() in {"available_end", "available end", "end"}:
                values.append(item)
            elif isinstance(item, Mapping):
                values.extend(_walk_available_end_values(item))
            elif isinstance(item, Sequence) and not isinstance(item, (str, bytes)):
                values.extend(_walk_available_end_values(item))
        return tuple(values)
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes)):
        values = []
        for item in value:
            values.extend(_walk_available_end_values(item))
        return tuple(values)
    return ()


def _provider_timestamp(value: Any) -> datetime | None:
    normalized = str(value or "").strip().strip("'\"")
    if not normalized or not re.search(r"\d{4}-\d{2}-\d{2}", normalized):
        return None
    normalized = normalized.replace("Z", "+00:00")
    normalized = re.sub(r"(\.\d{6})\d+(\+00:00|[+-]\d{2}:\d{2})$", r"\1\2", normalized)
    try:
        parsed = datetime.fromisoformat(normalized)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _optional_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    normalized = str(value or "").strip()
    if not normalized:
        return None
    try:
        parsed = datetime.fromisoformat(normalized.replace("Z", "+00:00"))
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=UTC)
    return parsed.astimezone(UTC)


def _basic_auth_header(api_key: str) -> str:
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    return f"Basic {token}"
