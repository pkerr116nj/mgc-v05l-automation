"""Track B-local Databento quote provider.

This module is intentionally independent of the legacy market-data runtime.
It uses an injectable stdlib HTTP transport and normalizes provider records into
Track B QuoteSnapshot objects.
"""

from __future__ import annotations

import base64
import json
from dataclasses import dataclass
from datetime import UTC, datetime, timedelta
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Protocol, Sequence
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .pricing import MarketDataMode, MarketDataRole
from .quote_provider import QuoteProviderError, QuoteSnapshot


class DatabentoQuoteProviderError(QuoteProviderError):
    """Raised when Databento quote retrieval cannot produce a snapshot."""


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


@dataclass(frozen=True)
class DatabentoQuoteProviderConfig:
    contract_key: str
    databento_symbol: str
    tick_size: str
    exchange: str
    currency: str
    api_key: str
    dataset: str = "GLBX.MDP3"
    stype_in: str = "raw_symbol"
    base_url: str = "https://hist.databento.com/v0"
    bbo_schema: str = "mbp-1"
    trades_schema: str = "trades"
    lookback_seconds: int = 300
    record_limit: int = 1000
    realtime_max_age_seconds: int = 15


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
            raise DatabentoQuoteProviderError(f"Databento HTTP error {exc.code}: {detail}") from exc
        except URLError as exc:  # pragma: no cover - exercised only by real operator command.
            raise DatabentoQuoteProviderError(f"Databento transport error: {exc}") from exc
        return tuple(json.loads(line) for line in text.splitlines() if line.strip())


class DatabentoQuoteProvider:
    provider_name = "DATABENTO"

    def __init__(
        self,
        *,
        config: DatabentoQuoteProviderConfig,
        transport: DatabentoQuoteTransport | None = None,
        now: datetime | None = None,
    ) -> None:
        self.config = config
        self.transport = transport or UrllibDatabentoQuoteTransport()
        self._now = now
        _require_config(config)

    def get_quote(self, contract_key: str) -> QuoteSnapshot:
        if str(contract_key or "").strip() != self.config.contract_key:
            raise DatabentoQuoteProviderError("contract_key must match explicit Databento quote provider mapping")
        now = self._now or datetime.now(UTC)
        start = now - timedelta(seconds=max(int(self.config.lookback_seconds), 1))
        bbo_records = tuple(
            self.transport.request_records(
                base_url=self.config.base_url,
                api_key=self.config.api_key,
                dataset=self.config.dataset,
                symbol=self.config.databento_symbol,
                schema=self.config.bbo_schema,
                start=start,
                end=now,
                stype_in=self.config.stype_in,
                limit=self.config.record_limit,
            )
        )
        trade_records = tuple(
            self.transport.request_records(
                base_url=self.config.base_url,
                api_key=self.config.api_key,
                dataset=self.config.dataset,
                symbol=self.config.databento_symbol,
                schema=self.config.trades_schema,
                start=start,
                end=now,
                stype_in=self.config.stype_in,
                limit=self.config.record_limit,
            )
        )
        latest_bbo = _latest_record_with_bid_ask(bbo_records)
        latest_trade = _latest_record_with_price(trade_records)
        if latest_bbo is None:
            raise DatabentoQuoteProviderError("Databento returned no bid/ask quote records for explicit symbol")
        bid = _first_decimal(latest_bbo, ("bid_px", "bid_price", "bid"))
        ask = _first_decimal(latest_bbo, ("ask_px", "ask_price", "ask"))
        last = _first_decimal(latest_trade or {}, ("price", "last", "last_px")) if latest_trade is not None else None
        timestamp = max(_record_timestamp(latest_bbo), _record_timestamp(latest_trade) if latest_trade is not None else _record_timestamp(latest_bbo))
        warnings: list[str] = []
        if latest_trade is None:
            warnings.append("Databento returned no trade record for last price in lookback window.")
        mode = MarketDataMode.REALTIME if (now - timestamp).total_seconds() <= int(self.config.realtime_max_age_seconds) else MarketDataMode.UNKNOWN
        return QuoteSnapshot(
            provider=self.provider_name,
            mode=mode,
            role=MarketDataRole.PRIMARY,
            contract_key=self.config.contract_key,
            provider_symbol=self.config.databento_symbol,
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
                "stype_in": self.config.stype_in,
                "bbo_schema": self.config.bbo_schema,
                "trades_schema": self.config.trades_schema,
                "bbo_record_count": len(bbo_records),
                "trade_record_count": len(trade_records),
            },
        )


def _require_config(config: DatabentoQuoteProviderConfig) -> None:
    required = {
        "contract_key": config.contract_key,
        "databento_symbol": config.databento_symbol,
        "tick_size": config.tick_size,
        "exchange": config.exchange,
        "currency": config.currency,
        "api_key": config.api_key,
    }
    missing = [key for key, value in required.items() if not str(value or "").strip()]
    if missing:
        raise DatabentoQuoteProviderError(f"Databento quote provider missing required field(s): {', '.join(missing)}")


def _latest_record_with_bid_ask(records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    candidates = [record for record in records if _first_decimal(record, ("bid_px", "bid_price", "bid")) is not None and _first_decimal(record, ("ask_px", "ask_price", "ask")) is not None]
    return max(candidates, key=_record_timestamp) if candidates else None


def _latest_record_with_price(records: Sequence[Mapping[str, Any]]) -> Mapping[str, Any] | None:
    candidates = [record for record in records if _first_decimal(record, ("price", "last", "last_px")) is not None]
    return max(candidates, key=_record_timestamp) if candidates else None


def _first_decimal(record: Mapping[str, Any], keys: tuple[str, ...]) -> Decimal | None:
    for key in keys:
        value = _nested_value(record, key)
        if value is None or value == "":
            continue
        try:
            return Decimal(str(value))
        except (InvalidOperation, ValueError):
            continue
    return None


def _nested_value(record: Mapping[str, Any], key: str) -> Any:
    if key in record:
        return record[key]
    levels = record.get("levels")
    if isinstance(levels, Sequence) and levels:
        first_level = levels[0]
        if isinstance(first_level, Mapping) and key in first_level:
            return first_level[key]
    return None


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


def _basic_auth_header(api_key: str) -> str:
    token = base64.b64encode(f"{api_key}:".encode("utf-8")).decode("ascii")
    return f"Basic {token}"
