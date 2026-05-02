"""Provider-agnostic quote interface for Track B pricing."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation
from typing import Any, Protocol

from .models import JsonSerializable, TrackBModelError, normalize_decimal, require_aware_datetime, require_id
from .pricing import MarketDataMode, MarketDataRole


class QuoteProviderError(ValueError):
    """Raised when a provider quote cannot safely price an order."""


class QuoteProvider(Protocol):
    provider_name: str

    def get_quote(self, contract_key: str) -> "QuoteSnapshot": ...


@dataclass(frozen=True)
class QuoteSnapshot(JsonSerializable):
    provider: str
    mode: str
    role: str
    contract_key: str
    bid: Decimal | int | float | str | None
    ask: Decimal | int | float | str | None
    last: Decimal | int | float | str | None
    timestamp: datetime
    tick_size: Decimal | int | float | str | None
    exchange: str
    currency: str
    provider_warnings: tuple[str, ...] = ()
    delayed_data_warning_seen: bool = False
    source_latency_ms: Decimal | int | float | str | None = None
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "provider", require_id(self.provider, "provider"))
        object.__setattr__(self, "mode", _normalize_mode(self.mode))
        object.__setattr__(self, "role", _normalize_role(self.role))
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        object.__setattr__(self, "timestamp", require_aware_datetime(self.timestamp, "timestamp"))
        if self.tick_size is not None:
            object.__setattr__(self, "tick_size", normalize_decimal(self.tick_size, "tick_size"))
        object.__setattr__(self, "exchange", require_id(self.exchange, "exchange"))
        object.__setattr__(self, "currency", require_id(self.currency, "currency").upper())
        object.__setattr__(self, "provider_warnings", tuple(str(item) for item in self.provider_warnings))
        if self.source_latency_ms is not None:
            latency = normalize_decimal(self.source_latency_ms, "source_latency_ms")
            if latency < 0:
                raise TrackBModelError("source_latency_ms must be non-negative.")
            object.__setattr__(self, "source_latency_ms", latency)

    def age_seconds(self, now: datetime) -> Decimal:
        require_aware_datetime(now, "now")
        return Decimal(str((now - self.timestamp).total_seconds()))


def validate_quote_for_pricing(
    quote: QuoteSnapshot,
    *,
    now: datetime,
    max_age_seconds: Decimal | int | float | str,
    allow_delayed_for_paper: bool,
    live_money: bool,
    allow_locked: bool = False,
) -> QuoteSnapshot:
    validate_market_data_policy(quote, allow_delayed_for_paper=allow_delayed_for_paper, live_money=live_money)
    validate_quote_freshness(quote, now=now, max_age_seconds=max_age_seconds)
    validate_bid_ask_sanity(quote, allow_locked=allow_locked)
    validate_tick_size_compatibility(quote)
    return quote


def validate_market_data_policy(
    quote: QuoteSnapshot,
    *,
    allow_delayed_for_paper: bool,
    live_money: bool,
) -> None:
    if live_money and quote.mode != MarketDataMode.REALTIME:
        raise QuoteProviderError("delayed or unknown market data blocks live-money readiness")
    if not live_money and quote.mode in {MarketDataMode.DELAYED, MarketDataMode.DELAYED_FROZEN} and not allow_delayed_for_paper:
        raise QuoteProviderError("delayed market data requires explicit paper-proof approval")
    if quote.mode == MarketDataMode.UNKNOWN:
        raise QuoteProviderError("unknown market data mode blocks quote-derived pricing")


def validate_quote_freshness(
    quote: QuoteSnapshot,
    *,
    now: datetime,
    max_age_seconds: Decimal | int | float | str,
) -> None:
    require_aware_datetime(now, "now")
    max_age = _positive_decimal(max_age_seconds, "max_age_seconds")
    age = quote.age_seconds(now)
    if age < 0:
        raise QuoteProviderError("quote timestamp is in the future")
    if age > max_age:
        raise QuoteProviderError("quote is stale")


def validate_bid_ask_sanity(quote: QuoteSnapshot, *, allow_locked: bool = False) -> None:
    bid = _positive_decimal(quote.bid, "bid")
    ask = _positive_decimal(quote.ask, "ask")
    _positive_decimal(quote.last, "last")
    if ask < bid:
        raise QuoteProviderError("quote is crossed")
    if ask == bid and not allow_locked:
        raise QuoteProviderError("quote is locked")


def validate_tick_size_compatibility(quote: QuoteSnapshot) -> None:
    tick = _positive_decimal(quote.tick_size, "tick_size")
    for field_name, value in (("bid", quote.bid), ("ask", quote.ask), ("last", quote.last)):
        price = _positive_decimal(value, field_name)
        if price % tick != 0:
            raise QuoteProviderError(f"{field_name} is not compatible with tick_size")


def production_live_money_quote_ready(quote: QuoteSnapshot) -> bool:
    return quote.mode == MarketDataMode.REALTIME


def paper_proof_quote_ready(quote: QuoteSnapshot, *, allow_delayed_for_paper: bool) -> bool:
    if quote.mode == MarketDataMode.REALTIME:
        return True
    if quote.mode in {MarketDataMode.DELAYED, MarketDataMode.DELAYED_FROZEN}:
        return allow_delayed_for_paper
    return False


def _normalize_mode(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in {MarketDataMode.REALTIME, MarketDataMode.DELAYED, MarketDataMode.DELAYED_FROZEN, MarketDataMode.UNKNOWN}:
        raise TrackBModelError("mode must be REALTIME, DELAYED, DELAYED_FROZEN, or UNKNOWN.")
    return normalized


def _normalize_role(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in {MarketDataRole.PRIMARY, MarketDataRole.SECONDARY, MarketDataRole.BACKUP, MarketDataRole.DIAGNOSTIC}:
        raise TrackBModelError("role must be PRIMARY, SECONDARY, BACKUP, or DIAGNOSTIC.")
    return normalized


def _positive_decimal(value: Decimal | int | float | str | None, field_name: str) -> Decimal:
    if value is None:
        raise QuoteProviderError(f"{field_name} is required")
    try:
        normalized = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise QuoteProviderError(f"{field_name} must be decimal-compatible") from exc
    if normalized <= 0:
        raise QuoteProviderError(f"{field_name} must be positive")
    return normalized
