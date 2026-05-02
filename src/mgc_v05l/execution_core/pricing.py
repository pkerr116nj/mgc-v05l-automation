"""Marketable bounded limit pricing for Track B proof runs."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_CEILING, ROUND_FLOOR
from typing import Any

from .models import Action, JsonSerializable, TrackBModelError, normalize_action, require_aware_datetime, require_id, to_jsonable


class PricingError(ValueError):
    """Raised when a proof quote cannot safely produce a bounded LMT price."""


class MarketDataMode(str):
    REALTIME = "REALTIME"
    DELAYED = "DELAYED"
    DELAYED_FROZEN = "DELAYED_FROZEN"
    UNKNOWN = "UNKNOWN"


class MarketDataRole(str):
    PRIMARY = "PRIMARY"
    SECONDARY = "SECONDARY"
    BACKUP = "BACKUP"
    DIAGNOSTIC = "DIAGNOSTIC"


@dataclass(frozen=True)
class QuoteObservation(JsonSerializable):
    quote_id: str
    run_id: str
    contract_key: str
    source: str
    bid: Decimal | int | float | str | None
    ask: Decimal | int | float | str | None
    last: Decimal | int | float | str | None
    observed_at: datetime
    market_data_provider: str = "UNKNOWN"
    market_data_mode: str = MarketDataMode.UNKNOWN
    market_data_role: str = MarketDataRole.DIAGNOSTIC
    delayed_data_warning_seen: bool = False
    timestamp: datetime | None = None
    tick_size: Decimal | int | float | str | None = None
    exchange: str | None = None
    currency: str | None = None
    provider_warnings: tuple[str, ...] = ()
    raw: dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        object.__setattr__(self, "quote_id", require_id(self.quote_id, "quote_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        object.__setattr__(self, "source", require_id(self.source, "source"))
        object.__setattr__(self, "observed_at", require_aware_datetime(self.observed_at, "observed_at"))
        timestamp = self.timestamp or self.observed_at
        object.__setattr__(self, "timestamp", require_aware_datetime(timestamp, "timestamp"))
        object.__setattr__(self, "market_data_provider", require_id(self.market_data_provider, "market_data_provider"))
        object.__setattr__(self, "market_data_mode", _normalize_market_data_mode(self.market_data_mode))
        object.__setattr__(self, "market_data_role", _normalize_market_data_role(self.market_data_role))
        object.__setattr__(self, "provider_warnings", tuple(str(item) for item in self.provider_warnings))


def _normalize_market_data_mode(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in {
        MarketDataMode.REALTIME,
        MarketDataMode.DELAYED,
        MarketDataMode.DELAYED_FROZEN,
        MarketDataMode.UNKNOWN,
    }:
        raise TrackBModelError("market_data_mode must be REALTIME, DELAYED, DELAYED_FROZEN, or UNKNOWN.")
    return normalized


def _normalize_market_data_role(value: str) -> str:
    normalized = str(value or "").strip().upper()
    if normalized not in {MarketDataRole.PRIMARY, MarketDataRole.SECONDARY, MarketDataRole.BACKUP, MarketDataRole.DIAGNOSTIC}:
        raise TrackBModelError("market_data_role must be PRIMARY, SECONDARY, BACKUP, or DIAGNOSTIC.")
    return normalized


@dataclass(frozen=True)
class PricingDecision(JsonSerializable):
    pricing_decision_id: str
    run_id: str
    quote_id: str
    contract_key: str
    action: Action | str
    limit_price: Decimal
    bid: Decimal
    ask: Decimal
    last: Decimal | None
    mid: Decimal
    tick_size: Decimal
    fill_offset_ticks: Decimal
    max_distance_ticks: Decimal
    max_distance_percent: Decimal
    distance_from_mid: Decimal
    formula: str
    created_at: datetime

    def __post_init__(self) -> None:
        object.__setattr__(self, "pricing_decision_id", require_id(self.pricing_decision_id, "pricing_decision_id"))
        object.__setattr__(self, "run_id", require_id(self.run_id, "run_id"))
        object.__setattr__(self, "quote_id", require_id(self.quote_id, "quote_id"))
        object.__setattr__(self, "contract_key", require_id(self.contract_key, "contract_key"))
        object.__setattr__(self, "action", normalize_action(self.action))
        object.__setattr__(self, "created_at", require_aware_datetime(self.created_at, "created_at"))


def create_marketable_limit_decision(
    *,
    pricing_decision_id: str,
    quote: QuoteObservation,
    action: Action | str,
    tick_size: Decimal | int | float | str,
    fill_offset_ticks: Decimal | int | float | str,
    max_quote_age_seconds: Decimal | int | float | str,
    max_distance_ticks: Decimal | int | float | str,
    max_distance_percent: Decimal | int | float | str,
    now: datetime,
) -> PricingDecision:
    """Create a marketable but bounded LMT price from one fresh quote."""

    require_aware_datetime(now, "now")
    normalized_action = normalize_action(action)
    bid = _positive_decimal(quote.bid, "bid")
    ask = _positive_decimal(quote.ask, "ask")
    last = _optional_positive_decimal(quote.last, "last")
    tick = _positive_decimal(tick_size, "tick_size")
    offset_ticks = _positive_decimal(fill_offset_ticks, "fill_offset_ticks")
    max_age = _positive_decimal(max_quote_age_seconds, "max_quote_age_seconds")
    distance_ticks = _positive_decimal(max_distance_ticks, "max_distance_ticks")
    distance_percent = _positive_decimal(max_distance_percent, "max_distance_percent")

    age_seconds = Decimal(str((now - quote.observed_at).total_seconds()))
    if age_seconds < 0:
        raise PricingError("quote timestamp is in the future")
    if age_seconds > max_age:
        raise PricingError("quote is stale")
    if ask == bid:
        raise PricingError("quote is locked")
    if ask < bid:
        raise PricingError("quote is crossed")

    mid = (bid + ask) / Decimal("2")
    if mid <= 0:
        raise PricingError("quote mid cannot be computed")

    max_allowed_distance = min(distance_ticks * tick, mid * (distance_percent / Decimal("100")))
    if last is not None and abs(last - mid) > max_allowed_distance:
        raise PricingError("last price contradicts bid/ask")

    if normalized_action == Action.BUY:
        raw_limit = ask + (offset_ticks * tick)
        limit = round_up_to_tick(raw_limit, tick)
        formula = "raw_buy_limit = ask + fill_offset_ticks * tick_size; buy_limit = round_up_to_tick(raw_buy_limit)"
    else:
        raw_limit = bid - (offset_ticks * tick)
        limit = round_down_to_tick(raw_limit, tick)
        formula = "raw_sell_limit = bid - fill_offset_ticks * tick_size; sell_limit = round_down_to_tick(raw_sell_limit)"

    distance_from_mid = abs(limit - mid)
    if distance_from_mid > max_allowed_distance:
        raise PricingError("limit price exceeds maximum distance from mid")

    return PricingDecision(
        pricing_decision_id=pricing_decision_id,
        run_id=quote.run_id,
        quote_id=quote.quote_id,
        contract_key=quote.contract_key,
        action=normalized_action,
        limit_price=limit,
        bid=bid,
        ask=ask,
        last=last,
        mid=mid,
        tick_size=tick,
        fill_offset_ticks=offset_ticks,
        max_distance_ticks=distance_ticks,
        max_distance_percent=distance_percent,
        distance_from_mid=distance_from_mid,
        formula=formula,
        created_at=now,
    )


def round_up_to_tick(value: Decimal | int | float | str, tick_size: Decimal | int | float | str) -> Decimal:
    value_decimal = _decimal(value, "value")
    tick = _positive_decimal(tick_size, "tick_size")
    return (value_decimal / tick).to_integral_value(rounding=ROUND_CEILING) * tick


def round_down_to_tick(value: Decimal | int | float | str, tick_size: Decimal | int | float | str) -> Decimal:
    value_decimal = _decimal(value, "value")
    tick = _positive_decimal(tick_size, "tick_size")
    return (value_decimal / tick).to_integral_value(rounding=ROUND_FLOOR) * tick


def _positive_decimal(value: Decimal | int | float | str | None, field_name: str) -> Decimal:
    decimal_value = _decimal(value, field_name)
    if decimal_value <= 0:
        raise PricingError(f"{field_name} must be positive")
    return decimal_value


def _optional_positive_decimal(value: Decimal | int | float | str | None, field_name: str) -> Decimal | None:
    if value is None or value == "":
        return None
    return _positive_decimal(value, field_name)


def _decimal(value: Decimal | int | float | str | None, field_name: str) -> Decimal:
    if value is None:
        raise PricingError(f"{field_name} is required")
    try:
        return value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, ValueError) as exc:
        raise PricingError(f"{field_name} must be decimal-compatible") from exc


def pricing_payload(decision: PricingDecision) -> dict[str, Any]:
    return to_jsonable(decision)
