"""Validated index-option vertical construction and locked mutation gateway."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..production_link.client import SchwabBrokerHttpClient
from .products import product_for_root, width_allowed


# Live transmission is compiled only behind the runtime, launch, origin, and preview gates below.
# Runtime and origin gates remain mandatory.
LIVE_TRANSMISSION_COMPILED = True
_OPTION_RE = re.compile(r"^([A-Z0-9.$]{1,6})\s*(\d{6})([CP])(\d{8})$")


class SpreadValidationError(ValueError):
    """Raised when a proposed spread is not a supported defined-risk vertical."""


class TransmissionDisabledError(RuntimeError):
    """Raised before a broker mutation while a transmission gate is closed."""


@dataclass(frozen=True)
class ParsedOptionSymbol:
    raw: str
    root: str
    expiration: str
    option_type: str
    strike: Decimal


@dataclass(frozen=True)
class NdxpSpreadRequest:
    account_hash: str
    short_symbol: str
    long_symbol: str
    quantity: int
    limit_price: Decimal
    action: str = "OPEN"
    duration: str = "DAY"
    session: str = "NORMAL"

    @classmethod
    def from_json(cls, payload: dict[str, Any]) -> "NdxpSpreadRequest":
        try:
            quantity = int(payload.get("quantity"))
            limit_price = Decimal(str(payload.get("limit_price", payload.get("net_credit"))))
        except (TypeError, ValueError, InvalidOperation) as exc:
            raise SpreadValidationError("Quantity and limit price must be valid numbers.") from exc
        request = cls(
            account_hash=str(payload.get("account_hash") or "").strip(),
            short_symbol=str(payload.get("short_symbol") or "").strip().upper(),
            long_symbol=str(payload.get("long_symbol") or "").strip().upper(),
            quantity=quantity,
            limit_price=limit_price,
            action=str(payload.get("action") or "OPEN").strip().upper(),
            duration=str(payload.get("duration") or "DAY").strip().upper(),
            session=str(payload.get("session") or "NORMAL").strip().upper(),
        )
        validate_spread_request(request)
        return request


def parse_option_symbol(symbol: str) -> ParsedOptionSymbol:
    text = str(symbol or "").strip().upper()
    match = _OPTION_RE.fullmatch(text)
    if not match:
        raise SpreadValidationError(f"Unrecognized Schwab option symbol: {symbol!r}.")
    root, expiration, option_type, strike_raw = match.groups()
    return ParsedOptionSymbol(
        raw=text,
        root=root.rstrip(),
        expiration=expiration,
        option_type="CALL" if option_type == "C" else "PUT",
        strike=Decimal(strike_raw) / Decimal("1000"),
    )


def validate_spread_request(request: NdxpSpreadRequest, *, required_width: Decimal | None = None) -> dict[str, Any]:
    if not request.account_hash:
        raise SpreadValidationError("A live-verified Schwab account hash is required.")
    if request.quantity <= 0 or request.quantity > 100:
        raise SpreadValidationError("Quantity must be between 1 and 100 contracts.")
    if request.action not in {"OPEN", "CLOSE"}:
        raise SpreadValidationError("Spread action must be OPEN or CLOSE.")
    if request.duration != "DAY" or request.session != "NORMAL":
        raise SpreadValidationError("The index-spread terminal permits NORMAL-session DAY orders only.")

    short = parse_option_symbol(request.short_symbol)
    long = parse_option_symbol(request.long_symbol)
    product = product_for_root(short.root)
    if product is None or product_for_root(long.root) != product:
        raise SpreadValidationError("Both legs must belong to the same supported NDX, SPX, or RUT option product.")
    if short.root != long.root or short.expiration != long.expiration or short.option_type != long.option_type:
        raise SpreadValidationError("Vertical legs must share the same root, expiration, and option type.")
    width = abs(short.strike - long.strike)
    if required_width is not None and width != required_width:
        raise SpreadValidationError(f"The selected product requires an exact {required_width}-point spread.")
    if not width_allowed(product, width):
        allowed = ", ".join(str(value) for value in product.spread_widths)
        raise SpreadValidationError(f"{product.display_symbol} spread width must be one of: {allowed} points.")
    if request.limit_price <= 0 or request.limit_price >= width:
        raise SpreadValidationError("Net limit price must be greater than zero and below the spread width.")
    if short.option_type == "CALL" and short.strike >= long.strike:
        raise SpreadValidationError("A call credit spread must sell the lower strike and buy the higher strike.")
    if short.option_type == "PUT" and short.strike <= long.strike:
        raise SpreadValidationError("A put credit spread must sell the higher strike and buy the lower strike.")

    multiplier = Decimal(product.multiplier)
    gross_width_dollars = width * multiplier * request.quantity
    order_value_dollars = request.limit_price * multiplier * request.quantity
    summary = {
        "root": short.root,
        "product": product.key,
        "expiration": short.expiration,
        "option_type": short.option_type,
        "short_strike": str(short.strike),
        "long_strike": str(long.strike),
        "width_points": str(width),
        "quantity": request.quantity,
        "action": request.action,
        "price_effect": "CREDIT" if request.action == "OPEN" else "DEBIT",
        "limit_price": str(request.limit_price),
        "gross_width_dollars": str(gross_width_dollars),
    }
    if request.action == "OPEN":
        summary.update(
            premium_dollars=str(order_value_dollars),
            maximum_loss_dollars=str(gross_width_dollars - order_value_dollars),
        )
    else:
        summary["closing_debit_dollars"] = str(order_value_dollars)
    return summary


def build_vertical_order_payload(request: NdxpSpreadRequest) -> dict[str, Any]:
    validate_spread_request(request)
    opening = request.action == "OPEN"
    return {
        "session": "NORMAL",
        "duration": "DAY",
        "orderType": "NET_CREDIT" if opening else "NET_DEBIT",
        "complexOrderStrategyType": "VERTICAL",
        "price": _format_price(request.limit_price),
        "orderStrategyType": "SINGLE",
        "orderLegCollection": [
            {
                "instruction": "SELL_TO_OPEN" if opening else "BUY_TO_CLOSE",
                "quantity": request.quantity,
                "instrument": {"symbol": request.short_symbol, "assetType": "OPTION"},
            },
            {
                "instruction": "BUY_TO_OPEN" if opening else "SELL_TO_CLOSE",
                "quantity": request.quantity,
                "instrument": {"symbol": request.long_symbol, "assetType": "OPTION"},
            },
        ],
    }


class LockedSchwabMutationGateway:
    """Broker mutation methods protected by independent reviewed runtime gates."""

    def __init__(self, client: SchwabBrokerHttpClient, *, live_trading_requested: bool = False) -> None:
        self._client = client
        self._live_trading_requested = live_trading_requested

    @property
    def enabled(self) -> bool:
        return (
            LIVE_TRANSMISSION_COMPILED
            and self._live_trading_requested
            and os.environ.get("MGC_NDXP_LIVE_TRANSMISSION_ENABLED") == "1"
        )

    def submit(self, request: NdxpSpreadRequest) -> dict[str, Any]:
        self._assert_enabled("submission")
        return self._client.submit_order(request.account_hash, build_vertical_order_payload(request))

    def cancel(self, *, account_hash: str, broker_order_id: str) -> dict[str, Any]:
        self._assert_enabled("cancellation")
        if not account_hash.strip() or not broker_order_id.strip():
            raise SpreadValidationError("Cancellation requires account_hash and broker_order_id.")
        return self._client.cancel_order(account_hash.strip(), broker_order_id.strip())

    def replace(self, *, broker_order_id: str, request: NdxpSpreadRequest) -> dict[str, Any]:
        self._assert_enabled("replacement")
        if not broker_order_id.strip():
            raise SpreadValidationError("Replacement requires broker_order_id.")
        return self._client.replace_order(
            request.account_hash,
            broker_order_id.strip(),
            build_vertical_order_payload(request),
        )

    def _assert_enabled(self, operation: str) -> None:
        if not LIVE_TRANSMISSION_COMPILED:
            raise TransmissionDisabledError(
                f"Schwab {operation} is source-locked. LIVE_TRANSMISSION_COMPILED is False pending live-trading authorization."
            )
        if os.environ.get("MGC_NDXP_LIVE_TRANSMISSION_ENABLED") != "1":
            raise TransmissionDisabledError(
                f"Schwab {operation} is runtime-locked. MGC_NDXP_LIVE_TRANSMISSION_ENABLED is not 1."
            )
        if not self._live_trading_requested:
            raise TransmissionDisabledError(
                f"Schwab {operation} is launch-locked. Start the terminal with --live-trading."
            )


def _format_price(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01')):.2f}"
