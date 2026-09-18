"""Validated NDX/NDXP vertical order construction and locked mutation gateway."""

from __future__ import annotations

import os
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any

from ..production_link.client import SchwabBrokerHttpClient


# Live transmission is compiled only for the deliberately constrained pilot below.
# Runtime and origin gates remain mandatory.
LIVE_TRANSMISSION_COMPILED = True
LIVE_PILOT_QUANTITY = 1
_OPTION_RE = re.compile(r"^([A-Z0-9.$]{1,6})\s*(\d{6})([CP])(\d{8})$")
_ALLOWED_ROOTS = {"NDX", "NDXP"}


class SpreadValidationError(ValueError):
    """Raised when a proposed spread is not an exact, defined-risk NDX vertical."""


class TransmissionDisabledError(RuntimeError):
    """Raised before a broker mutation while the pilot gate is closed."""


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


def validate_spread_request(request: NdxpSpreadRequest, *, required_width: Decimal = Decimal("10")) -> dict[str, Any]:
    if not request.account_hash:
        raise SpreadValidationError("A live-verified Schwab account hash is required.")
    if request.quantity <= 0 or request.quantity > 100:
        raise SpreadValidationError("Quantity must be between 1 and 100 contracts.")
    if request.action not in {"OPEN", "CLOSE"}:
        raise SpreadValidationError("Spread action must be OPEN or CLOSE.")
    if request.limit_price <= 0 or request.limit_price >= required_width:
        raise SpreadValidationError("Net limit price must be greater than zero and below the spread width.")
    if request.duration != "DAY" or request.session != "NORMAL":
        raise SpreadValidationError("The initial NDXP terminal permits NORMAL-session DAY orders only.")

    short = parse_option_symbol(request.short_symbol)
    long = parse_option_symbol(request.long_symbol)
    if short.root not in _ALLOWED_ROOTS or long.root not in _ALLOWED_ROOTS:
        raise SpreadValidationError("Both legs must be NDX or NDXP options supplied by Schwab.")
    if short.root != long.root or short.expiration != long.expiration or short.option_type != long.option_type:
        raise SpreadValidationError("Vertical legs must share the same root, expiration, and option type.")
    width = abs(short.strike - long.strike)
    if width != required_width:
        raise SpreadValidationError(f"The initial terminal requires an exact {required_width}-point spread.")
    if short.option_type == "CALL" and short.strike >= long.strike:
        raise SpreadValidationError("A call credit spread must sell the lower strike and buy the higher strike.")
    if short.option_type == "PUT" and short.strike <= long.strike:
        raise SpreadValidationError("A put credit spread must sell the higher strike and buy the lower strike.")

    gross_width_dollars = width * Decimal("100") * request.quantity
    order_value_dollars = request.limit_price * Decimal("100") * request.quantity
    summary = {
        "root": short.root,
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
    """Implemented broker mutation methods that fail closed until a reviewed pilot unlock."""

    def __init__(self, client: SchwabBrokerHttpClient, *, pilot_requested: bool = False) -> None:
        self._client = client
        self._pilot_requested = pilot_requested

    @property
    def enabled(self) -> bool:
        return (
            LIVE_TRANSMISSION_COMPILED
            and self._pilot_requested
            and os.environ.get("MGC_NDXP_LIVE_TRANSMISSION_ENABLED") == "1"
        )

    def submit(self, request: NdxpSpreadRequest) -> dict[str, Any]:
        self._assert_enabled("submission")
        validate_live_pilot_request(request)
        return self._client.submit_order(request.account_hash, build_vertical_order_payload(request))

    def cancel(self, *, account_hash: str, broker_order_id: str) -> dict[str, Any]:
        self._assert_enabled("cancellation")
        if not account_hash.strip() or not broker_order_id.strip():
            raise SpreadValidationError("Cancellation requires account_hash and broker_order_id.")
        return self._client.cancel_order(account_hash.strip(), broker_order_id.strip())

    def replace(self, *, broker_order_id: str, request: NdxpSpreadRequest) -> dict[str, Any]:
        self._assert_enabled("replacement")
        raise TransmissionDisabledError("Order replacement is disabled during the one-contract live pilot.")

    def _assert_enabled(self, operation: str) -> None:
        if not LIVE_TRANSMISSION_COMPILED:
            raise TransmissionDisabledError(
                f"Schwab {operation} is source-locked. LIVE_TRANSMISSION_COMPILED is False pending Patrick's live-pilot authorization."
            )
        if os.environ.get("MGC_NDXP_LIVE_TRANSMISSION_ENABLED") != "1":
            raise TransmissionDisabledError(
                f"Schwab {operation} is runtime-locked. MGC_NDXP_LIVE_TRANSMISSION_ENABLED is not 1."
            )
        if not self._pilot_requested:
            raise TransmissionDisabledError(
                f"Schwab {operation} is launch-locked. Start the terminal with --live-pilot."
            )


def validate_live_pilot_request(request: NdxpSpreadRequest) -> None:
    """Enforce the one-contract opening envelope for the first live pilot."""

    validate_spread_request(request)
    if request.action != "OPEN":
        raise SpreadValidationError("The first live pilot permits opening orders only.")
    if request.quantity != LIVE_PILOT_QUANTITY:
        raise SpreadValidationError("The first live pilot is hard-limited to exactly one spread.")


def _format_price(value: Decimal) -> str:
    return f"{value.quantize(Decimal('0.01')):.2f}"
