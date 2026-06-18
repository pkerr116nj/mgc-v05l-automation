"""Current broker/market truth authority helpers for Track B PAPER.

This module is deliberately narrower than strategy governance. It answers only
whether the current broker/order/market/identity state is safe enough for a
broker mutation. Historical publications, registry disagreements, and stale
projection rows belong in diagnostics unless they prove one of these current
truth risks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_broker_startup_authority import TRACK_B_FUTURES_ROOTS

CURRENT_STATE_AUTHORITY_ALLOWED = "CURRENT_STATE_AUTHORITY_ALLOWED"
CURRENT_STATE_AUTHORITY_BLOCKED = "CURRENT_STATE_AUTHORITY_BLOCKED"

BROKER_TRUTH_CRITICAL = "BROKER_TRUTH_CRITICAL"
MARKET_TRUTH_CRITICAL = "MARKET_TRUTH_CRITICAL"
RISK_TRUTH_CRITICAL = "RISK_TRUTH_CRITICAL"
DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"

EXPECTED_PAPER_ACCOUNT = "DUM882026"
TRACK_B_FUTURES_SYMBOLS = set(TRACK_B_FUTURES_ROOTS)
DEFAULT_PRICE_MAX_AGE_SECONDS = 180.0


@dataclass(frozen=True)
class CurrentStateAuthorityInput:
    account_id: str
    instrument: str
    action: str
    quantity: float
    paper_only: bool = True
    live_money_eligible: bool = False
    paper_proof: bool = False
    flat_start_required: bool = True
    max_quantity: float = 1.0
    broker_positions_snapshot: Mapping[str, Any] = field(default_factory=dict)
    broker_open_orders_snapshot: Mapping[str, Any] = field(default_factory=dict)
    open_order_truth: Mapping[str, Any] = field(default_factory=dict)
    runtime_price: Mapping[str, Any] = field(default_factory=dict)
    contract: Mapping[str, Any] = field(default_factory=dict)
    require_resolved_contract: bool = True
    require_fresh_price: bool = True
    price_max_age_seconds: float = DEFAULT_PRICE_MAX_AGE_SECONDS
    now: datetime | None = None
    diagnostics: Mapping[str, Any] = field(default_factory=dict)


def evaluate_current_state_authority(authority_input: CurrentStateAuthorityInput) -> dict[str, Any]:
    """Evaluate current-state broker/order/market truth.

    The returned ``block_reasons`` are intentionally stable because bridge,
    startup, and managed-exit callers surface them to operator artifacts.
    """

    now = authority_input.now or datetime.now(timezone.utc)
    blockers: list[dict[str, Any]] = []
    instrument = str(authority_input.instrument or "").strip().upper()

    def block(category: str, reason: str, detail: str, **extra: Any) -> None:
        row = {"category": category, "reason": reason, "detail": detail}
        row.update({key: value for key, value in extra.items() if value is not None})
        blockers.append(row)

    if str(authority_input.account_id or "").strip() != EXPECTED_PAPER_ACCOUNT:
        block(BROKER_TRUTH_CRITICAL, "wrong_account", "PAPER account must be DUM882026.")
    if not bool(authority_input.paper_only):
        block(BROKER_TRUTH_CRITICAL, "paper_only_false", "PAPER intent must remain paper_only=true.")
    if bool(authority_input.live_money_eligible):
        block(BROKER_TRUTH_CRITICAL, "live_money_eligible", "live_money_eligible=true blocks PAPER mutation.")
    if bool(authority_input.paper_proof):
        block(BROKER_TRUTH_CRITICAL, "paper_proof_true", "paper_proof=true blocks PAPER mutation.")

    quantity = float_or_zero(authority_input.quantity)
    max_quantity = float_or_zero(authority_input.max_quantity)
    if quantity <= 0.0 or max_quantity <= 0.0 or quantity > max_quantity:
        block(
            RISK_TRUTH_CRITICAL,
            "invalid_quantity",
            f"PAPER quantity {quantity} exceeds configured cap {max_quantity} or is not positive.",
        )

    positions = broker_positions(authority_input.broker_positions_snapshot)
    positions_known = positions_known_for_paper(authority_input.broker_positions_snapshot)
    if not positions_known:
        block(BROKER_TRUTH_CRITICAL, "broker_positions_unavailable", "Broker positions are unavailable or incomplete.")
    track_b_positions = [row for row in positions if is_track_b_futures_position(row)]
    nonflat_positions = [row for row in track_b_positions if abs(float_or_zero(row.get("quantity"))) > 1e-9]
    instrument_nonflat_positions = [row for row in nonflat_positions if position_matches_instrument(row, instrument)]
    if authority_input.flat_start_required and instrument_nonflat_positions:
        block(
            BROKER_TRUTH_CRITICAL,
            "broker_nonflat_flat_start_violation",
            "Flat-start PAPER entry is blocked by actual same-instrument broker futures exposure.",
            broker_positions=instrument_nonflat_positions,
        )

    open_orders = broker_open_orders(authority_input.broker_open_orders_snapshot)
    open_orders_known = open_orders_known_for_paper(authority_input.broker_open_orders_snapshot)
    if not open_orders_known:
        block(BROKER_TRUTH_CRITICAL, "broker_open_orders_unavailable", "Broker open orders are unavailable or incomplete.")
    track_b_open_orders = [row for row in open_orders if is_track_b_order(row)]
    if track_b_open_orders:
        block(
            BROKER_TRUTH_CRITICAL,
            "duplicate_or_conflicting_working_order",
            "PAPER mutation is blocked by actual Track B broker working orders.",
            broker_open_orders=track_b_open_orders,
        )

    unknown_order_count = open_order_truth_unknown_count(authority_input.open_order_truth)
    if unknown_order_count > 0:
        block(
            BROKER_TRUTH_CRITICAL,
            "unknown_open_orders",
            f"PAPER mutation is blocked by {unknown_order_count} unknown broker open order(s).",
            unknown_order_count=unknown_order_count,
        )

    price_status = runtime_price_status(
        authority_input.runtime_price,
        now=now,
        max_age_seconds=authority_input.price_max_age_seconds,
    )
    if authority_input.require_fresh_price:
        if not price_status["available"]:
            block(MARKET_TRUTH_CRITICAL, "runtime_price_unavailable", "Runtime market price is missing or invalid.")
        elif not price_status["fresh"]:
            block(
                MARKET_TRUTH_CRITICAL,
                "runtime_price_stale",
                "Runtime market price is stale.",
                price_age_seconds=price_status.get("age_seconds"),
            )

    contract_status = contract_identity_status(
        authority_input.contract,
        expected_instrument=instrument,
        require_resolved_contract=authority_input.require_resolved_contract,
    )
    if not contract_status["valid"]:
        block(MARKET_TRUTH_CRITICAL, contract_status["reason"], contract_status["detail"])

    allowed = not blockers
    return {
        "classification": CURRENT_STATE_AUTHORITY_ALLOWED if allowed else CURRENT_STATE_AUTHORITY_BLOCKED,
        "allowed": allowed,
        "blockers": blockers,
        "block_reasons": [str(row.get("reason") or "") for row in blockers],
        "diagnostics": dict(authority_input.diagnostics or {}),
        "authority_scope": "CURRENT_BROKER_ORDER_MARKET_IDENTITY_TRUTH",
        "broker_truth": {
            "positions_known": positions_known,
            "track_b_position_count": len(track_b_positions),
            "track_b_nonflat_position_count": len(nonflat_positions),
            "instrument_nonflat_position_count": len(instrument_nonflat_positions),
            "open_orders_known": open_orders_known,
            "track_b_open_order_count": len(track_b_open_orders),
            "unknown_order_count": unknown_order_count,
        },
        "market_truth": {
            "instrument": instrument,
            "price": price_status,
            "contract": contract_status,
        },
    }


def broker_positions(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in list(snapshot.get("positions") or []) if isinstance(row, Mapping)]


def positions_known_for_paper(snapshot: Mapping[str, Any]) -> bool:
    if snapshot.get("ok") is not True:
        return False
    if "positions" not in snapshot:
        return False
    return str(snapshot.get("account") or snapshot.get("selected_account_id") or "").strip() == EXPECTED_PAPER_ACCOUNT


def broker_open_orders(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in list(snapshot.get("open_orders") or []) if isinstance(row, Mapping)]


def open_orders_known_for_paper(snapshot: Mapping[str, Any]) -> bool:
    if snapshot.get("ok") is not True:
        return False
    if snapshot.get("open_orders_complete") is not True:
        return False
    return str(snapshot.get("account") or snapshot.get("selected_account_id") or "").strip() == EXPECTED_PAPER_ACCOUNT


def is_track_b_futures_position(row: Mapping[str, Any]) -> bool:
    sec_type = str(row.get("security_type") or row.get("secType") or "").strip().upper()
    symbol = str(row.get("symbol") or row.get("track_b_root") or "").strip().upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    if sec_type and sec_type != "FUT":
        return False
    root = symbol or track_b_root_from_local_symbol(local_symbol)
    return root in TRACK_B_FUTURES_SYMBOLS


def position_matches_instrument(row: Mapping[str, Any], instrument: str) -> bool:
    expected = str(instrument or "").strip().upper()
    if not expected:
        return False
    symbol = str(
        row.get("symbol")
        or row.get("track_b_root")
        or row.get("instrument")
        or row.get("instrument_family")
        or ""
    ).strip().upper()
    if symbol == expected:
        return True
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    return bool(local_symbol) and local_symbol.startswith(expected)


def is_track_b_order(row: Mapping[str, Any]) -> bool:
    symbol = str(row.get("symbol") or row.get("track_b_root") or "").strip().upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    sec_type = str(row.get("security_type") or row.get("secType") or "").strip().upper()
    if sec_type and sec_type != "FUT":
        return False
    root = symbol or track_b_root_from_local_symbol(local_symbol)
    return root in TRACK_B_FUTURES_SYMBOLS


def track_b_root_from_local_symbol(local_symbol: str) -> str:
    text = str(local_symbol or "").strip().upper()
    if not text:
        return ""
    for root in sorted(TRACK_B_FUTURES_SYMBOLS, key=len, reverse=True):
        if text.startswith(root):
            return root
    return "".join(ch for ch in text if ch.isalpha())[:3]


def open_order_truth_unknown_count(open_order_truth: Mapping[str, Any]) -> int:
    for key in (
        "unknown_open_order_count",
        "unknown_broker_open_order_count",
        "unknown_order_count",
    ):
        value = int_or_none(open_order_truth.get(key))
        if value is not None:
            return max(0, value)
    unknown_orders = open_order_truth.get("unknown_orders") or open_order_truth.get("unknown_broker_open_orders") or []
    if isinstance(unknown_orders, list):
        return len(unknown_orders)
    return 0


def runtime_price_status(price_payload: Mapping[str, Any], *, now: datetime, max_age_seconds: float) -> dict[str, Any]:
    price = float_or_none(price_payload.get("price") or price_payload.get("last") or price_payload.get("close"))
    timestamp = parse_datetime(price_payload.get("timestamp") or price_payload.get("generated_at") or price_payload.get("bar_end"))
    available = price is not None and price > 0.0 and timestamp is not None
    age_seconds = None
    fresh = False
    if timestamp is not None:
        timestamp = timestamp if timestamp.tzinfo else timestamp.replace(tzinfo=timezone.utc)
        age_seconds = max(0.0, (now.astimezone(timezone.utc) - timestamp.astimezone(timezone.utc)).total_seconds())
        fresh = age_seconds <= max_age_seconds
    return {
        "available": available,
        "fresh": bool(available and fresh),
        "price": price,
        "timestamp": None if timestamp is None else timestamp.isoformat(),
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "source": price_payload.get("source"),
    }


def contract_identity_status(
    contract: Mapping[str, Any],
    *,
    expected_instrument: str,
    require_resolved_contract: bool,
) -> dict[str, Any]:
    symbol = str(contract.get("symbol") or contract.get("instrument") or "").strip().upper()
    local_symbol = str(contract.get("local_symbol") or contract.get("localSymbol") or "").strip()
    expiry = str(contract.get("expiry") or contract.get("contract_month") or "").strip()
    con_id = int_or_none(contract.get("con_id") or contract.get("conId"))
    if expected_instrument and symbol and symbol != expected_instrument:
        return {
            "valid": False,
            "reason": "contract_instrument_mismatch",
            "detail": f"Resolved contract symbol {symbol} does not match intent instrument {expected_instrument}.",
        }
    if not symbol:
        return {"valid": False, "reason": "invalid_instrument", "detail": "Resolved contract instrument is missing."}
    if require_resolved_contract and (con_id is None or con_id <= 0):
        return {"valid": False, "reason": "unresolved_con_id", "detail": "Resolved contract con_id must be positive."}
    if require_resolved_contract and not local_symbol:
        return {"valid": False, "reason": "unresolved_local_symbol", "detail": "Resolved contract localSymbol is missing."}
    if require_resolved_contract and not expiry:
        return {"valid": False, "reason": "unresolved_expiry", "detail": "Resolved contract expiry is missing."}
    return {
        "valid": True,
        "reason": None,
        "detail": "Resolved contract identity is valid.",
        "symbol": symbol,
        "local_symbol": local_symbol or None,
        "expiry": expiry or None,
        "con_id": con_id,
    }


def parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def float_or_zero(value: Any) -> float:
    parsed = float_or_none(value)
    return 0.0 if parsed is None else parsed


def int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
