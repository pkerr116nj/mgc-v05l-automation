"""Broker/market-truth authority for Track B PAPER new entries."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

BROKER_MARKET_TRUTH_ENTRY_ALLOWED = "BROKER_MARKET_TRUTH_ENTRY_ALLOWED"
BROKER_MARKET_TRUTH_ENTRY_BLOCKED = "BROKER_MARKET_TRUTH_ENTRY_BLOCKED"

BROKER_TRUTH_CRITICAL = "BROKER_TRUTH_CRITICAL"
MARKET_TRUTH_CRITICAL = "MARKET_TRUTH_CRITICAL"
DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"

EXPECTED_PAPER_ACCOUNT = "DUM882026"
EXPECTED_EXECUTION_MODE = "IBKR_PAPER_BRIDGE"
EXPECTED_ROUTE_DESTINATION = "ibkr_paper_bridge_submit_capable"
TRACK_B_FUTURES_SYMBOLS = {"MES", "MNQ", "MGC", "ES", "NQ", "GC"}
DEFAULT_PRICE_MAX_AGE_SECONDS = 180.0


@dataclass(frozen=True)
class BrokerMarketTruthEntryAuthorityInput:
    account_id: str
    mode: str
    route_destination: str
    execution_mode: str
    lane_id: str
    instrument: str
    action: str
    quantity: float
    paper_only: bool = True
    live_money_eligible: bool = False
    paper_proof: bool = False
    flat_start_required: bool = True
    max_quantity: float = 1.0
    active_profile_lane_ids: Sequence[str] = field(default_factory=tuple)
    broker_positions_snapshot: Mapping[str, Any] = field(default_factory=dict)
    broker_open_orders_snapshot: Mapping[str, Any] = field(default_factory=dict)
    open_order_truth: Mapping[str, Any] = field(default_factory=dict)
    runtime_price: Mapping[str, Any] = field(default_factory=dict)
    contract: Mapping[str, Any] = field(default_factory=dict)
    require_resolved_contract: bool = True
    price_max_age_seconds: float = DEFAULT_PRICE_MAX_AGE_SECONDS
    now: datetime | None = None
    diagnostics: Mapping[str, Any] = field(default_factory=dict)


def evaluate_broker_market_truth_entry_authority(
    authority_input: BrokerMarketTruthEntryAuthorityInput,
) -> dict[str, Any]:
    """Return the one PAPER new-entry veto decision for broker-backed lanes."""

    now = authority_input.now or datetime.now(timezone.utc)
    blockers: list[dict[str, Any]] = []
    diagnostics = dict(authority_input.diagnostics or {})
    lane_ids = {str(value or "").strip() for value in authority_input.active_profile_lane_ids}
    lane_ids.discard("")
    lane_id = str(authority_input.lane_id or "").strip()
    instrument = str(authority_input.instrument or "").strip().upper()

    def block(category: str, reason: str, detail: str, **extra: Any) -> None:
        row = {"category": category, "reason": reason, "detail": detail}
        row.update({key: value for key, value in extra.items() if value is not None})
        blockers.append(row)

    if str(authority_input.account_id or "").strip() != EXPECTED_PAPER_ACCOUNT:
        block(BROKER_TRUTH_CRITICAL, "wrong_account", "PAPER entry account must be DUM882026.")
    if str(authority_input.mode or "").strip().upper() != "PAPER":
        block(BROKER_TRUTH_CRITICAL, "non_paper_mode", "PAPER entry authority only allows PAPER mode.")
    if str(authority_input.route_destination or "").strip() != EXPECTED_ROUTE_DESTINATION:
        block(BROKER_TRUTH_CRITICAL, "non_paper_route", "PAPER entry route must target the IBKR PAPER bridge.")
    if str(authority_input.execution_mode or "").strip().upper() != EXPECTED_EXECUTION_MODE:
        block(BROKER_TRUTH_CRITICAL, "wrong_execution_mode", "PAPER entry execution mode must be IBKR_PAPER_BRIDGE.")
    if lane_ids and lane_id not in lane_ids:
        block(MARKET_TRUTH_CRITICAL, "lane_not_in_active_profile_roster", "PAPER bridge lane is not in the active profile roster.")
    if not bool(authority_input.paper_only):
        block(BROKER_TRUTH_CRITICAL, "paper_only_false", "PAPER entry intent must remain paper_only=true.")
    if bool(authority_input.live_money_eligible):
        block(BROKER_TRUTH_CRITICAL, "live_money_eligible", "live_money_eligible=true blocks PAPER entry.")
    if bool(authority_input.paper_proof):
        block(BROKER_TRUTH_CRITICAL, "paper_proof_true", "paper_proof=true blocks PAPER entry.")

    try:
        quantity = float(authority_input.quantity)
    except (TypeError, ValueError):
        quantity = 0.0
    try:
        max_quantity = float(authority_input.max_quantity)
    except (TypeError, ValueError):
        max_quantity = 0.0
    if quantity <= 0.0 or max_quantity <= 0.0 or quantity > max_quantity:
        block(
            BROKER_TRUTH_CRITICAL,
            "invalid_quantity",
            f"PAPER entry quantity {quantity} exceeds configured cap {max_quantity} or is not positive.",
        )

    positions = _broker_positions(authority_input.broker_positions_snapshot)
    positions_known = _positions_known(authority_input.broker_positions_snapshot)
    if not positions_known:
        block(BROKER_TRUTH_CRITICAL, "broker_positions_unavailable", "Broker positions are unavailable or incomplete.")
    track_b_positions = [row for row in positions if _is_track_b_futures_position(row)]
    nonflat_positions = [row for row in track_b_positions if abs(_float(row.get("quantity"))) > 1e-9]
    if authority_input.flat_start_required and nonflat_positions:
        block(
            BROKER_TRUTH_CRITICAL,
            "broker_nonflat_flat_start_violation",
            "Flat-start PAPER entry is blocked by actual broker futures exposure.",
            broker_positions=nonflat_positions,
        )

    open_orders = _broker_open_orders(authority_input.broker_open_orders_snapshot)
    open_orders_known = _open_orders_known(authority_input.broker_open_orders_snapshot)
    if not open_orders_known:
        block(BROKER_TRUTH_CRITICAL, "broker_open_orders_unavailable", "Broker open orders are unavailable or incomplete.")
    track_b_open_orders = [row for row in open_orders if _is_track_b_order(row)]
    if track_b_open_orders:
        block(
            BROKER_TRUTH_CRITICAL,
            "duplicate_or_conflicting_working_order",
            "PAPER entry is blocked by actual Track B broker working orders.",
            broker_open_orders=track_b_open_orders,
        )

    unknown_order_count = _unknown_order_count(authority_input.open_order_truth)
    if unknown_order_count > 0:
        block(
            BROKER_TRUTH_CRITICAL,
            "unknown_open_orders",
            f"PAPER entry is blocked by {unknown_order_count} unknown broker open order(s).",
            unknown_order_count=unknown_order_count,
        )

    price_status = _runtime_price_status(authority_input.runtime_price, now=now, max_age_seconds=authority_input.price_max_age_seconds)
    if not price_status["available"]:
        block(MARKET_TRUTH_CRITICAL, "runtime_price_unavailable", "Runtime market price is missing or invalid.")
    elif not price_status["fresh"]:
        block(
            MARKET_TRUTH_CRITICAL,
            "runtime_price_stale",
            "Runtime market price is stale.",
            price_age_seconds=price_status.get("age_seconds"),
        )

    contract_status = _contract_identity_status(
        authority_input.contract,
        expected_instrument=instrument,
        require_resolved_contract=authority_input.require_resolved_contract,
    )
    if not contract_status["valid"]:
        block(MARKET_TRUTH_CRITICAL, contract_status["reason"], contract_status["detail"])

    allowed = not blockers
    return {
        "classification": BROKER_MARKET_TRUTH_ENTRY_ALLOWED if allowed else BROKER_MARKET_TRUTH_ENTRY_BLOCKED,
        "allowed": allowed,
        "blockers": blockers,
        "block_reasons": [str(row.get("reason") or "") for row in blockers],
        "diagnostics": diagnostics,
        "authority_scope": "BROKER_AND_MARKET_TRUTH_ONLY",
        "broker_truth": {
            "positions_known": positions_known,
            "track_b_position_count": len(track_b_positions),
            "track_b_nonflat_position_count": len(nonflat_positions),
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


def build_broker_market_truth_entry_authority_from_repo(
    *,
    repo_root: Path,
    account_id: str,
    mode: str,
    route_destination: str,
    execution_mode: str,
    lane_id: str,
    instrument: str,
    action: str,
    quantity: float,
    contract: Mapping[str, Any] | None = None,
    require_resolved_contract: bool = True,
    paper_only: bool = True,
    live_money_eligible: bool = False,
    paper_proof: bool = False,
    flat_start_required: bool = True,
    max_quantity: float = 1.0,
    now: datetime | None = None,
    price_max_age_seconds: float = DEFAULT_PRICE_MAX_AGE_SECONDS,
    diagnostics: Mapping[str, Any] | None = None,
) -> dict[str, Any]:
    root = Path(repo_root)
    active_profile = _load_json(
        root
        / "outputs"
        / "probationary_pattern_engine"
        / "paper_session"
        / "runtime"
        / "paper_config_in_force.json"
    )
    lane_ids = _active_profile_lane_ids(active_profile)
    positions = _load_json(root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_positions_snapshot.json")
    orders = _load_json(root / "outputs" / "reports" / "ibkr_read_only_verification" / "ibkr_open_orders_snapshot.json")
    order_truth = _load_json(root / "outputs" / "track_b_execution_core" / "open_order_truth" / "latest_open_order_truth.json")
    runtime_price = _load_runtime_price(root=root, instrument=instrument)
    return evaluate_broker_market_truth_entry_authority(
        BrokerMarketTruthEntryAuthorityInput(
            account_id=account_id,
            mode=mode,
            route_destination=route_destination,
            execution_mode=execution_mode,
            lane_id=lane_id,
            instrument=instrument,
            action=action,
            quantity=quantity,
            paper_only=paper_only,
            live_money_eligible=live_money_eligible,
            paper_proof=paper_proof,
            flat_start_required=flat_start_required,
            max_quantity=max_quantity,
            active_profile_lane_ids=tuple(lane_ids),
            broker_positions_snapshot=positions,
            broker_open_orders_snapshot=orders,
            open_order_truth=order_truth,
            runtime_price=runtime_price,
            contract=dict(contract or {}),
            require_resolved_contract=require_resolved_contract,
            price_max_age_seconds=price_max_age_seconds,
            now=now,
            diagnostics=dict(diagnostics or {}),
        )
    )


def _load_json(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def _active_profile_lane_ids(payload: Mapping[str, Any]) -> list[str]:
    lane_ids = [str(value or "").strip() for value in list(payload.get("active_lane_ids") or [])]
    for row in list(payload.get("lanes") or []):
        if isinstance(row, Mapping):
            lane_ids.append(str(row.get("lane_id") or row.get("strategy_id") or "").strip())
    return sorted({value for value in lane_ids if value})


def _load_runtime_price(*, root: Path, instrument: str) -> dict[str, Any]:
    symbol = str(instrument or "").strip().upper()
    payload = _load_json(
        root
        / "outputs"
        / "track_b_execution_core"
        / "phase1_runtime_market_data"
        / symbol
        / "1m"
        / "latest_runtime_candles.json"
    )
    bars = [dict(row) for row in list(payload.get("bars") or []) if isinstance(row, Mapping)]
    latest = bars[-1] if bars else {}
    return {
        "source": "phase1_runtime_market_data",
        "instrument": symbol,
        "price": latest.get("close") or latest.get("last"),
        "timestamp": latest.get("bar_end") or latest.get("timestamp") or payload.get("generated_at"),
        "bar_count": payload.get("bar_count") or len(bars),
    }


def _broker_positions(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in list(snapshot.get("positions") or []) if isinstance(row, Mapping)]


def _positions_known(snapshot: Mapping[str, Any]) -> bool:
    if snapshot.get("ok") is not True:
        return False
    if "positions" not in snapshot:
        return False
    return str(snapshot.get("account") or snapshot.get("selected_account_id") or "").strip() == EXPECTED_PAPER_ACCOUNT


def _broker_open_orders(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in list(snapshot.get("open_orders") or []) if isinstance(row, Mapping)]


def _open_orders_known(snapshot: Mapping[str, Any]) -> bool:
    if snapshot.get("ok") is not True:
        return False
    if snapshot.get("open_orders_complete") is not True:
        return False
    return str(snapshot.get("account") or snapshot.get("selected_account_id") or "").strip() == EXPECTED_PAPER_ACCOUNT


def _is_track_b_futures_position(row: Mapping[str, Any]) -> bool:
    sec_type = str(row.get("security_type") or row.get("secType") or "").strip().upper()
    symbol = str(row.get("symbol") or row.get("track_b_root") or "").strip().upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    if sec_type and sec_type != "FUT":
        return False
    root = symbol or "".join(ch for ch in local_symbol if ch.isalpha())[:3]
    return root in TRACK_B_FUTURES_SYMBOLS


def _is_track_b_order(row: Mapping[str, Any]) -> bool:
    symbol = str(row.get("symbol") or row.get("track_b_root") or "").strip().upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    sec_type = str(row.get("security_type") or row.get("secType") or "").strip().upper()
    if sec_type and sec_type != "FUT":
        return False
    root = symbol or "".join(ch for ch in local_symbol if ch.isalpha())[:3]
    return root in TRACK_B_FUTURES_SYMBOLS


def _unknown_order_count(open_order_truth: Mapping[str, Any]) -> int:
    for key in (
        "unknown_open_order_count",
        "unknown_broker_open_order_count",
        "unknown_order_count",
    ):
        value = _int_or_none(open_order_truth.get(key))
        if value is not None:
            return max(0, value)
    unknown_orders = open_order_truth.get("unknown_orders") or open_order_truth.get("unknown_broker_open_orders") or []
    if isinstance(unknown_orders, list):
        return len(unknown_orders)
    classification = str(open_order_truth.get("classification") or "").strip().upper()
    return 0 if classification in {"", "NO_OPEN_ORDERS"} else 0


def _runtime_price_status(price_payload: Mapping[str, Any], *, now: datetime, max_age_seconds: float) -> dict[str, Any]:
    price = _float_or_none(price_payload.get("price") or price_payload.get("last") or price_payload.get("close"))
    timestamp = _parse_datetime(price_payload.get("timestamp") or price_payload.get("generated_at") or price_payload.get("bar_end"))
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


def _contract_identity_status(
    contract: Mapping[str, Any],
    *,
    expected_instrument: str,
    require_resolved_contract: bool,
) -> dict[str, Any]:
    symbol = str(contract.get("symbol") or contract.get("instrument") or "").strip().upper()
    local_symbol = str(contract.get("local_symbol") or contract.get("localSymbol") or "").strip()
    expiry = str(contract.get("expiry") or contract.get("contract_month") or "").strip()
    con_id = _int_or_none(contract.get("con_id") or contract.get("conId"))
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


def _parse_datetime(value: Any) -> datetime | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float(value: Any) -> float:
    parsed = _float_or_none(value)
    return 0.0 if parsed is None else parsed


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
