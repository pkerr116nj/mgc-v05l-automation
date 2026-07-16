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
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping

from mgc_v05l.execution_core.track_b_contract_identity import normalize_track_b_contract_row

CURRENT_STATE_AUTHORITY_ALLOWED = "CURRENT_STATE_AUTHORITY_ALLOWED"
CURRENT_STATE_AUTHORITY_BLOCKED = "CURRENT_STATE_AUTHORITY_BLOCKED"

EXIT_CAPABILITY_READY = "EXIT_CAPABILITY_READY"
EXIT_CAPABILITY_STALE = "EXIT_CAPABILITY_STALE"
EXIT_CAPABILITY_APPLY_BLOCKED = "EXIT_CAPABILITY_APPLY_BLOCKED"
EXIT_CAPABILITY_PROCESS_DOWN = "EXIT_CAPABILITY_PROCESS_DOWN"
EXIT_CAPABILITY_CLOSE_PATH_UNAVAILABLE = "EXIT_CAPABILITY_CLOSE_PATH_UNAVAILABLE"

BROKER_TRUTH_CRITICAL = "BROKER_TRUTH_CRITICAL"
MARKET_TRUTH_CRITICAL = "MARKET_TRUTH_CRITICAL"
RISK_TRUTH_CRITICAL = "RISK_TRUTH_CRITICAL"
DIAGNOSTIC_ONLY = "DIAGNOSTIC_ONLY"

EXPECTED_PAPER_ACCOUNT = "DUM882026"
TRACK_B_FUTURES_ROOTS = frozenset(
    {"MES", "MNQ", "MGC", "GC", "NQ", "ES", "ZT", "ZF", "ZN", "ZB", "BTC", "MBT", "ETH", "MET", "SOL", "MSL"}
)
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
    managed_exit_status: Mapping[str, Any] = field(default_factory=dict)
    require_exit_capability: bool = True
    managed_exit_max_age_seconds: float = 180.0


def evaluate_exit_capability(
    managed_exit_status: Mapping[str, Any],
    *,
    now: datetime | None = None,
    max_age_seconds: float = 180.0,
) -> dict[str, Any]:
    actual_now = now or datetime.now(timezone.utc)
    blockers: list[str] = []
    status = dict(managed_exit_status or {})
    classification = str(status.get("classification") or status.get("status") or "").strip().upper()
    mode = str(status.get("mode") or status.get("apply_mode") or "").strip().upper()
    pid = int_or_none(status.get("pid") or status.get("process_pid"))
    generated_at = parse_datetime(status.get("heartbeat_at") or status.get("generated_at") or status.get("last_success_at"))
    age_seconds = None
    fresh = False
    if generated_at is not None:
        observed = generated_at if generated_at.tzinfo else generated_at.replace(tzinfo=timezone.utc)
        age_seconds = max(0.0, (actual_now.astimezone(timezone.utc) - observed.astimezone(timezone.utc)).total_seconds())
        fresh = age_seconds <= max_age_seconds

    if not status or pid is None or pid <= 0:
        blockers.append(EXIT_CAPABILITY_PROCESS_DOWN)
    if not fresh:
        blockers.append(EXIT_CAPABILITY_STALE)
    if mode != "GUARDED_CLOSE_ONLY_APPLY":
        blockers.append(EXIT_CAPABILITY_CLOSE_PATH_UNAVAILABLE)
    if classification == "APPLY_BLOCKED" or "APPLY_BLOCKED" in classification:
        blockers.append(EXIT_CAPABILITY_APPLY_BLOCKED)

    close_path_available = (
        mode == "GUARDED_CLOSE_ONLY_APPLY"
        and status.get("live_money_eligible") is not True
        and status.get("paper_proof_invoked") is not True
    )
    if not close_path_available:
        if EXIT_CAPABILITY_CLOSE_PATH_UNAVAILABLE not in blockers:
            blockers.append(EXIT_CAPABILITY_CLOSE_PATH_UNAVAILABLE)

    ready = not blockers
    return {
        "classification": EXIT_CAPABILITY_READY if ready else blockers[0],
        "ready": ready,
        "block_reasons": list(dict.fromkeys(blockers)),
        "managed_exit_pid": pid,
        "managed_exit_mode": mode or None,
        "managed_exit_classification": classification or None,
        "heartbeat_at": status.get("heartbeat_at") or status.get("generated_at"),
        "age_seconds": age_seconds,
        "max_age_seconds": max_age_seconds,
        "close_path_available": close_path_available,
        "stale_artifact_policy": "ENTRY_BLOCKING_CLOSE_AUTHORITY_SIGNAL",
    }


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

    exit_capability = evaluate_exit_capability(
        authority_input.managed_exit_status,
        now=now,
        max_age_seconds=authority_input.managed_exit_max_age_seconds,
    )
    if authority_input.require_exit_capability and exit_capability.get("ready") is not True:
        for reason in list(exit_capability.get("block_reasons") or []):
            block(
                RISK_TRUTH_CRITICAL,
                str(reason),
                "PAPER new entry is blocked because Managed Exit close capability is unavailable.",
                exit_capability=exit_capability,
            )

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
    instrument_open_orders = [row for row in track_b_open_orders if order_matches_instrument(row, instrument)]
    if instrument_open_orders:
        block(
            BROKER_TRUTH_CRITICAL,
            "duplicate_or_conflicting_working_order",
            "PAPER mutation is blocked by actual same-instrument Track B broker working orders.",
            broker_open_orders=instrument_open_orders,
        )

    unknown_order_count = open_order_truth_unknown_count(authority_input.open_order_truth)
    if unknown_order_count > 0:
        block(
            BROKER_TRUTH_CRITICAL,
            "unknown_open_orders",
            f"PAPER mutation is blocked by {unknown_order_count} unknown broker open order(s).",
            unknown_order_count=unknown_order_count,
        )
    duplicate_close_group_count = open_order_truth_duplicate_close_group_count(authority_input.open_order_truth)
    if duplicate_close_group_count > 0:
        block(
            BROKER_TRUTH_CRITICAL,
            "duplicate_close_groups",
            f"PAPER mutation is blocked by {duplicate_close_group_count} duplicate close order group(s).",
            duplicate_close_group_count=duplicate_close_group_count,
        )
    review_required_count = open_order_truth_review_required_count(authority_input.open_order_truth)
    if review_required_count > 0:
        block(
            BROKER_TRUTH_CRITICAL,
            "review_required",
            f"PAPER mutation is blocked by {review_required_count} current review-required exposure(s).",
            review_required_count=review_required_count,
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
        "exit_capability": exit_capability,
        "authority_scope": "CURRENT_BROKER_ORDER_MARKET_IDENTITY_TRUTH",
        "broker_truth": {
            "positions_known": positions_known,
            "track_b_position_count": len(track_b_positions),
            "track_b_nonflat_position_count": len(nonflat_positions),
            "instrument_nonflat_position_count": len(instrument_nonflat_positions),
            "open_orders_known": open_orders_known,
            "track_b_open_order_count": len(track_b_open_orders),
            "instrument_open_order_count": len(instrument_open_orders),
            "unknown_order_count": unknown_order_count,
            "duplicate_close_group_count": duplicate_close_group_count,
            "review_required_count": review_required_count,
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


def order_matches_instrument(row: Mapping[str, Any], instrument: str) -> bool:
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
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or row.get("contract") or "").strip().upper()
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


def open_order_truth_open_order_count(open_order_truth: Mapping[str, Any]) -> int:
    for key in (
        "open_order_count",
        "broker_open_order_count",
        "track_b_broker_open_order_count",
    ):
        value = int_or_none(open_order_truth.get(key))
        if value is not None:
            return value
    rows = open_order_truth.get("broker_open_orders") or open_order_truth.get("open_orders") or open_order_truth.get("orders") or []
    if isinstance(rows, list):
        return len(rows)
    return 0


def open_order_truth_duplicate_close_group_count(open_order_truth: Mapping[str, Any]) -> int:
    for key in (
        "duplicate_close_group_count",
        "duplicate_close_order_group_count",
        "duplicate_group_count",
    ):
        value = int_or_none(open_order_truth.get(key))
        if value is not None:
            return value
    rows = open_order_truth.get("duplicate_close_order_groups") or open_order_truth.get("duplicate_close_groups") or []
    if isinstance(rows, list):
        return len(rows)
    return 0


def open_order_truth_review_required_count(open_order_truth: Mapping[str, Any]) -> int:
    for key in (
        "review_required_count",
        "current_scope_review_required_count",
        "review_required_exposure_count",
    ):
        value = int_or_none(open_order_truth.get(key))
        if value is not None:
            return max(0, value)
    summary = open_order_truth.get("summary")
    if isinstance(summary, Mapping):
        for key in (
            "review_required_count",
            "current_scope_review_required_count",
            "review_required_exposure_count",
        ):
            value = int_or_none(summary.get(key))
            if value is not None:
                return max(0, value)
    rows = open_order_truth.get("review_required_positions") or open_order_truth.get("review_required_exposures") or []
    if isinstance(rows, list):
        return len(rows)
    return 0


def open_order_truth_is_global_complete(open_order_truth: Mapping[str, Any]) -> bool:
    scope = str(
        open_order_truth.get("canonical_refresh_scope")
        or open_order_truth.get("refresh_scope")
        or open_order_truth.get("scope")
        or ""
    ).strip().upper()
    return scope == "GLOBAL_COMPLETE"


def open_order_truth_is_global_no_open_orders(open_order_truth: Mapping[str, Any]) -> bool:
    if not open_order_truth_is_global_complete(open_order_truth):
        return False
    classification = str(open_order_truth.get("classification") or open_order_truth.get("state") or "").strip().upper()
    if classification not in {"NO_OPEN_ORDERS", "OPEN_ORDER_TRUTH_CLEAN_FLAT"}:
        return False
    return (
        open_order_truth_open_order_count(open_order_truth) == 0
        and open_order_truth_unknown_count(open_order_truth) == 0
        and open_order_truth_duplicate_close_group_count(open_order_truth) == 0
    )


def normalize_current_broker_position(
    row: Mapping[str, Any],
    *,
    account_id: str,
    instrument: str,
    local_symbol: str,
    con_id: int,
) -> dict[str, Any]:
    """Normalize a current broker-position row for entry/exit authority.

    Fresh IBKR snapshots sometimes carry only ``local_symbol``/expiry and no
    con_id. The shared contract resolver gets first chance to prove exact
    identity; legacy exact rows with both local symbol and con_id remain
    accepted for older fixtures and archived artifacts.
    """

    account = str(row.get("account_id") or row.get("account") or account_id or "").strip()
    if account != str(account_id or "").strip():
        return {}
    quantity = decimal_or_zero(row.get("quantity") or row.get("position") or row.get("signed_qty"))
    if quantity == Decimal("0"):
        return {}

    normalized = normalize_track_b_contract_row(row, account_id=account)
    identity = normalized.get("contract_identity") if isinstance(normalized.get("contract_identity"), Mapping) else {}
    resolved = bool(identity.get("resolved"))
    row_local_symbol = str(normalized.get("local_symbol") or row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    row_con_id = int_or_none(normalized.get("con_id") or row.get("con_id") or row.get("conId") or row.get("qualified_contract_identifier"))
    row_symbol = str(
        normalized.get("track_b_root")
        or normalized.get("symbol")
        or normalized.get("instrument")
        or row.get("track_b_root")
        or row.get("symbol")
        or row.get("instrument")
        or ""
    ).strip().upper()
    expected_symbol = str(instrument or "").strip().upper()
    expected_local_symbol = str(local_symbol or "").strip().upper()
    expected_con_id = int(con_id or 0)

    if resolved:
        if expected_symbol and row_symbol != expected_symbol:
            return {}
        if expected_local_symbol and row_local_symbol != expected_local_symbol:
            return {}
        if expected_con_id > 0 and row_con_id != expected_con_id:
            return {}
    else:
        exact_legacy_identity = bool(
            expected_local_symbol
            and row_local_symbol == expected_local_symbol
            and (
                (expected_con_id > 0 and row_con_id == expected_con_id)
                or (row_con_id is None and expected_symbol and row_symbol == expected_symbol)
            )
        )
        instrument_only_identity = bool(
            expected_symbol
            and row_symbol == expected_symbol
            and not expected_local_symbol
            and expected_con_id <= 0
        )
        if not (exact_legacy_identity or instrument_only_identity):
            return {}

    return {
        **dict(row),
        **{key: value for key, value in dict(normalized).items() if key != "quantity"},
        "account_id": account,
        "local_symbol": row_local_symbol or expected_local_symbol,
        "con_id": int(row_con_id or expected_con_id),
        "symbol": row_symbol or expected_symbol,
        "track_b_root": row_symbol or expected_symbol,
        "quantity": str(quantity),
        "contract_identity": identity or normalized.get("contract_identity") or {},
    }


def current_state_same_contract(
    row: Mapping[str, Any],
    *,
    account_id: str,
    local_symbol: str,
    con_id: int,
) -> bool:
    broker = row.get("broker_position") if isinstance(row.get("broker_position"), Mapping) else {}
    account = str(row.get("account_id") or row.get("account") or broker.get("account_id") or broker.get("account") or account_id or "").strip()
    if account != str(account_id or "").strip():
        return False
    normalized = normalize_track_b_contract_row({**dict(broker), **dict(row)}, account_id=account)
    row_local_symbol = str(
        normalized.get("local_symbol")
        or row.get("local_symbol")
        or row.get("localSymbol")
        or row.get("contract")
        or broker.get("local_symbol")
        or broker.get("localSymbol")
        or ""
    ).strip().upper()
    row_con_id = int_or_none(
        normalized.get("con_id")
        or row.get("con_id")
        or row.get("conId")
        or broker.get("con_id")
        or broker.get("conId")
    )
    expected_local_symbol = str(local_symbol or "").strip().upper()
    expected_con_id = int(con_id or 0)
    return bool(
        (expected_con_id > 0 and row_con_id == expected_con_id)
        or (expected_local_symbol and row_local_symbol == expected_local_symbol)
    )


def same_contract_working_close_qty(
    *,
    open_order_truth: Mapping[str, Any],
    account_id: str,
    local_symbol: str,
    con_id: int,
) -> Decimal:
    """Return broker-confirmed working close quantity for the exact contract.

    This intentionally ignores managed-order registry rows. Registry duplicate
    rows are diagnostics; fresh broker open-order truth is the authority.
    """

    total = Decimal("0")
    rows = [*list_or_empty(open_order_truth.get("broker_open_orders")), *list_or_empty(open_order_truth.get("open_orders"))]
    for row in (mapping_or_empty(item) for item in rows):
        if not current_state_same_contract(row, account_id=account_id, local_symbol=local_symbol, con_id=con_id):
            continue
        if row.get("working") is False:
            continue
        status = str(row.get("status") or row.get("order_status") or "").strip().upper()
        if status and status not in {"SUBMITTED", "PRESUBMITTED", "PENDING_SUBMIT", "PENDING_SUBMITTING", "API_PENDING", "HELD"}:
            continue
        total += abs(decimal_or_zero(row.get("remaining_quantity") or row.get("remaining") or row.get("quantity") or "0"))
    return total


def same_contract_unknown_order_count(
    *,
    open_order_truth: Mapping[str, Any],
    account_id: str,
    local_symbol: str,
    con_id: int,
) -> int:
    total_unknown = open_order_truth_unknown_count(open_order_truth)
    rows = [
        *list_or_empty(open_order_truth.get("unknown_open_orders")),
        *list_or_empty(open_order_truth.get("unknown_orders")),
        *list_or_empty(open_order_truth.get("unknown_broker_open_orders")),
    ]
    scoped = sum(
        1
        for row in (mapping_or_empty(item) for item in rows)
        if current_state_same_contract(row, account_id=account_id, local_symbol=local_symbol, con_id=con_id)
    )
    return max(total_unknown, scoped)


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


def decimal_or_zero(value: Any) -> Decimal:
    try:
        return Decimal(str(value or "0"))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def mapping_or_empty(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, Mapping) else {}


def list_or_empty(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []
