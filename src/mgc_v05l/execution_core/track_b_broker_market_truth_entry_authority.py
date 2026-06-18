"""Broker/market-truth authority for Track B PAPER new entries."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_broker_startup_authority import TRACK_B_FUTURES_ROOTS
from mgc_v05l.execution_core.track_b_current_state_authority import (
    BROKER_TRUTH_CRITICAL,
    DEFAULT_PRICE_MAX_AGE_SECONDS,
    DIAGNOSTIC_ONLY,
    EXPECTED_PAPER_ACCOUNT,
    MARKET_TRUTH_CRITICAL,
    CurrentStateAuthorityInput,
    evaluate_current_state_authority,
    track_b_root_from_local_symbol,
)

BROKER_MARKET_TRUTH_ENTRY_ALLOWED = "BROKER_MARKET_TRUTH_ENTRY_ALLOWED"
BROKER_MARKET_TRUTH_ENTRY_BLOCKED = "BROKER_MARKET_TRUTH_ENTRY_BLOCKED"

EXPECTED_EXECUTION_MODE = "IBKR_PAPER_BRIDGE"
EXPECTED_ROUTE_DESTINATION = "ibkr_paper_bridge_submit_capable"
TRACK_B_FUTURES_SYMBOLS = set(TRACK_B_FUTURES_ROOTS)


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
    current_state = evaluate_current_state_authority(
        CurrentStateAuthorityInput(
            account_id=authority_input.account_id,
            instrument=instrument,
            action=authority_input.action,
            quantity=authority_input.quantity,
            paper_only=authority_input.paper_only,
            live_money_eligible=authority_input.live_money_eligible,
            paper_proof=authority_input.paper_proof,
            flat_start_required=authority_input.flat_start_required,
            max_quantity=authority_input.max_quantity,
            broker_positions_snapshot=authority_input.broker_positions_snapshot,
            broker_open_orders_snapshot=authority_input.broker_open_orders_snapshot,
            open_order_truth=authority_input.open_order_truth,
            runtime_price=authority_input.runtime_price,
            contract=authority_input.contract,
            require_resolved_contract=authority_input.require_resolved_contract,
            price_max_age_seconds=authority_input.price_max_age_seconds,
            now=now,
            diagnostics=diagnostics,
        )
    )
    blockers.extend(dict(row) for row in current_state.get("blockers") or [] if isinstance(row, Mapping))

    allowed = not blockers
    return {
        "classification": BROKER_MARKET_TRUTH_ENTRY_ALLOWED if allowed else BROKER_MARKET_TRUTH_ENTRY_BLOCKED,
        "allowed": allowed,
        "blockers": blockers,
        "block_reasons": [str(row.get("reason") or "") for row in blockers],
        "diagnostics": diagnostics,
        "authority_scope": "BROKER_AND_MARKET_TRUTH_ONLY",
        "broker_truth": {
            **dict(current_state.get("broker_truth") or {}),
        },
        "market_truth": {
            **dict(current_state.get("market_truth") or {}),
        },
        "current_state_authority": current_state,
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
    resolved_contract = _enrich_contract_from_broker_truth(
        root=root,
        contract=dict(contract or {}),
        instrument=instrument,
        broker_positions_snapshot=positions,
    )
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
            contract=resolved_contract,
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


def _enrich_contract_from_broker_truth(
    *,
    root: Path,
    contract: Mapping[str, Any],
    instrument: str,
    broker_positions_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    """Fill missing active-profile contract identity from fresh broker truth.

    Active-profile PAPER bridge lanes are configured by source instrument. Near
    expiry, the submit bridge can roll the static target to the next contract,
    but the startup-route authority runs earlier and needs the same concrete
    identity. IBKR position snapshots often retain zero-quantity rows for the
    current scoped futures contracts after flat settlement; those rows are
    broker truth for conId/localSymbol/expiry and are safe to use for identity
    enrichment. If no positive conId is available, the authority still blocks.
    """

    enriched = dict(contract or {})
    if _int_or_none(enriched.get("con_id")) is not None and enriched.get("local_symbol") and enriched.get("expiry"):
        return enriched
    expected = str(instrument or enriched.get("symbol") or "").strip().upper()
    if not expected:
        return enriched
    broker_identity = _broker_position_contract_identity(
        instrument=expected,
        broker_positions_snapshot=broker_positions_snapshot,
    )
    ledger_identities = _trade_ledger_contract_identities(root=root, instrument=expected)
    candidates: list[dict[str, Any]] = []
    if broker_identity:
        broker_local_symbol = str(broker_identity.get("local_symbol") or "").strip().upper()
        broker_expiry = str(broker_identity.get("expiry") or "").strip()
        broker_con_id = _int_or_none(broker_identity.get("con_id"))
        if broker_con_id is not None and broker_con_id > 0 and broker_local_symbol:
            candidates.append(broker_identity)
        else:
            for row in ledger_identities:
                row_local_symbol = str(row.get("local_symbol") or "").strip().upper()
                if broker_local_symbol and row_local_symbol != broker_local_symbol:
                    continue
                con_id = _int_or_none(row.get("con_id"))
                if con_id is None or con_id <= 0 or not row_local_symbol:
                    continue
                candidates.append(
                    {
                        **broker_identity,
                        **row,
                        "expiry": broker_expiry or row.get("expiry") or enriched.get("expiry"),
                        "contract_month": (broker_expiry or str(row.get("expiry") or ""))[:6]
                        or row.get("contract_month")
                        or enriched.get("contract_month"),
                        "contract_identity_source": "broker_positions_snapshot_plus_trade_ledger_identity",
                    }
                )
    else:
        candidates.extend(
            _select_unanchored_identity_candidates(
                ledger_identities,
                selected_contract_month=str(enriched.get("contract_month") or "").strip(),
            )
        )
    candidates = _dedupe_contract_identities(candidates)
    if len(candidates) != 1:
        return enriched
    return {**enriched, **candidates[0]}


def _broker_position_contract_identity(
    *,
    instrument: str,
    broker_positions_snapshot: Mapping[str, Any],
) -> dict[str, Any]:
    candidates: list[dict[str, Any]] = []
    for row in _broker_positions(broker_positions_snapshot):
        if not _is_track_b_futures_position(row):
            continue
        if not _position_matches_instrument(row, instrument):
            continue
        con_id = _int_or_none(row.get("con_id") or row.get("conId"))
        local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip()
        expiry = str(row.get("expiry") or row.get("lastTradeDateOrContractMonth") or "").strip()
        if not local_symbol:
            continue
        candidates.append(
            {
                "symbol": instrument,
                "contract_month": expiry[:6] if len(expiry) >= 6 else None,
                "expiry": expiry or None,
                "con_id": con_id,
                "local_symbol": local_symbol,
                "exchange": row.get("exchange") or "CME",
                "currency": row.get("currency") or "USD",
                "multiplier": row.get("multiplier"),
                "trading_class": row.get("trading_class") or row.get("tradingClass") or instrument,
                "contract_identity_source": "broker_positions_snapshot",
            }
        )
    if len(candidates) != 1:
        return {}
    return candidates[0]


def _trade_ledger_contract_identities(*, root: Path, instrument: str) -> list[dict[str, Any]]:
    paths = (
        root
        / "outputs"
        / "track_b_execution_core"
        / "paper_trade_ledger"
        / "latest_track_b_broker_reconciled_live_position_status.json",
        root
        / "outputs"
        / "track_b_execution_core"
        / "paper_trade_ledger"
        / "latest_track_b_live_position_status.json",
        root
        / "outputs"
        / "track_b_execution_core"
        / "paper_trade_ledger"
        / "latest_track_b_broker_reconciled_paper_trade_summary.json",
    )
    candidates: list[dict[str, Any]] = []
    for path in paths:
        payload = _load_json(path)
        if not payload:
            continue
        for row in _iter_contract_identity_rows(payload):
            candidate = _contract_identity_candidate(row, instrument=instrument, source=str(path))
            if candidate:
                candidates.append(candidate)
    return _dedupe_contract_identities(candidates)


def _iter_contract_identity_rows(payload: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    rows: list[Mapping[str, Any]] = [payload]
    for key in (
        "positions_by_instrument",
        "positions_by_strategy",
        "broker_positions_by_instrument",
    ):
        value = payload.get(key)
        if isinstance(value, Mapping):
            rows.extend(dict(row) for row in value.values() if isinstance(row, Mapping))
    for key in (
        "recent_trades",
        "lifecycle_units",
        "broker_track_b_positions",
        "positions",
        "current_positions",
        "terminal_superseded_open_records",
        "terminal_suppressed_open_records",
    ):
        value = payload.get(key)
        if isinstance(value, list):
            rows.extend(dict(row) for row in value if isinstance(row, Mapping))
    expanded: list[Mapping[str, Any]] = []
    for row in rows:
        expanded.append(row)
        for nested_key in (
            "entry_broker_identity",
            "broker_identity",
            "contract",
            "contract_identity",
            "resolved_contract",
        ):
            nested = row.get(nested_key)
            if isinstance(nested, Mapping):
                expanded.append(nested)
        nested_row = row.get("row")
        if isinstance(nested_row, Mapping):
            expanded.append(nested_row)
        units = row.get("lifecycle_units")
        if isinstance(units, list):
            expanded.extend(dict(unit) for unit in units if isinstance(unit, Mapping))
    return expanded


def _contract_identity_candidate(row: Mapping[str, Any], *, instrument: str, source: str) -> dict[str, Any]:
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip()
    con_id = _int_or_none(row.get("con_id") or row.get("conId"))
    if con_id is None or con_id <= 0 or not local_symbol:
        return {}
    symbol = str(
        row.get("symbol")
        or row.get("instrument")
        or row.get("instrument_family")
        or row.get("track_b_root")
        or ""
    ).strip().upper()
    if not symbol:
        symbol = track_b_root_from_local_symbol(local_symbol)
    if symbol != instrument:
        return {}
    expiry = str(row.get("expiry") or row.get("lastTradeDateOrContractMonth") or "").strip()
    contract_month = str(row.get("contract_month") or row.get("contractMonth") or "").strip()
    if not contract_month and len(expiry) >= 6:
        contract_month = expiry[:6]
    if not contract_month:
        contract_month = _contract_month_from_local_symbol(local_symbol)
    return {
        "symbol": instrument,
        "contract_month": contract_month or None,
        "expiry": expiry or None,
        "con_id": con_id,
        "local_symbol": local_symbol,
        "exchange": row.get("exchange") or "CME",
        "currency": row.get("currency") or "USD",
        "multiplier": row.get("multiplier"),
        "trading_class": row.get("trading_class") or row.get("tradingClass") or instrument,
        "contract_identity_source": source,
    }


def _select_unanchored_identity_candidates(
    candidates: Sequence[Mapping[str, Any]],
    *,
    selected_contract_month: str,
) -> list[dict[str, Any]]:
    """Prefer a single next/current identity when no broker row anchors localSymbol.

    This is still identity-only: exposure and order truth continue to come from
    broker snapshots. The active profile can carry a stale front-month target
    while the bridge resolver has already been using the next quarterly
    contract. When historical identity artifacts contain both, choose the
    nearest later contract month. If that cannot be determined uniquely, fail
    closed by returning all candidates so the caller treats it as ambiguous.
    """

    deduped = _dedupe_contract_identities(candidates)
    if len(deduped) <= 1:
        return deduped
    selected_month = _int_or_none(selected_contract_month)
    if selected_month is None:
        return deduped
    later = [
        row
        for row in deduped
        if (_int_or_none(row.get("contract_month")) is not None and _int_or_none(row.get("contract_month")) > selected_month)
    ]
    if not later:
        return deduped
    earliest_month = min(int(row.get("contract_month")) for row in later if _int_or_none(row.get("contract_month")) is not None)
    selected = [row for row in later if _int_or_none(row.get("contract_month")) == earliest_month]
    return selected if len(selected) == 1 else deduped


def _dedupe_contract_identities(candidates: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    deduped: dict[tuple[int, str], dict[str, Any]] = {}
    for row in candidates:
        con_id = _int_or_none(row.get("con_id"))
        local_symbol = str(row.get("local_symbol") or "").strip().upper()
        if con_id is None or con_id <= 0 or not local_symbol:
            continue
        normalized = {key: value for key, value in dict(row).items() if value not in (None, "")}
        deduped[(con_id, local_symbol)] = normalized
    return list(deduped.values())


def _contract_month_from_local_symbol(local_symbol: str) -> str:
    text = str(local_symbol or "").strip().upper()
    if len(text) < 3:
        return ""
    month_by_code = {
        "F": "01",
        "G": "02",
        "H": "03",
        "J": "04",
        "K": "05",
        "M": "06",
        "N": "07",
        "Q": "08",
        "U": "09",
        "V": "10",
        "X": "11",
        "Z": "12",
    }
    code = text[-2:-1]
    year_code = text[-1:]
    month = month_by_code.get(code)
    if not month or not year_code.isdigit():
        return ""
    return f"202{year_code}{month}"


def _broker_positions(snapshot: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [dict(row) for row in list(snapshot.get("positions") or []) if isinstance(row, Mapping)]


def _is_track_b_futures_position(row: Mapping[str, Any]) -> bool:
    sec_type = str(row.get("security_type") or row.get("secType") or "").strip().upper()
    symbol = str(row.get("symbol") or row.get("track_b_root") or "").strip().upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    if sec_type and sec_type != "FUT":
        return False
    root = symbol or track_b_root_from_local_symbol(local_symbol)
    return root in TRACK_B_FUTURES_SYMBOLS


def _position_matches_instrument(row: Mapping[str, Any], instrument: str) -> bool:
    expected = str(instrument or "").strip().upper()
    if not expected:
        return False
    symbol = str(row.get("symbol") or row.get("track_b_root") or row.get("instrument") or row.get("instrument_family") or "").strip().upper()
    if symbol == expected:
        return True
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").strip().upper()
    return bool(local_symbol) and local_symbol.startswith(expected)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
