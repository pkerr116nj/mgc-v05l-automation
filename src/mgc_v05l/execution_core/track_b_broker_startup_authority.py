"""Fresh broker-truth authority rules for Track B PAPER startup/reload."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from typing import Any, Mapping, Sequence

from mgc_v05l.execution_core.track_b_current_state_authority import (
    TRACK_B_FUTURES_ROOTS,
    broker_open_orders,
    broker_positions,
    current_state_same_contract,
    is_track_b_futures_position,
    is_track_b_order,
    normalize_current_broker_position,
    open_order_truth_unknown_count,
)

FRESH_COMPLETE_CLEAN_BROKER_TRUTH = "FRESH_COMPLETE_CLEAN_BROKER_TRUTH"
FRESH_COMPLETE_MANAGED_BROKER_TRUTH = "FRESH_COMPLETE_MANAGED_BROKER_TRUTH"
BROKER_TRUTH_NOT_STARTUP_CLEAN = "BROKER_TRUTH_NOT_STARTUP_CLEAN"


@dataclass(frozen=True)
class BrokerStartupAuthority:
    classification: str
    broker_truth_clean: bool
    blockers: tuple[str, ...]
    diagnostics: tuple[str, ...]
    track_b_futures_positions: tuple[Mapping[str, Any], ...]
    broker_open_order_count: int
    unknown_open_order_count: int
    known_managed_position_count: int = 0
    unrelated_open_order_count: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "broker_truth_clean": self.broker_truth_clean,
            "blockers": list(self.blockers),
            "diagnostics": list(self.diagnostics),
            "track_b_futures_positions": [dict(row) for row in self.track_b_futures_positions],
            "broker_open_order_count": self.broker_open_order_count,
            "unknown_open_order_count": self.unknown_open_order_count,
            "known_managed_position_count": self.known_managed_position_count,
            "unrelated_open_order_count": self.unrelated_open_order_count,
        }


def classify_fresh_complete_clean_broker_truth(
    *,
    broker_truth_status: Mapping[str, Any] | None = None,
    positions_snapshot: Mapping[str, Any] | None = None,
    open_orders_snapshot: Mapping[str, Any] | None = None,
    reconciliation: Mapping[str, Any] | None = None,
    open_order_truth: Mapping[str, Any] | None = None,
    status: Mapping[str, Any] | None = None,
    safety: Mapping[str, Any] | None = None,
    managed_positions: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None = None,
    allow_known_managed_positions: bool = False,
    expected_account_id: str | None = None,
    track_b_roots: Sequence[str] = tuple(sorted(TRACK_B_FUTURES_ROOTS)),
) -> BrokerStartupAuthority:
    broker_truth_status = _mapping(broker_truth_status)
    positions_snapshot = _mapping(positions_snapshot)
    open_orders_snapshot = _mapping(open_orders_snapshot)
    reconciliation = _mapping(reconciliation)
    open_order_truth = _mapping(open_order_truth)
    status = _mapping(status)
    safety = _mapping(safety) or _mapping(status.get("safety"))
    roots = frozenset(str(root).upper() for root in track_b_roots)

    blockers: list[str] = []
    diagnostics: list[str] = []

    if _any_true(safety, broker_truth_status, reconciliation, open_order_truth, key="live_money_eligible"):
        blockers.append("live_money_eligible")
    if _any_true(safety, broker_truth_status, reconciliation, open_order_truth, key="paper_proof_invoked"):
        blockers.append("paper_proof_invoked")
    if expected_account_id:
        account_ids = _account_ids(broker_truth_status, positions_snapshot, open_orders_snapshot)
        if expected_account_id not in account_ids:
            blockers.append("broker_account_not_confirmed")
        if len(account_ids) > 1:
            blockers.append("broker_account_identity_ambiguity")

    if broker_truth_status.get("fresh") is not True:
        blockers.append("broker_truth_not_fresh")
    if broker_truth_status.get("positions_complete") is not True:
        blockers.append("broker_positions_incomplete")
    if broker_truth_status.get("open_orders_complete") is not True:
        blockers.append("broker_open_orders_incomplete")

    positions = tuple(broker_positions(positions_snapshot) or _position_rows(broker_truth_status))
    track_b_positions = tuple(row for row in positions if _is_track_b_future(row, roots) and _quantity(row) != 0.0)
    open_orders = tuple(broker_open_orders(open_orders_snapshot) or _open_order_rows(broker_truth_status))
    track_b_open_orders = tuple(row for row in open_orders if _is_track_b_order(row, roots))
    unrelated_open_orders = tuple(row for row in open_orders if not _is_track_b_order(row, roots))
    same_contract_conflicts = _same_contract_order_conflicts(
        broker_positions=track_b_positions,
        open_orders=track_b_open_orders,
    )
    known_managed_position_count = 0
    if track_b_positions:
        if allow_known_managed_positions:
            managed_classification = _classify_known_managed_positions(
                broker_positions=track_b_positions,
                managed_positions=managed_positions,
                expected_account_id=expected_account_id,
            )
            known_managed_position_count = int(managed_classification["known_count"])
            blockers.extend(managed_classification["blockers"])
            diagnostics.extend(managed_classification["diagnostics"])
        else:
            blockers.append("track_b_futures_positions_present")

    broker_open_order_count = _open_order_count(
        broker_truth_status,
        open_orders_snapshot,
        open_order_truth,
        roots=roots,
    )
    unrelated_open_order_count = len(unrelated_open_orders)
    if unrelated_open_order_count:
        diagnostics.append(f"unrelated_non_track_b_open_orders:{unrelated_open_order_count}")
    if same_contract_conflicts:
        blockers.append("conflicting_same_contract_futures_open_order")
    elif broker_open_order_count != 0:
        blockers.append("track_b_futures_open_orders_present")

    unknown_open_order_count = max(
        _unknown_open_order_count(broker_truth_status, open_orders_snapshot),
        open_order_truth_unknown_count(open_order_truth),
    )
    if unknown_open_order_count != 0:
        blockers.append("unknown_open_orders_present")

    if _has_identity_ambiguity(broker_truth_status, positions_snapshot, open_orders_snapshot):
        blockers.append("broker_identity_ambiguity")

    lease = _mapping(broker_truth_status.get("broker_truth_lease_refresh"))
    if lease.get("lease_state") and lease.get("lease_state") != "ACTIVE":
        diagnostics.append(f"broker_truth_lease_state:{lease.get('lease_state')}")
    for blocker in lease.get("blockers") or ():
        diagnostics.append(f"broker_truth_lease_blocker:{blocker}")
    recon_class = str(reconciliation.get("classification") or reconciliation.get("reconciliation_classification") or "")
    if recon_class and "RECONCILED" not in recon_class:
        diagnostics.append(f"reconciliation_classification:{recon_class}")
    order_class = str(open_order_truth.get("classification") or open_order_truth.get("order_truth_classification") or "")
    if order_class and order_class not in {"NO_OPEN_ORDERS", "BROKER_OPEN_ORDER_TRUTH_CLEAN"}:
        diagnostics.append(f"open_order_truth_classification:{order_class}")

    if blockers:
        classification = BROKER_TRUTH_NOT_STARTUP_CLEAN
    elif track_b_positions and allow_known_managed_positions:
        classification = FRESH_COMPLETE_MANAGED_BROKER_TRUTH
    else:
        classification = FRESH_COMPLETE_CLEAN_BROKER_TRUTH
    return BrokerStartupAuthority(
        classification=classification,
        broker_truth_clean=not blockers,
        blockers=tuple(dict.fromkeys(blockers)),
        diagnostics=tuple(dict.fromkeys(diagnostics)),
        track_b_futures_positions=track_b_positions,
        broker_open_order_count=broker_open_order_count,
        unknown_open_order_count=unknown_open_order_count,
        known_managed_position_count=known_managed_position_count,
        unrelated_open_order_count=unrelated_open_order_count,
    )


def derived_mismatch_is_diagnostic(authority: BrokerStartupAuthority | Mapping[str, Any]) -> bool:
    clean = authority.broker_truth_clean if isinstance(authority, BrokerStartupAuthority) else authority.get("broker_truth_clean")
    return clean is True


def _mapping(value: Any) -> Mapping[str, Any]:
    return value if isinstance(value, Mapping) else {}


def _any_true(*payloads: Mapping[str, Any], key: str) -> bool:
    return any(_mapping(payload).get(key) is True for payload in payloads)


def _position_rows(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    rows = payload.get("positions") or payload.get("track_b_positions") or payload.get("futures_positions") or ()
    return tuple(row for row in rows if isinstance(row, Mapping))


def _open_order_rows(payload: Mapping[str, Any]) -> tuple[Mapping[str, Any], ...]:
    rows = payload.get("open_orders") or payload.get("track_b_open_orders") or ()
    return tuple(row for row in rows if isinstance(row, Mapping))


def _is_track_b_future(row: Mapping[str, Any], roots: frozenset[str]) -> bool:
    _ = roots
    return is_track_b_futures_position(row)


def _is_track_b_order(row: Mapping[str, Any], roots: frozenset[str]) -> bool:
    _ = roots
    return is_track_b_order(row)


def _quantity(row: Mapping[str, Any]) -> float:
    try:
        return float(row.get("quantity") if row.get("quantity") is not None else row.get("position") or 0)
    except (TypeError, ValueError):
        return 1.0


def _classify_known_managed_positions(
    *,
    broker_positions: Sequence[Mapping[str, Any]],
    managed_positions: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None,
    expected_account_id: str | None,
) -> dict[str, Any]:
    blockers: list[str] = []
    diagnostics: list[str] = []
    managed_rows = _managed_position_rows(managed_positions)
    if not managed_rows:
        return {
            "known_count": 0,
            "blockers": ["track_b_futures_positions_unmanaged_or_ambiguous"],
            "diagnostics": [],
        }

    known_count = 0
    for broker_position in broker_positions:
        matches = [
            row
            for row in managed_rows
            if _managed_position_matches_broker_position(
                managed_position=row,
                broker_position=broker_position,
                expected_account_id=expected_account_id,
            )
        ]
        if len(matches) == 1:
            known_count += 1
            continue
        if len(matches) > 1:
            blockers.append("track_b_futures_position_managed_identity_ambiguous")
        else:
            blockers.append("track_b_futures_positions_unmanaged_or_ambiguous")
    if known_count:
        diagnostics.append(f"track_b_futures_positions_known_managed:{known_count}")
    return {
        "known_count": known_count,
        "blockers": blockers,
        "diagnostics": diagnostics,
    }


def _managed_position_rows(payload: Mapping[str, Any] | Sequence[Mapping[str, Any]] | None) -> tuple[Mapping[str, Any], ...]:
    if isinstance(payload, Mapping):
        rows = payload.get("managed_positions") or payload.get("positions") or ()
    elif isinstance(payload, (list, tuple)):
        rows = payload
    else:
        rows = ()
    return tuple(row for row in rows if isinstance(row, Mapping))


def _managed_position_matches_broker_position(
    *,
    managed_position: Mapping[str, Any],
    broker_position: Mapping[str, Any],
    expected_account_id: str | None,
) -> bool:
    broker_symbol = _text(broker_position.get("symbol") or broker_position.get("track_b_root") or broker_position.get("instrument"))
    broker_local_symbol = _text(broker_position.get("local_symbol") or broker_position.get("localSymbol"))
    broker_con_id = _int(broker_position.get("con_id") or broker_position.get("conId"), default=0)
    normalized_broker = normalize_current_broker_position(
        broker_position,
        account_id=expected_account_id or _text(broker_position.get("account_id") or broker_position.get("account")),
        instrument=broker_symbol,
        local_symbol=broker_local_symbol,
        con_id=broker_con_id,
    ) or dict(broker_position)
    broker_account = _text(normalized_broker.get("account_id") or normalized_broker.get("account"))
    managed_broker = _mapping(managed_position.get("broker_position"))
    lifecycle = _mapping(managed_position.get("lifecycle_position"))
    managed_account = _text(
        managed_position.get("account_id")
        or managed_broker.get("account_id")
        or lifecycle.get("account_id")
        or managed_position.get("account")
        or managed_broker.get("account")
        or lifecycle.get("account")
    )
    if expected_account_id and broker_account != expected_account_id:
        return False
    if broker_account and managed_account and broker_account != managed_account:
        return False

    broker_con_id = _text(normalized_broker.get("con_id") or normalized_broker.get("conId"))
    managed_con_id = _text(managed_position.get("con_id") or managed_broker.get("con_id") or lifecycle.get("con_id"))
    broker_local_symbol = _text(normalized_broker.get("local_symbol") or normalized_broker.get("localSymbol"))
    managed_local_symbol = _text(
        managed_position.get("local_symbol")
        or managed_position.get("localSymbol")
        or managed_broker.get("local_symbol")
        or managed_broker.get("localSymbol")
        or lifecycle.get("local_symbol")
        or lifecycle.get("localSymbol")
    )
    if broker_con_id and managed_con_id:
        identity_matches = broker_con_id == managed_con_id
    else:
        identity_matches = bool(broker_local_symbol and managed_local_symbol and broker_local_symbol == managed_local_symbol)
    if not identity_matches:
        return False

    if managed_position.get("projection_authority_owner_confirmed") is False:
        return False
    lifecycle_id = _text(managed_position.get("lifecycle_id") or lifecycle.get("lifecycle_id"))
    trade_id = _text(managed_position.get("trade_id") or lifecycle.get("trade_id"))
    policy_id = _text(managed_position.get("managed_exit_policy_id") or lifecycle.get("managed_exit_policy_id"))
    if not lifecycle_id:
        return False
    if not trade_id:
        return False
    if not policy_id:
        return False
    if not _managed_position_is_current_open(managed_position) and not _managed_position_is_startup_adoptable(
        managed_position
    ):
        return False

    return _signed_quantity(managed_position) == _decimal_quantity(normalized_broker)


def _managed_position_is_current_open(row: Mapping[str, Any]) -> bool:
    classification = _text(row.get("classification"))
    return classification in {"OPEN_MANAGED", "OPEN_MANAGED_MATCHED", "OPEN_MANAGED_EXIT_DUE"} or row.get("exit_due") is True


def _managed_position_is_startup_adoptable(row: Mapping[str, Any]) -> bool:
    if row.get("projection_authority_owner_confirmed") is not True:
        return False
    classification = _text(row.get("classification"))
    return classification in {
        "BROKER_BACKED_ADOPTION_REQUIRED",
        "REVIEW_REQUIRED",
        "STALE_MANAGED_POSITION_EVIDENCE",
        "STRAY_POSITION_REVIEW_REQUIRED",
    }


def _signed_quantity(row: Mapping[str, Any]) -> Decimal:
    for key in ("signed_broker_qty", "signed_lifecycle_qty", "signed_quantity"):
        if row.get(key) not in {None, ""}:
            return _decimal(row.get(key))
    broker_position = _mapping(row.get("broker_position"))
    lifecycle = _mapping(row.get("lifecycle_position"))
    for payload in (broker_position, lifecycle, row):
        qty = _decimal(payload.get("quantity") if payload.get("quantity") is not None else payload.get("position"))
        if qty == 0:
            continue
        side = _text(payload.get("side")).upper()
        if side == "SHORT":
            return -abs(qty)
        if side == "LONG":
            return abs(qty)
        return qty
    return Decimal("0")


def _decimal_quantity(row: Mapping[str, Any]) -> Decimal:
    return _decimal(row.get("quantity") if row.get("quantity") is not None else row.get("position"))


def _decimal(value: Any) -> Decimal:
    try:
        return Decimal(str(value))
    except (InvalidOperation, ValueError):
        return Decimal("0")


def _text(value: Any) -> str:
    return str(value or "").strip().upper()


def _open_order_count(*payloads: Mapping[str, Any], roots: frozenset[str]) -> int:
    all_orders: list[Mapping[str, Any]] = []
    for payload in payloads:
        all_orders.extend(_open_order_rows(_mapping(payload)))
    if all_orders:
        return len([row for row in all_orders if _is_track_b_future(row, roots)])

    scoped_count = 0
    for payload in payloads:
        payload = _mapping(payload)
        for key in ("track_b_broker_open_order_count", "broker_track_b_open_order_count", "track_b_open_order_count"):
            if payload.get(key) not in {None, ""}:
                scoped_count = max(scoped_count, _int(payload.get(key), default=1))
    if scoped_count:
        return scoped_count
    if any(_mapping(payload).get(key) in {0, "0"} for payload in payloads for key in ("track_b_broker_open_order_count", "broker_track_b_open_order_count", "track_b_open_order_count")):
        return 0

    count = 0
    for payload in payloads:
        payload = _mapping(payload)
        for key in ("open_order_count", "broker_open_order_count"):
            if payload.get(key) not in {None, ""}:
                count = max(count, _int(payload.get(key), default=1))
    return count


def _same_contract_order_conflicts(
    *,
    broker_positions: Sequence[Mapping[str, Any]],
    open_orders: Sequence[Mapping[str, Any]],
) -> tuple[Mapping[str, Any], ...]:
    conflicts: list[Mapping[str, Any]] = []
    for order in open_orders:
        if any(_same_contract(position, order) for position in broker_positions):
            conflicts.append(order)
    return tuple(conflicts)


def _same_contract(left: Mapping[str, Any], right: Mapping[str, Any]) -> bool:
    return current_state_same_contract(
        right,
        account_id=_text(left.get("account_id") or left.get("account")),
        local_symbol=_text(left.get("local_symbol") or left.get("localSymbol")),
        con_id=_int(left.get("con_id") or left.get("conId")),
    )


def _unknown_open_order_count(*payloads: Mapping[str, Any]) -> int:
    count = 0
    for payload in payloads:
        payload = _mapping(payload)
        for key in ("unknown_open_order_count", "unknown_order_count", "unknown_broker_open_order_count"):
            if payload.get(key) not in {None, ""}:
                count = max(count, _int(payload.get(key), default=1))
        unknown = payload.get("unknown_open_orders") or payload.get("unknown_orders")
        if isinstance(unknown, list):
            count = max(count, len(unknown))
    return count


def _has_identity_ambiguity(*payloads: Mapping[str, Any]) -> bool:
    ambiguity_keys = (
        "account_identity_ambiguous",
        "contract_identity_ambiguous",
        "broker_identity_ambiguous",
        "identity_ambiguity",
    )
    ambiguity_markers = ("AMBIGUOUS", "UNKNOWN_ACCOUNT", "ACCOUNT_MISMATCH", "CONTRACT_MISMATCH")
    for payload in payloads:
        payload = _mapping(payload)
        if any(payload.get(key) is True for key in ambiguity_keys):
            return True
        text = " ".join(str(payload.get(key) or "") for key in ("classification", "detail", "reason"))
        if any(marker in text.upper() for marker in ambiguity_markers):
            return True
    return False


def _account_ids(*payloads: Mapping[str, Any]) -> set[str]:
    values: set[str] = set()
    for payload in payloads:
        payload = _mapping(payload)
        for key in ("selected_account_id", "account_id", "account"):
            value = str(payload.get(key) or "").strip()
            if value:
                values.add(value)
        for row in _position_rows(payload):
            value = str(row.get("account_id") or row.get("account") or "").strip()
            if value:
                values.add(value)
        for row in payload.get("open_orders") or ():
            if isinstance(row, Mapping):
                value = str(row.get("account_id") or row.get("account") or "").strip()
                if value:
                    values.add(value)
    return values


def _int(value: Any, *, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default
