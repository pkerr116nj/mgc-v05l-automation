"""Fresh broker-truth authority rules for Track B PAPER startup/reload."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


TRACK_B_FUTURES_ROOTS = frozenset({"MES", "MNQ", "MGC", "GC", "NQ", "ES"})
FRESH_COMPLETE_CLEAN_BROKER_TRUTH = "FRESH_COMPLETE_CLEAN_BROKER_TRUTH"
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

    def to_dict(self) -> dict[str, Any]:
        return {
            "classification": self.classification,
            "broker_truth_clean": self.broker_truth_clean,
            "blockers": list(self.blockers),
            "diagnostics": list(self.diagnostics),
            "track_b_futures_positions": [dict(row) for row in self.track_b_futures_positions],
            "broker_open_order_count": self.broker_open_order_count,
            "unknown_open_order_count": self.unknown_open_order_count,
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

    positions = _position_rows(positions_snapshot) or _position_rows(broker_truth_status)
    track_b_positions = tuple(row for row in positions if _is_track_b_future(row, roots) and _quantity(row) != 0.0)
    if track_b_positions:
        blockers.append("track_b_futures_positions_present")

    broker_open_order_count = _open_order_count(broker_truth_status, open_orders_snapshot, open_order_truth, reconciliation)
    if broker_open_order_count != 0:
        blockers.append("broker_open_orders_present")

    unknown_open_order_count = _unknown_open_order_count(broker_truth_status, open_orders_snapshot, open_order_truth, reconciliation)
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

    classification = BROKER_TRUTH_NOT_STARTUP_CLEAN if blockers else FRESH_COMPLETE_CLEAN_BROKER_TRUTH
    return BrokerStartupAuthority(
        classification=classification,
        broker_truth_clean=not blockers,
        blockers=tuple(dict.fromkeys(blockers)),
        diagnostics=tuple(dict.fromkeys(diagnostics)),
        track_b_futures_positions=track_b_positions,
        broker_open_order_count=broker_open_order_count,
        unknown_open_order_count=unknown_open_order_count,
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


def _is_track_b_future(row: Mapping[str, Any], roots: frozenset[str]) -> bool:
    sec_type = str(row.get("security_type") or row.get("secType") or "FUT").upper()
    if sec_type and sec_type != "FUT":
        return False
    symbol = str(row.get("symbol") or "").upper()
    local_symbol = str(row.get("local_symbol") or row.get("localSymbol") or "").upper()
    return symbol in roots or any(local_symbol.startswith(root) for root in roots)


def _quantity(row: Mapping[str, Any]) -> float:
    try:
        return float(row.get("quantity") if row.get("quantity") is not None else row.get("position") or 0)
    except (TypeError, ValueError):
        return 1.0


def _open_order_count(*payloads: Mapping[str, Any]) -> int:
    count = 0
    for payload in payloads:
        payload = _mapping(payload)
        for key in ("open_order_count", "broker_open_order_count", "track_b_broker_open_order_count"):
            if payload.get(key) not in {None, ""}:
                count = max(count, _int(payload.get(key), default=1))
        orders = payload.get("open_orders") or payload.get("track_b_open_orders")
        if isinstance(orders, list):
            count = max(count, len(orders))
    return count


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
