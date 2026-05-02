"""Pure fail-closed risk gate for Track B milestone-one submits."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Mapping, Sequence

from .models import (
    Action,
    GateDecision,
    IntentKind,
    OrderIntent,
    PositionState,
    ReconciliationResult,
    ReconciliationStage,
    ReconciliationStatus,
)


FORBIDDEN_ORDER_FIELD_NAMES = {
    "algo",
    "algostrategy",
    "bracket",
    "child",
    "children",
    "oca",
    "ocagroup",
    "oco",
    "parent",
    "parentid",
}


def evaluate_order_gate(
    *,
    order_intent: OrderIntent,
    environment: Mapping[str, Any],
    configured_account_id: str,
    contract_allowlist: Mapping[str, Mapping[str, Any]],
    broker_position: PositionState | None,
    broker_open_orders: Sequence[str | Any] = (),
    reconciliation: ReconciliationResult | None,
    created_at: datetime | None = None,
) -> GateDecision:
    """Evaluate all milestone-one submit checks without side effects."""

    checks: list[dict[str, Any]] = []
    expected_account = str(configured_account_id or "").strip()
    mode = str(environment.get("mode") or "").strip().upper()
    host = str(environment.get("host") or "").strip()
    port = _int_or_none(environment.get("port"))
    allowlist_entry = contract_allowlist.get(order_intent.contract_key)

    _add_check(checks, "paper_mode", mode == "PAPER", f"mode must be PAPER, got {mode or '<missing>'}.")
    _add_check(checks, "paper_host", host == "127.0.0.1", f"host must be 127.0.0.1, got {host or '<missing>'}.")
    _add_check(checks, "paper_port", port == 7497, f"port must be 7497, got {port!r}.")
    _add_check(checks, "explicit_configured_account", bool(expected_account), "configured account_id is required.")
    _add_check(
        checks,
        "order_account_matches_config",
        bool(expected_account) and order_intent.account_id == expected_account,
        "order intent account_id must match configured paper account.",
    )
    _add_check(
        checks,
        "contract_allowlisted",
        allowlist_entry is not None,
        f"contract_key {order_intent.contract_key!r} must be explicitly allowlisted.",
    )
    _add_check(
        checks,
        "contract_symbol_matches_allowlist",
        allowlist_entry is not None and str(allowlist_entry.get("symbol") or "").strip().upper() == order_intent.symbol,
        "order intent symbol must match allowlist entry.",
    )
    _add_check(checks, "quantity_exactly_one", str(order_intent.quantity) == "1", "quantity must be exactly 1.")
    _add_check(checks, "order_type_lmt", order_intent.order_type == "LMT", "order_type must be LMT.")
    _add_check(checks, "time_in_force_day", order_intent.time_in_force == "DAY", "time_in_force must be DAY.")
    _add_check(checks, "market_orders_forbidden", order_intent.order_type != "MKT", "market orders are forbidden.")
    _add_check(
        checks,
        "complex_order_fields_absent",
        not _has_forbidden_order_fields(order_intent.extra_fields),
        "bracket/OCO/parent/child/algo fields are forbidden.",
    )

    if broker_position is None:
        _add_check(checks, "broker_position_present", False, "broker position truth is required.")
    else:
        _add_check(checks, "broker_position_present", True, "broker position truth is present.")
        _add_check(
            checks,
            "broker_account_matches_config",
            broker_position.account_id == expected_account,
            "broker position account must match configured account.",
        )
        _add_check(
            checks,
            "broker_contract_matches_intent",
            broker_position.contract_key == order_intent.contract_key,
            "broker position contract_key must match order intent.",
        )

    broker_order_ids = _open_order_ids(broker_open_orders)
    position_order_ids = tuple(broker_position.open_order_ids if broker_position is not None else ())
    all_open_order_ids = tuple(sorted(set(broker_order_ids) | set(position_order_ids)))
    _add_check(
        checks,
        "broker_open_orders_clean",
        not all_open_order_ids,
        f"broker open orders must be empty, got {list(all_open_order_ids)}.",
    )

    if order_intent.intent_kind == IntentKind.OPEN:
        _add_check(
            checks,
            "broker_flat_before_open",
            broker_position is not None and broker_position.signed_quantity == 0,
            "broker must be flat before an open intent.",
        )
        expected_stage = ReconciliationStage.PRE_OPEN
    else:
        expected_quantity = 1 if order_intent.action == Action.SELL else -1
        _add_check(
            checks,
            "broker_expected_one_lot_before_close",
            broker_position is not None and broker_position.signed_quantity == expected_quantity,
            "broker must hold the exact 1-lot position needed before close.",
        )
        expected_stage = ReconciliationStage.PRE_CLOSE

    if reconciliation is None:
        _add_check(checks, "reconciliation_present", False, "clean reconciliation is required before submit.")
    else:
        _add_check(checks, "reconciliation_present", True, "reconciliation is present.")
        _add_check(
            checks,
            "reconciliation_account_matches_config",
            reconciliation.account_id == expected_account,
            "reconciliation account must match configured account.",
        )
        _add_check(
            checks,
            "reconciliation_contract_matches_intent",
            reconciliation.contract_key == order_intent.contract_key,
            "reconciliation contract must match order intent.",
        )
        _add_check(
            checks,
            "reconciliation_stage_expected",
            reconciliation.stage == expected_stage,
            f"reconciliation stage must be {expected_stage.value}.",
        )
        _add_check(
            checks,
            "reconciliation_clean",
            reconciliation.status == ReconciliationStatus.CLEAN,
            "reconciliation must be CLEAN before submit.",
        )

    blocking_reason = _first_blocking_reason(checks)
    passed = blocking_reason is None
    now = created_at or datetime.now(timezone.utc)
    return GateDecision(
        gate_decision_id=f"gate_{order_intent.order_intent_id}",
        run_id=order_intent.run_id,
        passed=passed,
        blocking_reason=blocking_reason,
        checks=tuple(checks),
        event_payload={
            "order_intent_id": order_intent.order_intent_id,
            "intent_kind": order_intent.intent_kind.value,
            "account_id": order_intent.account_id,
            "contract_key": order_intent.contract_key,
            "passed": passed,
            "blocking_reason": blocking_reason,
        },
        created_at=now,
    )


def _add_check(checks: list[dict[str, Any]], name: str, passed: bool, detail: str, *, blocking: bool = True) -> None:
    checks.append({"name": name, "passed": bool(passed), "blocking": bool(blocking), "detail": detail})


def _first_blocking_reason(checks: Sequence[Mapping[str, Any]]) -> str | None:
    for check in checks:
        if bool(check.get("blocking")) and not bool(check.get("passed")):
            return str(check.get("detail") or check.get("name") or "risk gate rejected")
    return None


def _has_forbidden_order_fields(extra_fields: Mapping[str, Any]) -> bool:
    for key in extra_fields:
        normalized = str(key).replace("_", "").replace("-", "").strip().lower()
        if normalized in FORBIDDEN_ORDER_FIELD_NAMES:
            return True
    return False


def _open_order_ids(open_orders: Sequence[str | Any]) -> tuple[str, ...]:
    rows: list[str] = []
    for row in open_orders:
        if isinstance(row, str):
            value = row
        else:
            value = str(getattr(row, "broker_order_id", "") or "")
        normalized = value.strip()
        if normalized:
            rows.append(normalized)
    return tuple(rows)


def _int_or_none(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None
