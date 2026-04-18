"""IBKR Milestone A acceptance helpers for broker-truth proof without trading."""

from __future__ import annotations

from typing import Any


def evaluate_ibkr_milestone_a_snapshot(snapshot: dict[str, Any]) -> dict[str, Any]:
    payload = dict(snapshot or {})
    selected_account_id = str(payload.get("selected_account_id") or "").strip() or None
    accounts = list(payload.get("accounts") or [])
    balances = list(payload.get("balances") or [])
    positions = list(payload.get("positions") or [])
    open_orders = list(payload.get("open_orders") or [])
    completed_orders = list(payload.get("completed_orders") or [])
    executions = list(payload.get("executions") or [])
    health = dict(payload.get("health") or {})
    metadata = dict(payload.get("metadata") or {})
    visibility_scope = dict(metadata.get("visibility_scope") or {})
    scoped_account_ids = {
        str(account_id).strip()
        for account_id in [selected_account_id, *(visibility_scope.get("managed_accounts") or [])]
        if str(account_id).strip()
    }

    checks = {
        "provider_is_ibkr": _check(
            ok=str(payload.get("provider_id") or "").strip() == "ibkr_execution",
            summary="Provider id is ibkr_execution.",
            detail=payload.get("provider_id"),
        ),
        "health_connected": _check(
            ok=bool(health.get("connected")),
            summary="Health reports an active IBKR connection.",
            detail=health,
        ),
        "selected_account_present": _check(
            ok=bool(selected_account_id),
            summary="A selected IBKR account id is present.",
            detail=selected_account_id,
        ),
        "account_truth_present": _check(
            ok=bool(selected_account_id and any(str(row.get("account_id") or "").strip() == selected_account_id for row in accounts)),
            summary="Selected account is present in normalized account truth.",
            detail=accounts,
        ),
        "balances_present_for_scope": _check(
            ok=bool(balances) and _all_rows_within_scope(balances, scoped_account_ids),
            summary="Balances are present and scoped to the selected account visibility set.",
            detail=balances,
        ),
        "positions_shape_present": _check(
            ok=_rows_have_required_keys(positions, ("account_id", "symbol", "quantity", "side")),
            summary="Position truth rows have the expected normalized keys.",
            detail=positions,
        ),
        "open_orders_shape_present": _check(
            ok=_rows_have_required_keys(open_orders, ("account_id", "broker_order_id", "symbol", "status")),
            summary="Open order truth rows have the expected normalized keys.",
            detail=open_orders,
        ),
        "completed_orders_shape_present": _check(
            ok=_rows_have_required_keys(completed_orders, ("account_id", "broker_order_id", "symbol", "status")),
            summary="Completed-order truth rows have the expected normalized keys for the current visibility scope.",
            detail=completed_orders,
        ),
        "executions_shape_present": _check(
            ok=_rows_have_required_keys(executions, ("account_id", "symbol", "quantity")),
            summary="Execution truth rows have the expected normalized keys for the current visibility scope.",
            detail=executions,
        ),
        "freshness_present": _check(
            ok=bool(payload.get("generated_at")) and bool(health.get("checked_at")),
            summary="Snapshot and health freshness timestamps are populated.",
            detail={"generated_at": payload.get("generated_at"), "checked_at": health.get("checked_at")},
        ),
        "reconciliation_ingestion_ready": _check(
            ok=_reconciliation_compatibility_ready(payload),
            summary="Legacy reconciliation compatibility keys are present for the current runtime bridge.",
            detail={
                "connected": payload.get("connected"),
                "truth_complete": payload.get("truth_complete"),
                "position_quantity": payload.get("position_quantity"),
                "open_order_ids": payload.get("open_order_ids"),
                "order_status": payload.get("order_status"),
                "last_fill_timestamp": payload.get("last_fill_timestamp"),
            },
        ),
    }
    all_ok = all(item["ok"] for item in checks.values())
    return {
        "milestone": "IBKR_BROKER_TRUTH_PROOF",
        "ready": all_ok,
        "summary": "IBKR broker truth is ready for Milestone A proof." if all_ok else "IBKR broker truth is not yet ready for Milestone A proof.",
        "selected_account_id": selected_account_id,
        "visibility_scope": visibility_scope,
        "checks": checks,
    }


def _check(*, ok: bool, summary: str, detail: Any) -> dict[str, Any]:
    return {"ok": bool(ok), "summary": summary, "detail": detail}


def _rows_have_required_keys(rows: list[dict[str, Any]], required_keys: tuple[str, ...]) -> bool:
    if not rows:
        return True
    return all(all(key in row for key in required_keys) for row in rows)


def _all_rows_within_scope(rows: list[dict[str, Any]], account_ids: set[str]) -> bool:
    if not rows:
        return False
    if not account_ids:
        return False
    return all(str(row.get("account_id") or "").strip() in account_ids for row in rows)


def _reconciliation_compatibility_ready(payload: dict[str, Any]) -> bool:
    return (
        "connected" in payload
        and "truth_complete" in payload
        and "position_quantity" in payload
        and "open_order_ids" in payload
        and "order_status" in payload
        and "last_fill_timestamp" in payload
    )

