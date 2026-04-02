"""Broker-truth schema definitions and read-only validation helpers."""

from __future__ import annotations

from copy import deepcopy
from typing import Any

SUFFICIENT_BROKER_TRUTH = "sufficient_broker_truth"
PARTIAL_USABLE_TRUTH = "partial_but_usable_truth"
INSUFFICIENT_TRUTH_RECONCILE = "insufficient_truth_reconcile"
CONFLICTING_TRUTH_FAULT_REVIEW = "conflicting_truth_fault_review"

ORDER_STATUS_REQUIRED_FIELDS = ("broker_order_id", "status")
ORDER_STATUS_OPTIONAL_FIELDS = (
    "client_order_id",
    "symbol",
    "description",
    "asset_class",
    "instruction",
    "quantity",
    "filled_quantity",
    "order_type",
    "duration",
    "session",
    "entered_at",
    "closed_at",
    "updated_at",
    "limit_price",
    "stop_price",
    "source",
    "raw_payload",
)
OPEN_ORDER_REQUIRED_FIELDS = ("broker_order_id", "symbol", "status", "instruction", "quantity")
OPEN_ORDER_OPTIONAL_FIELDS = (
    "client_order_id",
    "description",
    "asset_class",
    "filled_quantity",
    "order_type",
    "duration",
    "session",
    "entered_at",
    "closed_at",
    "updated_at",
    "limit_price",
    "stop_price",
    "source",
    "raw_payload",
)
POSITION_REQUIRED_FIELDS = ("symbol", "side", "quantity")
POSITION_OPTIONAL_FIELDS = (
    "position_key",
    "description",
    "asset_class",
    "average_cost",
    "mark_price",
    "market_value",
    "current_day_pnl",
    "open_pnl",
    "ytd_pnl",
    "margin_impact",
    "broker_position_id",
    "fetched_at",
    "quote_fetched_at",
    "quote_state",
    "raw_payload",
)
ACCOUNT_HEALTH_REQUIRED_FIELDS = ("status", "broker_reachable", "auth_ready", "account_selected")
ACCOUNT_HEALTH_OPTIONAL_FIELDS = (
    "selected_account_hash",
    "broker_reachable_label",
    "auth_label",
    "account_selected_label",
    "balances_fresh_state",
    "positions_fresh_state",
    "orders_fresh_state",
    "fills_events_fresh_state",
    "reconciliation_fresh_label",
    "detail",
)

BROKER_TRUTH_SCHEMA_DEFINITIONS: dict[str, dict[str, Any]] = {
    "order_status": {
        "required_fields": list(ORDER_STATUS_REQUIRED_FIELDS),
        "optional_fields": list(ORDER_STATUS_OPTIONAL_FIELDS),
        "normalization_rules": [
            "broker_order_id comes from normalized orderId/orderID and must match the requested order id when provided.",
            "status is upper-trimmed from the broker payload and preserved exactly after normalization.",
            "symbol, instruction, and quantity prefer the first order leg when present.",
            "missing optional fields do not block read-only truth if required fields are present.",
        ],
        "sufficient_truth_definition": "broker_order_id and status are present and non-empty.",
        "insufficient_truth_definition": "required fields are missing or the payload cannot be normalized.",
        "conflicting_truth_definition": "normalized broker_order_id disagrees with the requested order id.",
    },
    "open_orders": {
        "required_fields": list(OPEN_ORDER_REQUIRED_FIELDS),
        "optional_fields": list(OPEN_ORDER_OPTIONAL_FIELDS),
        "normalization_rules": [
            "each retained row must normalize broker_order_id, symbol, status, instruction, and quantity.",
            "symbol is upper-trimmed; status is upper-trimmed; quantity is preserved as normalized decimal text.",
            "an empty normalized list is still sufficient broker truth when the transport itself succeeds.",
        ],
        "sufficient_truth_definition": "the payload is a list and every retained row has the required normalized fields.",
        "insufficient_truth_definition": "the payload is absent, not a list, or retained rows are missing required normalized fields.",
        "conflicting_truth_definition": "duplicate broker_order_id rows normalize to conflicting core fields.",
    },
    "position": {
        "required_fields": list(POSITION_REQUIRED_FIELDS),
        "optional_fields": list(POSITION_OPTIONAL_FIELDS),
        "normalization_rules": [
            "symbol is upper-trimmed from instrument.symbol/symbol.",
            "side is derived from longQuantity/shortQuantity or signed quantity fallback.",
            "quantity is normalized to absolute decimal size for the reported side.",
            "absence of a target-symbol row is sufficient truth for flat/no-position interpretation.",
        ],
        "sufficient_truth_definition": "all retained rows have symbol, side, and quantity, or the target symbol is cleanly absent.",
        "insufficient_truth_definition": "retained rows for the target symbol are missing required normalized fields.",
        "conflicting_truth_definition": "duplicate target-symbol rows disagree on side or quantity.",
    },
    "account_health": {
        "required_fields": list(ACCOUNT_HEALTH_REQUIRED_FIELDS),
        "optional_fields": list(ACCOUNT_HEALTH_OPTIONAL_FIELDS),
        "normalization_rules": [
            "Schwab does not expose a dedicated account-health payload here, so health is derived from auth, connectivity, selected-account, and freshness truth.",
            "broker_reachable/auth_ready/account_selected are required booleans for usable runtime health.",
            "freshness fields are advisory and may degrade the classification without invalidating the base schema.",
        ],
        "sufficient_truth_definition": "status, broker_reachable, auth_ready, and account_selected are present and non-empty.",
        "insufficient_truth_definition": "derived health omits one or more required fields.",
        "conflicting_truth_definition": "not used; conflicting health currently degrades to insufficient truth.",
    },
}


def validate_order_status_sample(
    *,
    raw_payload: dict[str, Any] | None,
    normalized_payload: dict[str, Any] | None,
    requested_broker_order_id: str | None,
) -> dict[str, Any]:
    normalized = _as_dict(normalized_payload)
    raw = deepcopy(raw_payload) if isinstance(raw_payload, dict) else raw_payload
    present_required, missing_required = _field_presence(normalized, ORDER_STATUS_REQUIRED_FIELDS)
    present_optional, missing_optional = _field_presence(normalized, ORDER_STATUS_OPTIONAL_FIELDS)
    issues: list[str] = []
    classification = SUFFICIENT_BROKER_TRUTH
    if not requested_broker_order_id and not normalized:
        classification = PARTIAL_USABLE_TRUTH
        issues.append("representative_order_unavailable")
    elif not normalized:
        classification = INSUFFICIENT_TRUTH_RECONCILE
        issues.append("normalized_payload_missing")
    elif missing_required:
        classification = INSUFFICIENT_TRUTH_RECONCILE
        issues.append("required_fields_missing")
    elif requested_broker_order_id and str(normalized.get("broker_order_id") or "").strip() != requested_broker_order_id:
        classification = CONFLICTING_TRUTH_FAULT_REVIEW
        issues.append("requested_order_id_mismatch")
    elif missing_optional:
        classification = PARTIAL_USABLE_TRUTH
        issues.append("optional_fields_missing")
    return {
        "schema_name": "order_status",
        "classification": classification,
        "requested_broker_order_id": requested_broker_order_id,
        "required_fields": list(ORDER_STATUS_REQUIRED_FIELDS),
        "optional_fields": list(ORDER_STATUS_OPTIONAL_FIELDS),
        "present_required_fields": present_required,
        "missing_required_fields": missing_required,
        "present_optional_fields": present_optional,
        "missing_optional_fields": missing_optional,
        "issues": issues,
        "normalized_payload": normalized,
        "raw_payload": raw,
    }


def validate_open_orders_rows(*, normalized_rows: list[dict[str, Any]] | None) -> dict[str, Any]:
    rows = [_as_dict(row) for row in list(normalized_rows or [])]
    issues: list[str] = []
    missing_required_rows: list[dict[str, Any]] = []
    partial_rows: list[str] = []
    duplicate_conflicts: list[str] = []
    seen_signatures: dict[str, tuple[str, str, str, str]] = {}
    for row in rows:
        broker_order_id = str(row.get("broker_order_id") or "").strip()
        _, missing_required = _field_presence(row, OPEN_ORDER_REQUIRED_FIELDS)
        _, missing_optional = _field_presence(row, OPEN_ORDER_OPTIONAL_FIELDS)
        if missing_required:
            missing_required_rows.append(
                {
                    "broker_order_id": broker_order_id or None,
                    "missing_required_fields": missing_required,
                }
            )
        elif missing_optional:
            partial_rows.append(broker_order_id or str(len(partial_rows)))
        if broker_order_id:
            signature = (
                str(row.get("symbol") or "").strip().upper(),
                str(row.get("status") or "").strip().upper(),
                str(row.get("instruction") or "").strip().upper(),
                str(row.get("quantity") or "").strip(),
            )
            previous = seen_signatures.get(broker_order_id)
            if previous is None:
                seen_signatures[broker_order_id] = signature
            elif previous != signature:
                duplicate_conflicts.append(broker_order_id)
    classification = SUFFICIENT_BROKER_TRUTH
    if duplicate_conflicts:
        classification = CONFLICTING_TRUTH_FAULT_REVIEW
        issues.append("duplicate_order_id_conflict")
    elif missing_required_rows:
        classification = INSUFFICIENT_TRUTH_RECONCILE
        issues.append("required_fields_missing")
    elif partial_rows:
        classification = PARTIAL_USABLE_TRUTH
        issues.append("optional_fields_missing")
    return {
        "schema_name": "open_orders",
        "classification": classification,
        "required_fields": list(OPEN_ORDER_REQUIRED_FIELDS),
        "optional_fields": list(OPEN_ORDER_OPTIONAL_FIELDS),
        "row_count": len(rows),
        "missing_required_rows": missing_required_rows,
        "partial_rows": partial_rows,
        "duplicate_conflicts": duplicate_conflicts,
        "issues": issues,
        "normalized_samples": rows[:5],
    }


def validate_position_rows(
    *,
    normalized_rows: list[dict[str, Any]] | None,
    target_symbol: str,
) -> dict[str, Any]:
    rows = [_as_dict(row) for row in list(normalized_rows or [])]
    target = str(target_symbol or "").strip().upper()
    target_rows = [row for row in rows if str(row.get("symbol") or "").strip().upper() == target]
    issues: list[str] = []
    missing_required_rows: list[dict[str, Any]] = []
    partial_rows: list[str] = []
    duplicate_conflicts: list[str] = []
    if len(target_rows) > 1:
        signatures = {
            (
                str(row.get("side") or "").strip().upper(),
                str(row.get("quantity") or "").strip(),
            )
            for row in target_rows
        }
        if len(signatures) > 1:
            duplicate_conflicts.append(target)
    for row in target_rows:
        _, missing_required = _field_presence(row, POSITION_REQUIRED_FIELDS)
        _, missing_optional = _field_presence(row, POSITION_OPTIONAL_FIELDS)
        if missing_required:
            missing_required_rows.append(
                {
                    "symbol": target,
                    "missing_required_fields": missing_required,
                }
            )
        elif missing_optional:
            partial_rows.append(target)
    classification = SUFFICIENT_BROKER_TRUTH
    if duplicate_conflicts:
        classification = CONFLICTING_TRUTH_FAULT_REVIEW
        issues.append("duplicate_position_conflict")
    elif missing_required_rows:
        classification = INSUFFICIENT_TRUTH_RECONCILE
        issues.append("required_fields_missing")
    elif partial_rows:
        classification = PARTIAL_USABLE_TRUTH
        issues.append("optional_fields_missing")
    return {
        "schema_name": "position",
        "classification": classification,
        "required_fields": list(POSITION_REQUIRED_FIELDS),
        "optional_fields": list(POSITION_OPTIONAL_FIELDS),
        "target_symbol": target,
        "target_position_present": bool(target_rows),
        "all_row_count": len(rows),
        "target_row_count": len(target_rows),
        "missing_required_rows": missing_required_rows,
        "partial_rows": partial_rows,
        "duplicate_conflicts": duplicate_conflicts,
        "issues": issues,
        "normalized_samples": target_rows[:3] if target_rows else rows[:3],
        "interpretation": "target_symbol_absent_flat_truth" if not target_rows else "target_symbol_position_truth",
    }


def validate_account_health_snapshot(*, snapshot: dict[str, Any]) -> dict[str, Any]:
    snapshot_copy = deepcopy(snapshot)
    health = _as_dict(snapshot_copy.get("health"))
    connection = _as_dict(snapshot_copy.get("connection"))
    auth = _as_dict(snapshot_copy.get("auth"))
    freshness = _as_dict(snapshot_copy.get("freshness"))
    normalized = {
        "status": "HEALTHY"
        if bool(_as_dict(health.get("broker_reachable")).get("ok"))
        and bool(_as_dict(health.get("auth_healthy")).get("ok"))
        and bool(_as_dict(health.get("account_selected")).get("ok"))
        else "DEGRADED",
        "broker_reachable": _as_dict(health.get("broker_reachable")).get("ok"),
        "auth_ready": _as_dict(health.get("auth_healthy")).get("ok"),
        "account_selected": _as_dict(health.get("account_selected")).get("ok"),
        "selected_account_hash": connection.get("selected_account_hash"),
        "broker_reachable_label": _as_dict(health.get("broker_reachable")).get("label"),
        "auth_label": _as_dict(health.get("auth_healthy")).get("label") or auth.get("label"),
        "account_selected_label": _as_dict(health.get("account_selected")).get("label"),
        "balances_fresh_state": _as_dict(freshness.get("balances")).get("state"),
        "positions_fresh_state": _as_dict(freshness.get("positions")).get("state"),
        "orders_fresh_state": _as_dict(freshness.get("orders")).get("state"),
        "fills_events_fresh_state": _as_dict(freshness.get("fills")).get("state"),
        "reconciliation_fresh_label": _as_dict(health.get("reconciliation_fresh")).get("label"),
        "detail": snapshot_copy.get("detail"),
    }
    present_required, missing_required = _field_presence(normalized, ACCOUNT_HEALTH_REQUIRED_FIELDS)
    present_optional, missing_optional = _field_presence(normalized, ACCOUNT_HEALTH_OPTIONAL_FIELDS)
    issues: list[str] = []
    classification = SUFFICIENT_BROKER_TRUTH
    if missing_required:
        classification = INSUFFICIENT_TRUTH_RECONCILE
        issues.append("required_fields_missing")
    elif missing_optional:
        classification = PARTIAL_USABLE_TRUTH
        issues.append("optional_fields_missing")
    return {
        "schema_name": "account_health",
        "classification": classification,
        "required_fields": list(ACCOUNT_HEALTH_REQUIRED_FIELDS),
        "optional_fields": list(ACCOUNT_HEALTH_OPTIONAL_FIELDS),
        "present_required_fields": present_required,
        "missing_required_fields": missing_required,
        "present_optional_fields": present_optional,
        "missing_optional_fields": missing_optional,
        "issues": issues,
        "normalized_payload": normalized,
    }


def build_broker_truth_shadow_validation_payload(
    *,
    generated_at: str,
    selected_account_hash: str | None,
    target_symbol: str,
    timeframe: str,
    direct_status_sample: dict[str, Any],
    open_orders_validation: dict[str, Any],
    position_validation: dict[str, Any],
    account_health_validation: dict[str, Any],
) -> dict[str, Any]:
    validations = [
        direct_status_sample,
        open_orders_validation,
        position_validation,
        account_health_validation,
    ]
    classifications = [str(row.get("classification") or "") for row in validations]
    if CONFLICTING_TRUTH_FAULT_REVIEW in classifications:
        overall = CONFLICTING_TRUTH_FAULT_REVIEW
        result = "FAIL"
    elif INSUFFICIENT_TRUTH_RECONCILE in classifications:
        overall = INSUFFICIENT_TRUTH_RECONCILE
        result = "FAIL"
    elif PARTIAL_USABLE_TRUTH in classifications:
        overall = PARTIAL_USABLE_TRUTH
        result = "WARN"
    else:
        overall = SUFFICIENT_BROKER_TRUTH
        result = "PASS"
    return {
        "generated_at": generated_at,
        "scope_label": "MGC 5m broker-truth schema validation",
        "allowed_scope": {
            "symbol": target_symbol,
            "timeframe": timeframe,
            "mode": "READ_ONLY_LIVE_SHADOW",
            "state_mutation": "none",
        },
        "selected_account_hash": selected_account_hash,
        "schemas": deepcopy(BROKER_TRUTH_SCHEMA_DEFINITIONS),
        "validations": {
            "order_status": direct_status_sample,
            "open_orders": open_orders_validation,
            "position": position_validation,
            "account_health": account_health_validation,
        },
        "summary": {
            "result": result,
            "overall_classification": overall,
            "target_symbol": target_symbol,
            "timeframe": timeframe,
            "sufficient_components": sum(1 for row in validations if row.get("classification") == SUFFICIENT_BROKER_TRUTH),
            "partial_components": sum(1 for row in validations if row.get("classification") == PARTIAL_USABLE_TRUTH),
            "insufficient_components": sum(1 for row in validations if row.get("classification") == INSUFFICIENT_TRUTH_RECONCILE),
            "conflicting_components": sum(1 for row in validations if row.get("classification") == CONFLICTING_TRUTH_FAULT_REVIEW),
            "missing_or_ambiguous_fields": [
                {
                    "schema_name": row.get("schema_name"),
                    "missing_required_fields": list(row.get("missing_required_fields") or []),
                    "missing_optional_fields": list(row.get("missing_optional_fields") or []),
                    "issues": list(row.get("issues") or []),
                }
                for row in validations
                if row.get("issues")
            ],
            "summary_line": (
                f"{result} | classification={overall} | "
                f"order_status={direct_status_sample.get('classification')} | "
                f"open_orders={open_orders_validation.get('classification')} | "
                f"position={position_validation.get('classification')} | "
                f"account_health={account_health_validation.get('classification')}"
            ),
        },
    }


def _field_presence(payload: dict[str, Any], fields: tuple[str, ...]) -> tuple[list[str], list[str]]:
    present: list[str] = []
    missing: list[str] = []
    for field in fields:
        value = payload.get(field)
        if value is None or (isinstance(value, str) and not value.strip()):
            missing.append(field)
        else:
            present.append(field)
    return present, missing


def _as_dict(value: Any) -> dict[str, Any]:
    return dict(value) if isinstance(value, dict) else {}
