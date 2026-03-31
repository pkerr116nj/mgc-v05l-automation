"""Tests for broker-truth schema validation helpers."""

from __future__ import annotations

from copy import deepcopy

from mgc_v05l.production_link.schema_validation import (
    CONFLICTING_TRUTH_FAULT_REVIEW,
    INSUFFICIENT_TRUTH_RECONCILE,
    PARTIAL_USABLE_TRUTH,
    SUFFICIENT_BROKER_TRUTH,
    build_broker_truth_shadow_validation_payload,
    validate_account_health_snapshot,
    validate_open_orders_rows,
    validate_order_status_sample,
    validate_position_rows,
)


def test_validate_order_status_sample_is_sufficient_when_required_fields_exist() -> None:
    payload = {
        "broker_order_id": "broker-123",
        "status": "WORKING",
        "symbol": "MGC",
    }

    result = validate_order_status_sample(
        raw_payload={"orderId": "broker-123", "status": "WORKING"},
        normalized_payload=payload,
        requested_broker_order_id="broker-123",
    )

    assert result["classification"] == PARTIAL_USABLE_TRUTH
    assert "optional_fields_missing" in result["issues"]


def test_validate_order_status_sample_conflicts_on_requested_order_id_mismatch() -> None:
    result = validate_order_status_sample(
        raw_payload={"orderId": "broker-999", "status": "WORKING"},
        normalized_payload={"broker_order_id": "broker-999", "status": "WORKING"},
        requested_broker_order_id="broker-123",
    )

    assert result["classification"] == CONFLICTING_TRUTH_FAULT_REVIEW
    assert "requested_order_id_mismatch" in result["issues"]


def test_validate_order_status_sample_is_partial_when_no_representative_order_exists() -> None:
    result = validate_order_status_sample(
        raw_payload=None,
        normalized_payload=None,
        requested_broker_order_id=None,
    )

    assert result["classification"] == PARTIAL_USABLE_TRUTH
    assert result["issues"] == ["representative_order_unavailable"]


def test_validate_open_orders_rows_treats_empty_payload_as_sufficient_truth() -> None:
    result = validate_open_orders_rows(normalized_rows=[])

    assert result["classification"] == SUFFICIENT_BROKER_TRUTH
    assert result["row_count"] == 0


def test_validate_open_orders_rows_escalates_when_required_fields_are_missing() -> None:
    result = validate_open_orders_rows(
        normalized_rows=[
            {
                "broker_order_id": "broker-1",
                "symbol": "MGC",
                "status": "WORKING",
            }
        ]
    )

    assert result["classification"] == INSUFFICIENT_TRUTH_RECONCILE
    assert result["missing_required_rows"][0]["missing_required_fields"] == ["instruction", "quantity"]


def test_validate_open_orders_rows_detects_duplicate_conflicts() -> None:
    result = validate_open_orders_rows(
        normalized_rows=[
            {
                "broker_order_id": "broker-1",
                "symbol": "MGC",
                "status": "WORKING",
                "instruction": "BUY",
                "quantity": "1",
            },
            {
                "broker_order_id": "broker-1",
                "symbol": "MGC",
                "status": "FILLED",
                "instruction": "BUY",
                "quantity": "1",
            },
        ]
    )

    assert result["classification"] == CONFLICTING_TRUTH_FAULT_REVIEW
    assert result["duplicate_conflicts"] == ["broker-1"]


def test_validate_position_rows_treats_absent_target_symbol_as_sufficient_flat_truth() -> None:
    result = validate_position_rows(
        normalized_rows=[{"symbol": "AAPL", "side": "LONG", "quantity": "1"}],
        target_symbol="MGC",
    )

    assert result["classification"] == SUFFICIENT_BROKER_TRUTH
    assert result["interpretation"] == "target_symbol_absent_flat_truth"


def test_validate_position_rows_detects_target_symbol_conflicts() -> None:
    result = validate_position_rows(
        normalized_rows=[
            {"symbol": "MGC", "side": "LONG", "quantity": "1"},
            {"symbol": "MGC", "side": "SHORT", "quantity": "1"},
        ],
        target_symbol="MGC",
    )

    assert result["classification"] == CONFLICTING_TRUTH_FAULT_REVIEW
    assert result["duplicate_conflicts"] == ["MGC"]


def test_validate_account_health_snapshot_handles_optional_freshness_as_partial() -> None:
    result = validate_account_health_snapshot(
        snapshot={
            "health": {
                "broker_reachable": {"ok": True, "label": "BROKER REACHABLE"},
                "auth_healthy": {"ok": True, "label": "AUTH READY"},
                "account_selected": {"ok": True, "label": "ACCOUNT SELECTED"},
            },
            "connection": {"selected_account_hash": "hash-123"},
            "auth": {"label": "AUTH READY"},
            "freshness": {},
            "detail": "healthy",
        }
    )

    assert result["classification"] == PARTIAL_USABLE_TRUTH
    assert "optional_fields_missing" in result["issues"]


def test_build_broker_truth_shadow_validation_payload_preserves_inputs_and_summary_truth() -> None:
    direct_status = {
        "schema_name": "order_status",
        "classification": PARTIAL_USABLE_TRUTH,
        "issues": ["representative_order_unavailable"],
        "missing_required_fields": [],
        "missing_optional_fields": [],
    }
    open_orders = {
        "schema_name": "open_orders",
        "classification": SUFFICIENT_BROKER_TRUTH,
        "issues": [],
        "missing_required_fields": [],
        "missing_optional_fields": [],
    }
    position = {
        "schema_name": "position",
        "classification": SUFFICIENT_BROKER_TRUTH,
        "issues": [],
        "missing_required_fields": [],
        "missing_optional_fields": [],
    }
    account_health = {
        "schema_name": "account_health",
        "classification": SUFFICIENT_BROKER_TRUTH,
        "issues": [],
        "missing_required_fields": [],
        "missing_optional_fields": [],
    }
    before = deepcopy((direct_status, open_orders, position, account_health))

    payload = build_broker_truth_shadow_validation_payload(
        generated_at="2026-03-27T12:00:00+00:00",
        selected_account_hash="hash-123",
        target_symbol="MGC",
        timeframe="5m",
        direct_status_sample=direct_status,
        open_orders_validation=open_orders,
        position_validation=position,
        account_health_validation=account_health,
    )

    assert payload["summary"]["result"] == "WARN"
    assert payload["summary"]["overall_classification"] == PARTIAL_USABLE_TRUTH
    assert payload["summary"]["partial_components"] == 1
    assert payload["summary"]["sufficient_components"] == 3
    assert payload["summary"]["missing_or_ambiguous_fields"][0]["schema_name"] == "order_status"
    assert (direct_status, open_orders, position, account_health) == before
