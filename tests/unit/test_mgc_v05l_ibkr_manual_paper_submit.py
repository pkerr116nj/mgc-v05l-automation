from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.brokers.ibkr import IbkrQualifiedContract
from mgc_v05l.execution.ibkr_manual_paper_submit import (
    _CLOSE_TEST_MODE,
    _FILL_TEST_MODE,
    _MANUAL_CONFIRMATION_WAIT_STATE,
    _classify_submit_lifecycle,
    _configure_minimal_futures_limit_order,
    _detect_order_rejection,
    _derive_marketable_limit_price,
    _exact_contract_position_quantity,
    _execute_submit_cancel_lifecycle,
    _qualified_contract_with_api_details,
    _submit_input_guardrails,
    artifact_stem_for_test_mode,
    IbkrManualPaperSubmitArtifacts,
    IbkrManualPaperSubmitConfig,
    IbkrManualPaperSubmitTransport,
    IbkrReadOnlyApiTransportConfig,
    build_submit_approval_phrase,
    evaluate_cancel_verification,
    render_ibkr_manual_paper_submit_markdown,
    run_ibkr_manual_paper_submit_test,
    write_ibkr_manual_paper_submit_artifacts,
)


def test_preview_only_default_does_not_submit(monkeypatch) -> None:
    _patch_harness_context(monkeypatch)

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(submit=False),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_PARTIAL"
    assert artifacts.report["submit_cancel_lifecycle"]["status"] == "preview_only"


def test_submit_cannot_occur_without_exact_digest(monkeypatch, tmp_path: Path) -> None:
    _, frozen_path = _preview_bundle(monkeypatch, tmp_path)
    expected_phrase = build_submit_approval_phrase(
        selected_account_id="DUM882026",
        digest="wrong-digest",
        requested_order={"symbol": "MGC", "expiry": "202606", "action": "BUY", "quantity": 1.0, "order_type": "LMT", "limit_price": 4639.7, "time_in_force": "DAY"},
        delayed_quote_warning="Live market data is unavailable in this paper session. The preview uses delayed data only.",
        test_mode="PAPER_RESTING_TEST",
    )

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(
            submit=True,
            approval_digest="wrong-digest",
            approval_phrase=expected_phrase,
            output_dir=tmp_path,
            frozen_preview_path=frozen_path,
        ),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"
    assert artifacts.report["submit_cancel_lifecycle"]["status"] == "approval_blocked"


def test_submit_cannot_occur_without_typed_phrase(monkeypatch, tmp_path: Path) -> None:
    _, frozen_path = _preview_bundle(monkeypatch, tmp_path)

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(
            submit=True,
            approval_digest="whatever",
            approval_phrase=None,
            output_dir=tmp_path,
            frozen_preview_path=frozen_path,
        ),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"
    assert artifacts.report["submit_cancel_lifecycle"]["status"] == "approval_blocked"


def test_digest_mismatch_fails_closed(monkeypatch, tmp_path: Path) -> None:
    _, frozen_path = _preview_bundle(monkeypatch, tmp_path)

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(
            submit=True,
            approval_digest="mismatch",
            approval_phrase="mismatch",
            output_dir=tmp_path,
            frozen_preview_path=frozen_path,
        ),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def test_live_port_7496_fails_closed() -> None:
    artifacts = run_ibkr_manual_paper_submit_test(config=_config(port=7496), stack_provider=_manual_stack)
    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def test_port_4001_fails_closed() -> None:
    artifacts = run_ibkr_manual_paper_submit_test(config=_config(port=4001), stack_provider=_manual_stack)
    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def test_port_4002_fails_closed_for_this_pass() -> None:
    artifacts = run_ibkr_manual_paper_submit_test(config=_config(port=4002), stack_provider=_manual_stack)
    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def test_qty_greater_than_one_fails_closed() -> None:
    artifacts = run_ibkr_manual_paper_submit_test(config=_config(quantity=2.0), stack_provider=_manual_stack)
    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def test_non_mgc_contract_fails_closed() -> None:
    artifacts = run_ibkr_manual_paper_submit_test(config=_config(symbol="GC"), stack_provider=_manual_stack)
    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def test_market_order_fails_closed() -> None:
    artifacts = run_ibkr_manual_paper_submit_test(config=_config(order_type="MKT"), stack_provider=_manual_stack)
    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def test_missing_limit_price_fails_closed() -> None:
    artifacts = run_ibkr_manual_paper_submit_test(config=_config(limit_price=None), stack_provider=_manual_stack)
    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def test_strategy_style_caller_fails_closed() -> None:
    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(caller_path="mgc_v05l.strategy.manual_submit"),
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.strategy.strategy_engine"}))],
    )
    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"


def test_delayed_quote_unavailable_fails_closed(monkeypatch) -> None:
    _patch_harness_context(monkeypatch, context_override={"quote_context": _quote_context(has_quote=False, updated_at=None, bid_price=None, last_price=None)})

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(limit_price=4639.7),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"
    assert any(check["name"] == "delayed_quote_available" and check["passed"] is False for check in artifacts.report["guardrail_checks"])


def test_stale_delayed_quote_fails_closed(monkeypatch) -> None:
    _patch_harness_context(monkeypatch, context_override={"quote_context": _quote_context(updated_at="2020-01-01T00:00:00+00:00")})

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(limit_price=4639.7),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"
    assert any(check["name"] == "delayed_quote_fresh" and check["passed"] is False for check in artifacts.report["guardrail_checks"])


def test_far_away_placeholder_limit_fails_closed(monkeypatch) -> None:
    _patch_harness_context(monkeypatch)

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(limit_price=4500.0),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"
    assert any(check["name"] == "near_market_non_marketable_buy_limit" and check["passed"] is False for check in artifacts.report["guardrail_checks"])


def test_audit_serialization_works(tmp_path: Path) -> None:
    artifacts = IbkrManualPaperSubmitArtifacts(
        classification="IBKR_MANUAL_PAPER_SUBMIT_CANCEL_PARTIAL",
        report={
            "classification": "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_PARTIAL",
            "generated_at": "2026-04-28T12:00:00+00:00",
            "account_id": "DUM882026",
            "connection_check": {"client_id": 9074},
            "environment_lock_check": {
                "configured_mode": "PAPER",
                "configured_host": "127.0.0.1",
                "configured_port": 7497,
            },
            "preview": {
                "preview_digest": "abc123",
                "expected_approval_phrase": "APPROVE ...",
                "quote_source_label": "DELAYED",
                "live_market_data_warning": "Delayed only.",
                "quote_snapshot": {"bid_price": 4639.8, "last_price": 4640.0, "updated_at": "2026-04-28T12:00:00+00:00"},
                "reference_price_source": "bid_price",
                "reference_price": 4639.8,
                "distance_from_quote": 0.1,
                "distance_ticks": 1.0,
                "estimated_notional": 45000.0,
                "estimated_tick_value": 1.0,
            },
            "guardrail_checks": [
                {"name": "manual_cli_only", "passed": True, "blocking": True, "detail": "ok"},
            ],
            "submit_cancel_lifecycle": {"status": "preview_only", "detail": "Preview only."},
            "audit_event_count": 1,
        },
        audit_events=[{"event_type": "preview_generated", "recorded_at": "2026-04-28T12:00:00+00:00"}],
        open_order_before={"open_order_count": 0},
        open_order_after_submit={"status": "not_run"},
        open_order_after_cancel={"status": "not_run"},
    )

    write_ibkr_manual_paper_submit_artifacts(output_dir=tmp_path, artifacts=artifacts)

    assert (tmp_path / "ibkr_manual_paper_submit_report.json").exists()
    assert (tmp_path / "ibkr_manual_paper_submit_report.md").exists()
    assert (tmp_path / "ibkr_manual_paper_submit_audit.jsonl").exists()
    assert (tmp_path / "ibkr_manual_paper_submit_open_order_before.json").exists()
    assert (tmp_path / "ibkr_manual_paper_submit_open_order_after_submit.json").exists()
    assert (tmp_path / "ibkr_manual_paper_submit_open_order_after_cancel.json").exists()
    payload = json.loads((tmp_path / "ibkr_manual_paper_submit_report.json").read_text(encoding="utf-8"))
    markdown = render_ibkr_manual_paper_submit_markdown(payload)
    assert "manual CLI only" in markdown


def test_manual_mgc_futures_order_does_not_include_etradeonly() -> None:
    raw_order = SimpleNamespace(
        eTradeOnly=True,
        firmQuoteOnly=True,
        nbboPriceCap=999.0,
        auctionStrategy=0,
        discretionaryAmt=1.0,
        outsideRth=True,
    )

    _configure_minimal_futures_limit_order(
        raw_order,
        order_id=1,
        account_id="DUM882026",
        action="BUY",
        quantity=1.0,
        limit_price=4599.6,
        time_in_force="DAY",
        common_module=SimpleNamespace(UNSET_DOUBLE=-1.0, UNSET_INTEGER=-1),
    )

    assert raw_order.eTradeOnly is False
    assert raw_order.orderType == "LMT"
    assert raw_order.tif == "DAY"
    assert raw_order.transmit is True


def test_manual_mgc_futures_order_omits_unsupported_stock_only_attributes() -> None:
    raw_order = SimpleNamespace(
        eTradeOnly=True,
        firmQuoteOnly=True,
        nbboPriceCap=999.0,
        auctionStrategy=7,
        discretionaryAmt=1.0,
        outsideRth=True,
    )

    _configure_minimal_futures_limit_order(
        raw_order,
        order_id=1,
        account_id="DUM882026",
        action="BUY",
        quantity=1.0,
        limit_price=4599.6,
        time_in_force="DAY",
        common_module=SimpleNamespace(UNSET_DOUBLE=-1.0, UNSET_INTEGER=-1),
    )

    assert raw_order.firmQuoteOnly is False
    assert raw_order.nbboPriceCap == -1.0
    assert raw_order.auctionStrategy == -1
    assert raw_order.discretionaryAmt == -1.0
    assert raw_order.outsideRth is False


def test_qualified_mgc_submit_payload_uses_exact_last_trade_date() -> None:
    qualified = IbkrQualifiedContract(
        internal_symbol="MGC",
        broker_symbol="MGC",
        local_symbol="MGCM26",
        security_type="FUT",
        exchange="COMEX",
        currency="USD",
        expiry="202606",
        multiplier="10",
        trading_class="MGC",
        con_id=712565978,
        metadata={"contract_month": "202606"},
    )
    exact = _qualified_contract_with_api_details(
        qualified,
        {
            "con_id": 712565978,
            "expiry": "20260626",
            "local_symbol": "MGCM6",
            "exchange": "COMEX",
            "currency": "USD",
            "multiplier": "10",
            "trading_class": "MGC",
        },
    )

    class _FakeContract:
        pass

    transport = IbkrManualPaperSubmitTransport(
        client=SimpleNamespace(record_event=lambda *args, **kwargs: None),
        collector=SimpleNamespace(),
        config=IbkrReadOnlyApiTransportConfig(host="127.0.0.1", port=7497, client_id=1, read_only=False),
        module_loader=lambda name: SimpleNamespace(Contract=_FakeContract) if name == "ibapi.contract" else SimpleNamespace(),
    )
    raw_contract = transport._raw_contract(exact)

    assert exact.expiry == "20260626"
    assert raw_contract.lastTradeDateOrContractMonth == "20260626"
    assert raw_contract.conId == 712565978
    assert raw_contract.localSymbol == "MGCM6"


def test_unqualified_contract_lookup_does_not_force_local_symbol() -> None:
    unqualified = IbkrQualifiedContract(
        internal_symbol="MGC",
        broker_symbol="MGC",
        local_symbol="MGCM26",
        security_type="FUT",
        exchange="COMEX",
        currency="USD",
        expiry="202606",
        multiplier="10",
        trading_class="MGC",
        con_id=None,
        metadata={"contract_month": "202606"},
    )

    class _FakeContract:
        pass

    transport = IbkrManualPaperSubmitTransport(
        client=SimpleNamespace(record_event=lambda *args, **kwargs: None),
        collector=SimpleNamespace(),
        config=IbkrReadOnlyApiTransportConfig(host="127.0.0.1", port=7497, client_id=1, read_only=False),
        module_loader=lambda name: SimpleNamespace(Contract=_FakeContract) if name == "ibapi.contract" else SimpleNamespace(),
    )
    raw_contract = transport._raw_contract(unqualified)

    assert raw_contract.lastTradeDateOrContractMonth == "202606"
    assert not hasattr(raw_contract, "localSymbol")


def test_error_478_is_classified_as_paper_order_rejected_with_contract_expiry_conflict() -> None:
    collector = SimpleNamespace(
        errors=[
            {
                "request_id": 1,
                "code": 478,
                "message": "Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;",
            }
        ],
        latest_order_status=lambda order_id: None,
    )

    rejection = _detect_order_rejection(collector=collector, order_id=1)

    assert rejection is not None
    assert rejection["reason"] == "contract_expiry_conflict"
    assert _classify_submit_lifecycle(_FILL_TEST_MODE, "rejected") == "PAPER_ORDER_REJECTED"


def test_close_mode_requires_sell_and_classifies_close_outcomes() -> None:
    buy_guardrails = _submit_input_guardrails(
        {
            "symbol": "MGC",
            "expiry": "202606",
            "action": "BUY",
            "quantity": 1.0,
            "order_type": "LMT",
            "limit_price": 4639.7,
            "time_in_force": "DAY",
        },
        test_mode=_CLOSE_TEST_MODE,
        require_limit_price=True,
    )
    sell_guardrails = _submit_input_guardrails(
        {
            "symbol": "MGC",
            "expiry": "202606",
            "action": "SELL",
            "quantity": 1.0,
            "order_type": "LMT",
            "limit_price": 4639.7,
            "time_in_force": "DAY",
        },
        test_mode=_CLOSE_TEST_MODE,
        require_limit_price=True,
    )

    assert buy_guardrails["supported_action"]["passed"] is False
    assert sell_guardrails["supported_action"]["passed"] is True
    assert _classify_submit_lifecycle(_CLOSE_TEST_MODE, "filled_flat") == "PAPER_CLOSE_FILLED_FLAT"
    assert _classify_submit_lifecycle(_CLOSE_TEST_MODE, "rejected") == "PAPER_CLOSE_REJECTED"
    assert _classify_submit_lifecycle(_CLOSE_TEST_MODE, "fill_timeout_cancelled") == "PAPER_CLOSE_NOT_FILLED_CANCELLED"
    assert _classify_submit_lifecycle(_CLOSE_TEST_MODE, "close_position_not_flat") == "PAPER_CLOSE_UNKNOWN_NEEDS_MANUAL_TWS_REVIEW"


def test_close_mode_derives_marketable_limit_below_delayed_bid() -> None:
    limit_price = _derive_marketable_limit_price(
        quote_context={"bid_price": 4608.5, "last_price": 4608.6},
        contract_report={"api_contract_details": [{"min_tick": 0.1}]},
        offset_ticks=1.0,
        test_mode=_CLOSE_TEST_MODE,
        action="SELL",
    )

    assert limit_price == 4608.4


def test_exact_contract_position_quantity_matches_local_symbol_and_expiry() -> None:
    quantity = _exact_contract_position_quantity(
        positions_snapshot={
            "positions": [
                {"symbol": "MGC", "local_symbol": "MGCM6", "expiry": "20260626", "quantity": "1.0"},
                {"symbol": "MGC", "local_symbol": "MGCQ6", "expiry": "20260827", "quantity": "2.0"},
            ]
        },
        contract_report={
            "qualified_contract": {
                "broker_symbol": "MGC",
                "local_symbol": "MGCM6",
                "expiry": "20260626",
            }
        },
    )

    assert quantity == 1.0


def test_error_10268_is_classified_as_paper_order_rejected_with_unsupported_attribute() -> None:
    collector = SimpleNamespace(
        errors=[
            {
                "request_id": 1,
                "code": 10268,
                "message": "The 'EtradeOnly' order attribute is not supported.",
            }
        ],
        latest_order_status=lambda order_id: None,
    )

    rejection = _detect_order_rejection(collector=collector, order_id=1)

    assert rejection is not None
    assert rejection["unsupported_attribute"] == "EtradeOnly"
    assert _classify_submit_lifecycle(_FILL_TEST_MODE, "rejected") == "PAPER_ORDER_REJECTED"


def test_markdown_classification_matches_structured_json_for_rejection() -> None:
    payload = {
        "classification": "PAPER_ORDER_REJECTED",
        "generated_at": "2026-04-28T12:33:22+00:00",
        "account_id": "DUM882026",
        "connection_check": {"client_id": 9074},
        "environment_lock_check": {
            "configured_mode": "PAPER",
            "configured_host": "127.0.0.1",
            "configured_port": 7497,
        },
        "preview": {
            "test_mode": _FILL_TEST_MODE,
            "preview_digest": "digest",
            "expected_approval_phrase": "APPROVE ...",
            "quote_source_label": "DELAYED",
            "live_market_data_warning": "Delayed only.",
            "quote_snapshot": {"ask_price": 4599.5},
            "reference_price_source": "ask_price",
            "reference_price": 4599.5,
            "limit_price": 4599.6,
            "distance_from_quote": 0.1,
            "distance_ticks": 1.0,
            "pricing_label": "MARKETABLE_LIMIT_INTENDED_TO_FILL_IN_PAPER",
            "intended_to_fill": True,
            "estimated_notional": 45996.0,
            "estimated_tick_value": 1.0,
        },
        "guardrail_checks": [],
        "submit_cancel_lifecycle": {
            "status": "rejected",
            "detail": "IBKR rejected the paper order before it became broker-visible: The 'EtradeOnly' order attribute is not supported.",
            "rejection": {
                "error_code": 10268,
                "error_message": "The 'EtradeOnly' order attribute is not supported.",
                "unsupported_attribute": "EtradeOnly",
            },
            "fill_verification": {"executions_after_submit": []},
            "manual_confirmation": {
                "state": _MANUAL_CONFIRMATION_WAIT_STATE,
                "operator_outcome": "approved",
            },
        },
        "errors": [
            {"code": 10268, "message": "The 'EtradeOnly' order attribute is not supported."},
            {"code": 10147, "message": "OrderId 1 that needs to be cancelled is not found."},
        ],
        "audit_event_count": 4,
    }

    markdown = render_ibkr_manual_paper_submit_markdown(payload)

    assert "classification: `PAPER_ORDER_REJECTED`" in markdown
    assert "unsupported attribute: `EtradeOnly`" in markdown
    assert "cancel 10147 was expected after rejection because no order was working" in markdown


def test_markdown_rejection_can_report_contract_expiry_conflict() -> None:
    payload = {
        "classification": "PAPER_ORDER_REJECTED",
        "generated_at": "2026-04-28T12:56:45+00:00",
        "account_id": "DUM882026",
        "connection_check": {"client_id": 9137},
        "environment_lock_check": {
            "configured_mode": "PAPER",
            "configured_host": "127.0.0.1",
            "configured_port": 7497,
        },
        "preview": {
            "test_mode": _FILL_TEST_MODE,
            "preview_digest": "digest",
            "expected_approval_phrase": "APPROVE ...",
            "quote_source_label": "DELAYED",
            "live_market_data_warning": "Delayed only.",
            "quote_snapshot": {"ask_price": 4599.8},
            "reference_price_source": "ask_price",
            "reference_price": 4599.8,
            "limit_price": 4599.9,
            "distance_from_quote": 0.1,
            "distance_ticks": 1.0,
            "pricing_label": "MARKETABLE_LIMIT_INTENDED_TO_FILL_IN_PAPER",
            "intended_to_fill": True,
            "estimated_notional": 45999.0,
            "estimated_tick_value": 1.0,
        },
        "guardrail_checks": [],
        "submit_cancel_lifecycle": {
            "status": "rejected",
            "detail": "IBKR rejected the paper order before it became broker-visible: Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;",
            "rejection": {
                "reason": "contract_expiry_conflict",
                "error_code": 478,
                "error_message": "Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;",
                "unsupported_attribute": None,
            },
            "fill_verification": {"executions_after_submit": []},
            "manual_confirmation": {
                "state": _MANUAL_CONFIRMATION_WAIT_STATE,
                "operator_outcome": "approved",
            },
        },
        "errors": [
            {"code": 478, "message": "Parameters in request conflicts with contract parameters received by contract id: requested expiry 202606, in contract 20260626;"},
            {"code": 10147, "message": "OrderId 1 that needs to be cancelled is not found."},
        ],
        "audit_event_count": 4,
    }

    markdown = render_ibkr_manual_paper_submit_markdown(payload)

    assert "classification: `PAPER_ORDER_REJECTED`" in markdown
    assert "rejection code: `478`" in markdown
    assert "cancel 10147 was expected after rejection because no order was working" in markdown


def test_markdown_fill_can_report_exact_contract_and_position_follow_up() -> None:
    payload = {
        "classification": "PAPER_ORDER_FILLED",
        "generated_at": "2026-04-28T13:08:32+00:00",
        "account_id": "DUM882026",
        "connection_check": {"client_id": 9157},
        "environment_lock_check": {
            "configured_mode": "PAPER",
            "configured_host": "127.0.0.1",
            "configured_port": 7497,
        },
        "preview": {
            "test_mode": _FILL_TEST_MODE,
            "preview_digest": "digest",
            "expected_approval_phrase": "APPROVE ...",
            "quote_source_label": "DELAYED",
            "live_market_data_warning": "Delayed only.",
            "quote_snapshot": {"ask_price": 4608.7},
            "reference_price_source": "ask_price",
            "reference_price": 4608.7,
            "limit_price": 4608.8,
            "distance_from_quote": 0.1,
            "distance_ticks": 1.0,
            "pricing_label": "MARKETABLE_LIMIT_INTENDED_TO_FILL_IN_PAPER",
            "intended_to_fill": True,
            "estimated_notional": 46088.0,
            "estimated_tick_value": 1.0,
        },
        "frozen_preview_bundle": {
            "contract_report": {
                "qualified_contract": {
                    "expiry": "20260626",
                    "con_id": 712565978,
                    "local_symbol": "MGCM6",
                }
            }
        },
        "guardrail_checks": [],
        "submit_cancel_lifecycle": {
            "status": "filled",
            "detail": "Submitted one manual paper MGC limit order and verified the fill through broker truth.",
            "submitted_order_id": 1,
            "submitted_perm_id": 490708929,
            "latest_order_status": {
                "status": "Filled",
                "last_fill_price": 4589.6,
            },
            "fill_verification": {
                "final_status": "Filled",
                "filled_quantity": 1.0,
                "positions_after_submit": {
                    "positions": [
                        {
                            "symbol": "MGC",
                            "quantity": "0.0",
                        }
                    ]
                },
            },
            "manual_confirmation": {
                "state": _MANUAL_CONFIRMATION_WAIT_STATE,
                "operator_outcome": "approved",
            },
        },
        "errors": [],
        "audit_event_count": 11,
    }

    markdown = render_ibkr_manual_paper_submit_markdown(payload)

    assert "classification: `PAPER_ORDER_FILLED`" in markdown
    assert "exact qualified contract was used: `MGC 20260626` / `conId=712565978` / `localSymbol=MGCM6`" in markdown
    assert "fill price: `4589.6`" in markdown
    assert "this proves app-to-IBKR paper submit-and-fill plumbing works" in markdown
    assert "immediate post-submit position snapshot still showed MGC quantity 0.0" in markdown


def test_cancel_verification_logic_with_mocks() -> None:
    verified = evaluate_cancel_verification(
        order_id=1001,
        latest_order_status={"status": "Cancelled"},
        open_orders_after_cancel={"open_orders": []},
    )
    not_verified = evaluate_cancel_verification(
        order_id=1001,
        latest_order_status={"status": "Submitted"},
        open_orders_after_cancel={"open_orders": [{"broker_order_id": 1001, "status": "Submitted"}]},
    )

    assert verified["verified"] is True
    assert not_verified["verified"] is False


def test_manual_confirmation_wait_state_exists_only_in_manual_harness() -> None:
    repo_root = Path(__file__).resolve().parents[2]
    manual_source = (repo_root / "src" / "mgc_v05l" / "execution" / "ibkr_manual_paper_submit.py").read_text(encoding="utf-8")
    preview_source = (repo_root / "src" / "mgc_v05l" / "execution" / "ibkr_paper_order_preview.py").read_text(encoding="utf-8")

    assert _MANUAL_CONFIRMATION_WAIT_STATE in manual_source
    assert _MANUAL_CONFIRMATION_WAIT_STATE not in preview_source


def test_timeout_during_manual_confirmation_fails_closed(monkeypatch) -> None:
    runtime = _fake_runtime()
    snapshots = iter(
        [
            {"selected_account_id": "DUM882026", "open_order_count": 0, "open_orders": []},
            {"selected_account_id": "DUM882026", "open_order_count": 0, "open_orders": []},
        ]
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._refresh_open_orders_snapshot",
        lambda **kwargs: next(snapshots),
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._snapshot_digest",
        lambda snapshot: "baseline",
    )

    audit_events: list[dict[str, object]] = []
    result = _execute_submit_cancel_lifecycle(
        config=_config(submit=True, approval_digest="digest", approval_phrase="phrase"),
        runtime=runtime,
        context=_context(),
        requested_order=_requested_order(),
        sleep_fn=lambda _: None,
        audit_events=audit_events,
        preview_payload={},
        preview_digest="digest",
        manual_confirmation_fn=lambda **kwargs: {
            "state": _MANUAL_CONFIRMATION_WAIT_STATE,
            "operator_outcome": "timeout",
            "response_text": None,
            "detail": "Timed out waiting for the operator.",
            "timed_out": True,
        },
    )

    assert result["status"] == "manual_confirmation_timeout"
    assert runtime.transport.cancel_calls == []
    assert _event_types(audit_events) == [
        "submit_attempted",
        "manual_confirmation_wait_started",
        "manual_confirmation_response_recorded",
        "open_order_verification_failed",
    ]


def test_operator_rejected_path_does_not_attempt_cancel_unless_order_exists(monkeypatch) -> None:
    runtime = _fake_runtime()
    snapshots = iter(
        [
            {"selected_account_id": "DUM882026", "open_order_count": 0, "open_orders": []},
            {"selected_account_id": "DUM882026", "open_order_count": 0, "open_orders": []},
        ]
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._refresh_open_orders_snapshot",
        lambda **kwargs: next(snapshots),
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._snapshot_digest",
        lambda snapshot: "baseline",
    )

    audit_events: list[dict[str, object]] = []
    result = _execute_submit_cancel_lifecycle(
        config=_config(submit=True, approval_digest="digest", approval_phrase="phrase"),
        runtime=runtime,
        context=_context(),
        requested_order=_requested_order(),
        sleep_fn=lambda _: None,
        audit_events=audit_events,
        preview_payload={},
        preview_digest="digest",
        manual_confirmation_fn=lambda **kwargs: {
            "state": _MANUAL_CONFIRMATION_WAIT_STATE,
            "operator_outcome": "rejected",
            "response_text": "rejected",
            "detail": "Operator rejected the dialog.",
            "timed_out": False,
        },
    )

    assert result["status"] == "manual_confirmation_rejected_no_order"
    assert runtime.transport.cancel_calls == []
    assert "cancel_requested" not in _event_types(audit_events)


def test_operator_approved_path_proceeds_to_broker_truth_verification(monkeypatch) -> None:
    runtime = _fake_runtime(latest_order_status={"status": "Submitted", "perm_id": 999001})
    order_snapshot = {
        "selected_account_id": "DUM882026",
        "open_order_count": 1,
        "open_orders": [
            {
                "broker_order_id": 1,
                "perm_id": 999001,
                "status": "Submitted",
                "symbol": "MGC",
                "quantity": "1.0",
                "limit_price": "4500.0",
            }
        ],
    }
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._refresh_open_orders_snapshot",
        lambda **kwargs: {"selected_account_id": "DUM882026", "open_order_count": 0, "open_orders": []},
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._snapshot_digest",
        lambda snapshot: "baseline",
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._wait_for_submitted_order_visibility",
        lambda **kwargs: order_snapshot,
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._cancel_and_verify_visible_order",
        lambda **kwargs: {
            "detail": "Cancelled successfully.",
            "open_order_after_submit": order_snapshot,
            "open_order_after_cancel": {"selected_account_id": "DUM882026", "open_order_count": 0, "open_orders": []},
            "latest_order_status": {"status": "Cancelled", "perm_id": 999001},
            "cancel_verification": {"verified": True, "final_status": "Cancelled", "detail": "Cancel verified."},
        },
    )

    audit_events: list[dict[str, object]] = []
    result = _execute_submit_cancel_lifecycle(
        config=_config(submit=True, approval_digest="digest", approval_phrase="phrase"),
        runtime=runtime,
        context=_context(),
        requested_order=_requested_order(),
        sleep_fn=lambda _: None,
        audit_events=audit_events,
        preview_payload={},
        preview_digest="digest",
        manual_confirmation_fn=lambda **kwargs: {
            "state": _MANUAL_CONFIRMATION_WAIT_STATE,
            "operator_outcome": "approved",
            "response_text": "approved",
            "detail": "Operator approved the dialog.",
            "timed_out": False,
        },
    )

    assert result["status"] == "passed"
    assert "broker_truth_verification_started" in _event_types(audit_events)
    assert "open_order_verified_after_submit" in _event_types(audit_events)


def test_audit_includes_manual_confirmation_events(monkeypatch) -> None:
    runtime = _fake_runtime()
    snapshots = iter(
        [
            {"selected_account_id": "DUM882026", "open_order_count": 0, "open_orders": []},
            {"selected_account_id": "DUM882026", "open_order_count": 0, "open_orders": []},
        ]
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._refresh_open_orders_snapshot",
        lambda **kwargs: next(snapshots),
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._snapshot_digest",
        lambda snapshot: "baseline",
    )

    audit_events: list[dict[str, object]] = []
    _execute_submit_cancel_lifecycle(
        config=_config(submit=True, approval_digest="digest", approval_phrase="phrase"),
        runtime=runtime,
        context=_context(),
        requested_order=_requested_order(),
        sleep_fn=lambda _: None,
        audit_events=audit_events,
        preview_payload={},
        preview_digest="digest",
        manual_confirmation_fn=lambda **kwargs: {
            "state": _MANUAL_CONFIRMATION_WAIT_STATE,
            "operator_outcome": "timeout",
            "response_text": None,
            "detail": "Timed out waiting for operator response.",
            "timed_out": True,
        },
    )

    assert "manual_confirmation_wait_started" in _event_types(audit_events)
    assert "manual_confirmation_response_recorded" in _event_types(audit_events)


def test_frozen_preview_payload_does_not_change_on_submit(monkeypatch, tmp_path: Path) -> None:
    preview_artifacts, frozen_path = _preview_bundle(monkeypatch, tmp_path, test_mode=_FILL_TEST_MODE, limit_price=None)
    frozen_bundle = json.loads(frozen_path.read_text(encoding="utf-8"))
    captured: dict[str, object] = {}

    def _fake_lifecycle(**kwargs):
        captured["preview_payload"] = kwargs["preview_payload"]
        captured["requested_order"] = kwargs["requested_order"]
        return {
            "status": "filled",
            "detail": "filled",
            "open_order_after_submit": {"open_orders": []},
            "open_order_after_cancel": {"status": "not_run"},
            "latest_order_status": {"status": "Filled", "order_id": 1},
            "fill_verification": {"verified": True},
            "executions_after_submit": [],
            "completed_orders_after_submit": [],
            "positions_after_submit": {"position_count": 0},
        }

    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._execute_submit_cancel_lifecycle",
        _fake_lifecycle,
    )

    submit_artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(
            submit=True,
            approval_digest=str(preview_artifacts.report["preview"]["preview_digest"]),
            approval_phrase=str(preview_artifacts.report["preview"]["expected_approval_phrase"]),
            output_dir=tmp_path,
            frozen_preview_path=frozen_path,
            test_mode=_FILL_TEST_MODE,
            limit_price=None,
        ),
        stack_provider=_manual_stack,
    )

    assert submit_artifacts.classification == "PAPER_ORDER_FILLED"
    assert captured["preview_payload"] == frozen_bundle["preview_payload"]
    assert captured["requested_order"] == frozen_bundle["requested_order"]
    assert json.loads(frozen_path.read_text(encoding="utf-8")) == frozen_bundle


def test_stale_approval_fails_if_preview_payload_changes(monkeypatch, tmp_path: Path) -> None:
    preview_artifacts, frozen_path = _preview_bundle(monkeypatch, tmp_path, test_mode=_FILL_TEST_MODE, limit_price=None)
    original_digest = str(preview_artifacts.report["preview"]["preview_digest"])
    original_phrase = str(preview_artifacts.report["preview"]["expected_approval_phrase"])
    frozen_bundle = json.loads(frozen_path.read_text(encoding="utf-8"))
    frozen_bundle["requested_order"]["limit_price"] = 4640.2
    frozen_bundle["preview_payload"]["hypothetical_order"]["limit_price"] = 4640.2
    from mgc_v05l.execution.ibkr_paper_order_preview import build_preview_digest as _build_preview_digest

    new_digest = _build_preview_digest(frozen_bundle["preview_payload"])
    frozen_bundle["preview_digest"] = new_digest
    frozen_bundle["expected_approval_phrase"] = build_submit_approval_phrase(
        selected_account_id="DUM882026",
        digest=new_digest,
        requested_order=frozen_bundle["requested_order"],
        delayed_quote_warning="Live market data is unavailable in this paper session. The preview uses delayed data only.",
        test_mode=_FILL_TEST_MODE,
    )
    frozen_path.write_text(json.dumps(frozen_bundle, indent=2, sort_keys=True), encoding="utf-8")

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(
            submit=True,
            approval_digest=original_digest,
            approval_phrase=original_phrase,
            output_dir=tmp_path,
            frozen_preview_path=frozen_path,
            test_mode=_FILL_TEST_MODE,
            limit_price=None,
        ),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_FILL_TEST_BLOCKED"
    assert artifacts.report["submit_cancel_lifecycle"]["status"] == "approval_blocked"


def test_paper_fill_test_uses_ask_side_marketable_limit_for_buy(monkeypatch, tmp_path: Path) -> None:
    preview_artifacts, frozen_path = _preview_bundle(monkeypatch, tmp_path, test_mode=_FILL_TEST_MODE, limit_price=None)
    preview = preview_artifacts.report["preview"]

    assert preview_artifacts.classification == "IBKR_MANUAL_PAPER_FILL_TEST_PARTIAL"
    assert preview["reference_price_source"] == "ask_price"
    assert preview["limit_price"] == 4640.1
    assert round(float(preview["distance_from_quote"]), 4) == 0.1
    assert preview["pricing_label"] == "MARKETABLE_LIMIT_INTENDED_TO_FILL_IN_PAPER"
    assert preview["intended_to_fill"] is True
    assert artifact_stem_for_test_mode(_FILL_TEST_MODE) in str(frozen_path)


def test_fill_timeout_invokes_cancel_path(monkeypatch) -> None:
    runtime = _fake_runtime(latest_order_status={"status": "Submitted", "order_id": 1, "perm_id": 999001})
    refresh_rows = [
        {
            "provider_snapshot": {},
            "positions": {"position_count": 0},
            "open_orders": {"selected_account_id": "DUM882026", "open_order_count": 1, "open_orders": [{"broker_order_id": 1, "perm_id": 999001, "status": "Submitted"}]},
            "executions": [],
            "completed_orders": [],
        }
    ]
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._refresh_execution_truth",
        lambda **kwargs: refresh_rows[0],
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._refresh_open_orders_snapshot",
        lambda **kwargs: refresh_rows[0]["open_orders"],
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._snapshot_digest",
        lambda snapshot: "baseline",
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._cancel_and_verify_visible_order",
        lambda **kwargs: (
            kwargs["audit_events"].append({"event_type": "cancel_requested"}),
            kwargs["audit_events"].append({"event_type": "cancel_verified"}),
            {
                "detail": "Cancelled successfully.",
                "open_order_after_submit": kwargs["after_submit"],
                "open_order_after_cancel": {"selected_account_id": "DUM882026", "open_order_count": 0, "open_orders": []},
                "latest_order_status": {"status": "Cancelled", "perm_id": 999001},
                "cancel_verification": {"verified": True, "final_status": "Cancelled", "detail": "Cancel verified."},
            },
        )[-1],
    )

    audit_events: list[dict[str, object]] = []
    result = _execute_submit_cancel_lifecycle(
        config=_config(
            submit=True,
            approval_digest="digest",
            approval_phrase="phrase",
            test_mode=_FILL_TEST_MODE,
            limit_price=None,
            fill_timeout_seconds=0.01,
        ),
        runtime=runtime,
        context=_context(),
        requested_order={**_requested_order(), "limit_price": 4640.1},
        sleep_fn=lambda _: None,
        audit_events=audit_events,
        preview_payload={},
        preview_digest="digest",
        manual_confirmation_fn=lambda **kwargs: {
            "state": _MANUAL_CONFIRMATION_WAIT_STATE,
            "operator_outcome": "approved",
            "response_text": "approved",
            "detail": "Operator approved the dialog.",
            "timed_out": False,
        },
    )

    assert result["status"] == "fill_timeout_cancelled"
    assert "fill_verification_timeout" in _event_types(audit_events)
    assert "cancel_requested" in _event_types(audit_events)
    assert "cancel_verified" in _event_types(audit_events)


def _patch_harness_context(monkeypatch, context_override: dict[str, object] | None = None) -> None:
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._build_runtime",
        lambda **_: SimpleNamespace(
            transport=SimpleNamespace(connect=lambda: None, disconnect=lambda: None),
            session=SimpleNamespace(state=SimpleNamespace(connected=True)),
            collector=SimpleNamespace(latest_error=lambda **kwargs: None, errors=[]),
            client=SimpleNamespace(),
        ),
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._start_runtime",
        lambda runtime: None,
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._wait_for_connection_ready",
        lambda **_: True,
    )
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._collect_truth_and_preview_context",
        lambda **_: _merge_context_override(_context(), context_override or {}),
    )


def _context() -> dict[str, object]:
    return {
        "selected_account_id": "DUM882026",
        "connection_check": {
            "connected": True,
            "host": "127.0.0.1",
            "port": 7497,
            "client_id": 9074,
            "server_version": 157,
            "connection_timestamp": "2026-04-28T12:00:00+00:00",
            "tws_connection_time": "b'20260428 08:00:00 EST'",
        },
        "account_truth": {
            "account_summary_available_fields": ["BuyingPower"],
        },
        "positions": {
            "position_count": 0,
        },
        "open_orders_before": {
            "ok": True,
            "open_order_count": 0,
            "open_orders": [],
            "selected_account_id": "DUM882026",
        },
        "contract_report": {
            "ok": True,
            "qualified_contract_identifier": 712565978,
            "qualified_contract_object": SimpleNamespace(
                broker_symbol="MGC",
                security_type="FUT",
                exchange="COMEX",
                currency="USD",
                expiry="202606",
                multiplier="10",
                trading_class="MGC",
                con_id=712565978,
            ),
            "qualified_contract": {
                "internal_symbol": "MGC",
                "broker_symbol": "MGC",
                "local_symbol": "MGCM26",
                "security_type": "FUT",
                "exchange": "COMEX",
                "currency": "USD",
                "expiry": "202606",
                "multiplier": "10",
                "trading_class": "MGC",
                "con_id": 712565978,
                "metadata": {"contract_month": "202606"},
            },
            "api_contract_details": [
                {
                    "con_id": 712565978,
                    "exchange": "COMEX",
                    "currency": "USD",
                    "local_symbol": "MGCM6",
                    "multiplier": "10",
                    "trading_class": "MGC",
                    "min_tick": 0.1,
                }
            ],
        },
        "quote_context": {
            **_quote_context(),
        },
        "errors": [],
        "audit_events": [],
        "open_order_baseline_digest": "baseline",
    }


def _config(
    *,
    port: int = 7497,
    symbol: str = "MGC",
    quantity: float = 1.0,
    order_type: str = "LMT",
    limit_price: float | None = 4639.7,
    caller_path: str = "manual_cli",
    submit: bool = False,
    approval_digest: str | None = None,
    approval_phrase: str | None = None,
    test_mode: str = "PAPER_RESTING_TEST",
    output_dir: Path | None = None,
    frozen_preview_path: Path | None = None,
    fill_timeout_seconds: float = 8.0,
) -> IbkrManualPaperSubmitConfig:
    return IbkrManualPaperSubmitConfig(
        repo_root=Path("."),
        mode="PAPER",
        host="127.0.0.1",
        port=port,
        client_id=9074,
        account_id="DUM882026",
        symbol=symbol,
        expiry="202606",
        action="BUY",
        quantity=quantity,
        order_type=order_type,
        limit_price=limit_price,
        time_in_force="DAY",
        test_mode=test_mode,
        fill_timeout_seconds=fill_timeout_seconds,
        caller_path=caller_path,
        submit=submit,
        approval_digest=approval_digest,
        approval_phrase=approval_phrase,
        output_dir=output_dir,
        frozen_preview_path=frozen_preview_path,
    )


def _preview_bundle(
    monkeypatch,
    tmp_path: Path,
    *,
    test_mode: str = "PAPER_RESTING_TEST",
    limit_price: float | None = 4639.7,
) -> tuple[IbkrManualPaperSubmitArtifacts, Path]:
    _patch_harness_context(monkeypatch)
    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(
            submit=False,
            test_mode=test_mode,
            limit_price=limit_price,
            output_dir=tmp_path,
        ),
        stack_provider=_manual_stack,
    )
    write_ibkr_manual_paper_submit_artifacts(output_dir=tmp_path, artifacts=artifacts)
    frozen_path = tmp_path / f"{artifact_stem_for_test_mode(test_mode)}_frozen_preview.json"
    assert frozen_path.exists()
    return artifacts, frozen_path


def _manual_stack() -> list[SimpleNamespace]:
    return [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.app.ibkr_manual_paper_submit"}))]


def _requested_order() -> dict[str, object]:
    return {
        "symbol": "MGC",
        "expiry": "202606",
        "action": "BUY",
        "quantity": 1.0,
        "order_type": "LMT",
        "limit_price": 4639.7,
        "time_in_force": "DAY",
    }


def _event_types(audit_events: list[dict[str, object]]) -> list[str]:
    return [str(row["event_type"]) for row in audit_events]


class _FakeTransport:
    def __init__(self) -> None:
        self.place_calls: list[dict[str, object]] = []
        self.cancel_calls: list[int] = []

    def place_limit_order(self, **kwargs) -> None:
        self.place_calls.append(dict(kwargs))

    def cancel_order(self, *, order_id: int) -> None:
        self.cancel_calls.append(int(order_id))


class _FakeCollector:
    def __init__(self, latest_order_status: dict[str, object] | None = None) -> None:
        self._latest_order_status = latest_order_status
        self.errors: list[dict[str, object]] = []

    def reset_order_status_event(self, order_id: int) -> None:
        _ = order_id

    def latest_order_status(self, order_id: int) -> dict[str, object] | None:
        _ = order_id
        return self._latest_order_status


def _fake_runtime(*, latest_order_status: dict[str, object] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        session=SimpleNamespace(allocate_order_id=lambda: 1),
        collector=_FakeCollector(latest_order_status=latest_order_status),
        transport=_FakeTransport(),
        client=SimpleNamespace(request_open_orders=lambda: None),
    )


def _quote_context(
    *,
    has_quote: bool = True,
    updated_at: str | None = None,
    bid_price: float | None = 4639.8,
    ask_price: float | None = 4640.0,
    last_price: float | None = 4640.0,
    close_price: float | None = 4693.7,
) -> dict[str, object]:
    if updated_at is None:
        updated_at = datetime.now(timezone.utc).isoformat()
    return {
        "quote_source_label": "DELAYED" if has_quote else "UNAVAILABLE",
        "live_market_data_warning": "Live market data is unavailable in this paper session. The preview uses delayed data only.",
        "bid_price": bid_price,
        "ask_price": ask_price,
        "last_price": last_price,
        "close_price": close_price,
        "live_market_data_available": False,
        "has_quote": has_quote,
        "delayed_data_warning_present": True,
        "updated_at": updated_at,
        "response_indication": "delayed_only" if has_quote else "unavailable",
    }


def _merge_context_override(base: dict[str, object], override: dict[str, object]) -> dict[str, object]:
    merged = dict(base)
    for key, value in override.items():
        merged[key] = value
    return merged
