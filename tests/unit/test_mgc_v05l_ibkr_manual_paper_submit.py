from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution.ibkr_manual_paper_submit import (
    _MANUAL_CONFIRMATION_WAIT_STATE,
    _execute_submit_cancel_lifecycle,
    IbkrManualPaperSubmitArtifacts,
    IbkrManualPaperSubmitConfig,
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


def test_submit_cannot_occur_without_exact_digest(monkeypatch) -> None:
    _patch_harness_context(monkeypatch)
    expected_phrase = build_submit_approval_phrase(
        selected_account_id="DUM882026",
        digest="wrong-digest",
        requested_order={"symbol": "MGC", "expiry": "202606", "action": "BUY", "quantity": 1.0, "order_type": "LMT", "limit_price": 4500.0, "time_in_force": "DAY"},
        delayed_quote_warning="Live market data is unavailable in this paper session. The preview uses delayed data only.",
    )

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(submit=True, approval_digest="wrong-digest", approval_phrase=expected_phrase),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"
    assert artifacts.report["submit_cancel_lifecycle"]["status"] == "approval_blocked"


def test_submit_cannot_occur_without_typed_phrase(monkeypatch) -> None:
    _patch_harness_context(monkeypatch)

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(submit=True, approval_digest="whatever", approval_phrase=None),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_MANUAL_PAPER_SUBMIT_CANCEL_BLOCKED"
    assert artifacts.report["submit_cancel_lifecycle"]["status"] == "approval_blocked"


def test_digest_mismatch_fails_closed(monkeypatch) -> None:
    _patch_harness_context(monkeypatch)

    artifacts = run_ibkr_manual_paper_submit_test(
        config=_config(submit=True, approval_digest="mismatch", approval_phrase="mismatch"),
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


def _patch_harness_context(monkeypatch) -> None:
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_manual_paper_submit._build_runtime",
        lambda **_: SimpleNamespace(
            transport=SimpleNamespace(connect=lambda: None, disconnect=lambda: None),
            session=SimpleNamespace(state=SimpleNamespace(connected=True)),
            collector=SimpleNamespace(latest_error=lambda **kwargs: None),
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
        lambda **_: _context(),
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
            "quote_source_label": "DELAYED",
            "live_market_data_warning": "Live market data is unavailable in this paper session. The preview uses delayed data only.",
            "bid_price": 4639.8,
            "ask_price": 4640.0,
            "last_price": 4640.0,
            "close_price": 4693.7,
            "live_market_data_available": False,
            "has_quote": True,
            "delayed_data_warning_present": True,
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
    limit_price: float | None = 4500.0,
    caller_path: str = "manual_cli",
    submit: bool = False,
    approval_digest: str | None = None,
    approval_phrase: str | None = None,
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
        caller_path=caller_path,
        submit=submit,
        approval_digest=approval_digest,
        approval_phrase=approval_phrase,
    )


def _manual_stack() -> list[SimpleNamespace]:
    return [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.app.ibkr_manual_paper_submit"}))]


def _requested_order() -> dict[str, object]:
    return {
        "symbol": "MGC",
        "expiry": "202606",
        "action": "BUY",
        "quantity": 1.0,
        "order_type": "LMT",
        "limit_price": 4500.0,
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
