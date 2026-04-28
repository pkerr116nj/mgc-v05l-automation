from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

from mgc_v05l.execution.ibkr_paper_order_preview import (
    IbkrPaperOrderPreviewArtifacts,
    IbkrPaperOrderPreviewConfig,
    build_preview_digest,
    render_ibkr_paper_order_preview_markdown,
    run_ibkr_paper_order_preview,
    write_ibkr_paper_order_preview_artifacts,
)


def test_paper_preview_passes_with_manual_cli_and_mocked_runtime(monkeypatch) -> None:
    monkeypatch.setattr(
        "mgc_v05l.execution.ibkr_paper_order_preview._collect_preview_runtime_context",
        lambda **_: {
            "connection_check": {
                "connected": True,
                "host": "127.0.0.1",
                "port": 7497,
                "client_id": 9073,
                "server_version": 157,
                "connection_timestamp": "2026-04-28T12:00:00+00:00",
                "tws_connection_time": "b'20260428 08:00:00 EST'",
            },
            "selected_account_id": "DUM882026",
            "open_orders_snapshot": {
                "ok": True,
                "open_order_count": 0,
                "open_orders": [],
            },
            "contract_report": {
                "ok": True,
                "qualified_contract_identifier": 712565978,
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
                "detail": "Contract details received from TWS.",
            },
            "quote_context": {
                "has_quote": True,
                "quote_source_label": "DELAYED",
                "live_market_data_available": False,
                "live_market_data_warning": "Live market data is unavailable in this paper session. The preview uses delayed data only.",
                "delayed_data_warning_present": True,
                "bid_price": 4642.6,
                "ask_price": 4642.8,
                "last_price": 4642.6,
                "close_price": 4693.7,
            },
            "errors": [],
        },
    )

    artifacts = run_ibkr_paper_order_preview(
        config=IbkrPaperOrderPreviewConfig(
            repo_root=Path("."),
            mode="PAPER",
            host="127.0.0.1",
            port=7497,
            client_id=9073,
            account_id="DUM882026",
            symbol="MGC",
            expiry="202606",
            action="BUY",
            quantity=1.0,
            order_type="LMT",
            limit_price=4500.0,
            time_in_force="DAY",
            caller_path="manual_cli",
        ),
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.app.ibkr_paper_order_preview"}))],
    )

    assert artifacts.classification == "IBKR_PAPER_ORDER_PREVIEW_READY"
    assert artifacts.report["preview_digest"]
    assert artifacts.report["no_submit_guarantee"]["submitted"] is False


def test_live_port_7496_fails_closed() -> None:
    artifacts = run_ibkr_paper_order_preview(
        config=_config(port=7496),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_PAPER_ORDER_PREVIEW_BLOCKED"
    assert "7496" in artifacts.report["environment_lock_check"]["port_policy"]


def test_ib_gateway_live_port_4001_fails_closed() -> None:
    artifacts = run_ibkr_paper_order_preview(
        config=_config(port=4001),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_PAPER_ORDER_PREVIEW_BLOCKED"
    assert "4001" in artifacts.report["environment_lock_check"]["port_policy"]


def test_unknown_port_fails_closed() -> None:
    artifacts = run_ibkr_paper_order_preview(
        config=_config(port=7555),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_PAPER_ORDER_PREVIEW_BLOCKED"
    assert "Unknown port" in artifacts.report["environment_lock_check"]["port_policy"]


def test_non_whitelisted_contract_fails_closed() -> None:
    artifacts = run_ibkr_paper_order_preview(
        config=_config(symbol="MES"),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_PAPER_ORDER_PREVIEW_BLOCKED"
    failed = {row["name"]: row for row in artifacts.report["guardrail_checks"]}
    assert failed["whitelisted_contract"]["passed"] is False


def test_qty_greater_than_one_fails_closed() -> None:
    artifacts = run_ibkr_paper_order_preview(
        config=_config(quantity=2.0),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_PAPER_ORDER_PREVIEW_BLOCKED"
    failed = {row["name"]: row for row in artifacts.report["guardrail_checks"]}
    assert failed["quantity_cap"]["passed"] is False


def test_market_order_fails_closed() -> None:
    artifacts = run_ibkr_paper_order_preview(
        config=_config(order_type="MKT"),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_PAPER_ORDER_PREVIEW_BLOCKED"
    failed = {row["name"]: row for row in artifacts.report["guardrail_checks"]}
    assert failed["limit_only_order_type"]["passed"] is False


def test_missing_limit_price_fails_closed() -> None:
    artifacts = run_ibkr_paper_order_preview(
        config=_config(limit_price=None),
        stack_provider=_manual_stack,
    )

    assert artifacts.classification == "IBKR_PAPER_ORDER_PREVIEW_BLOCKED"
    failed = {row["name"]: row for row in artifacts.report["guardrail_checks"]}
    assert failed["limit_price_present"]["passed"] is False


def test_strategy_caller_path_cannot_access_preview_harness() -> None:
    artifacts = run_ibkr_paper_order_preview(
        config=_config(caller_path="mgc_v05l.strategy.preview"),
        stack_provider=lambda: [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.strategy.strategy_engine"}))],
    )

    assert artifacts.classification == "IBKR_PAPER_ORDER_PREVIEW_BLOCKED"
    assert artifacts.report["manual_caller_check"]["passed"] is False


def test_preview_digest_is_deterministic_for_same_payload() -> None:
    payload = {
        "account_id": "DUM882026",
        "environment": {"mode": "PAPER", "host": "127.0.0.1", "port": 7497, "client_id": 9073},
        "contract": {"symbol": "MGC", "expiry": "202606"},
        "hypothetical_order": {"action": "BUY", "quantity": 1.0, "order_type": "LMT", "limit_price": 4500.0, "time_in_force": "DAY"},
        "quote_context": {"quote_source_label": "DELAYED", "live_market_data_available": False},
        "estimated_tick_value": 1.0,
        "estimated_notional": 45000.0,
        "guardrails": [{"name": "paper_tws_environment_lock", "passed": True, "blocking": True}],
        "no_submit_guarantee": {"submitted": False, "staged": False, "transmitted": False},
    }

    assert build_preview_digest(payload) == build_preview_digest(payload)


def test_preview_report_serialization_writes_json_markdown_and_audit(tmp_path: Path) -> None:
    artifacts = IbkrPaperOrderPreviewArtifacts(
        classification="IBKR_PAPER_ORDER_PREVIEW_READY",
        report={
            "classification": "IBKR_PAPER_ORDER_PREVIEW_READY",
            "generated_at": "2026-04-28T12:00:00+00:00",
            "account_id": "DUM882026",
            "connection_check": {
                "client_id": 9073,
            },
            "manual_caller_check": {
                "caller_path": "manual_cli",
                "forbidden_callers_detected": [],
            },
            "environment_lock_check": {
                "configured_mode": "PAPER",
                "configured_host": "127.0.0.1",
                "configured_port": 7497,
            },
            "contract_preview": {
                "symbol": "MGC",
                "expiry": "202606",
                "qualified_contract_identifier": 712565978,
            },
            "hypothetical_order": {
                "action": "BUY",
                "quantity": 1.0,
                "order_type": "LMT",
                "limit_price": 4500.0,
                "time_in_force": "DAY",
            },
            "quote_context": {
                "quote_source_label": "DELAYED",
                "live_market_data_available": False,
                "live_market_data_warning": "Delayed only.",
            },
            "estimated_notional": 45000.0,
            "estimated_tick_value": 1.0,
            "guardrail_checks": [
                {"name": "paper_tws_environment_lock", "passed": True, "blocking": True, "detail": "ok"},
            ],
            "preview_digest": "abc123",
            "no_submit_guarantee": {
                "submitted": False,
                "staged": False,
                "transmitted": False,
                "detail": "Preview only.",
            },
        },
        audit_entry={
            "event_type": "preview_generated",
            "preview_digest": "abc123",
            "submitted": False,
            "staged": False,
            "transmitted": False,
        },
    )

    write_ibkr_paper_order_preview_artifacts(output_dir=tmp_path, artifacts=artifacts)

    assert (tmp_path / "ibkr_paper_order_preview_report.json").exists()
    assert (tmp_path / "ibkr_paper_order_preview_report.md").exists()
    assert (tmp_path / "ibkr_paper_order_preview_audit.jsonl").exists()
    payload = json.loads((tmp_path / "ibkr_paper_order_preview_report.json").read_text(encoding="utf-8"))
    assert payload["classification"] == "IBKR_PAPER_ORDER_PREVIEW_READY"
    markdown = render_ibkr_paper_order_preview_markdown(payload)
    assert "Preview only." in markdown
    assert "this is preview-only by construction" in markdown
    assert "no submit-capable object is created" in markdown
    assert "manual CLI caller path only" in markdown
    assert "strategy-style callers fail closed" in markdown
    assert "environment lock is PAPER / 127.0.0.1 / 7497 only" in markdown
    assert "live port 7496 fails closed" in markdown
    assert "IB Gateway ports 4001 and 4002 fail closed" in markdown
    assert "unknown ports fail closed" in markdown
    assert "whitelist is GC/MGC 202606 only" in markdown
    assert "qty <= 1" in markdown
    assert "LMT only" in markdown
    assert "DAY only" in markdown
    assert "limit price required" in markdown
    assert "expected-account matching enforced" in markdown
    assert "fresh open-order baseline required" in markdown
    assert "deterministic preview digest generated" in markdown
    assert "audit log written" in markdown
    assert "delayed quote context captured and labeled" in markdown


def _config(
    *,
    port: int = 7497,
    symbol: str = "MGC",
    quantity: float = 1.0,
    order_type: str = "LMT",
    limit_price: float | None = 4500.0,
    caller_path: str = "manual_cli",
) -> IbkrPaperOrderPreviewConfig:
    return IbkrPaperOrderPreviewConfig(
        repo_root=Path("."),
        mode="PAPER",
        host="127.0.0.1",
        port=port,
        client_id=9073,
        account_id="DUM882026",
        symbol=symbol,
        expiry="202606",
        action="BUY",
        quantity=quantity,
        order_type=order_type,
        limit_price=limit_price,
        time_in_force="DAY",
        caller_path=caller_path,
    )


def _manual_stack() -> list[SimpleNamespace]:
    return [SimpleNamespace(frame=SimpleNamespace(f_globals={"__name__": "mgc_v05l.app.ibkr_paper_order_preview"}))]
