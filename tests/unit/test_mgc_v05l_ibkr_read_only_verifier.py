from __future__ import annotations

import json
import threading
from pathlib import Path

from mgc_v05l.execution.ibkr_read_only_verifier import (
    IbkrReadOnlyVerificationArtifacts,
    IbkrReadOnlyVerificationConfig,
    _wait_for_connection_ready,
    _wait_for_event,
    evaluate_ibkr_environment_lock,
    render_ibkr_read_only_connection_report_markdown,
    verify_ibkr_read_only_connection,
    write_ibkr_read_only_artifacts,
)


def test_environment_lock_paper_7497_passes() -> None:
    result = evaluate_ibkr_environment_lock(
        mode="PAPER",
        host="127.0.0.1",
        port=7497,
        read_only=True,
    )

    assert result["passed"] is True
    assert result["fail_closed"] is False
    assert result["mismatches"] == []


def test_environment_lock_paper_7496_fails_closed() -> None:
    result = evaluate_ibkr_environment_lock(
        mode="PAPER",
        host="127.0.0.1",
        port=7496,
        read_only=True,
    )

    assert result["passed"] is False
    assert result["fail_closed"] is True
    assert result["mismatches"] == ["port"]


def test_environment_lock_live_7497_fails_closed() -> None:
    result = evaluate_ibkr_environment_lock(
        mode="LIVE",
        host="127.0.0.1",
        port=7497,
        read_only=True,
    )

    assert result["passed"] is False
    assert result["fail_closed"] is True
    assert result["mismatches"] == ["mode"]


def test_environment_lock_read_only_false_fails_closed() -> None:
    result = evaluate_ibkr_environment_lock(
        mode="PAPER",
        host="127.0.0.1",
        port=7497,
        read_only=False,
    )

    assert result["passed"] is False
    assert result["fail_closed"] is True
    assert result["mismatches"] == ["read_only"]


def test_write_ibkr_read_only_artifacts_serializes_required_reports(tmp_path: Path) -> None:
    artifacts = IbkrReadOnlyVerificationArtifacts(
        classification="IBKR_READ_ONLY_PARTIAL",
        connection_report={
            "classification": "IBKR_READ_ONLY_PARTIAL",
            "generated_at": "2026-04-28T12:00:00+00:00",
            "connection_check": {
                "host": "127.0.0.1",
                "port": 7497,
                "client_id": 9071,
                "server_version": 180,
            },
            "environment_lock_check": {
                "read_only": True,
                "fail_closed": False,
                "mismatches": [],
                "account_id": "DU1234567",
            },
            "account_truth_check": {"ok": True},
            "position_truth_check": {"ok": True},
            "open_order_truth_check": {"ok": True},
            "contract_qualification_check": {"ok": True},
            "market_data_check": {"ok": False},
            "reconnect_check": {"ok": True},
            "next_manual_check": None,
        },
        account_truth_snapshot={
            "selected_account_id": "DU1234567",
            "account_summary_available_fields": ["BuyingPower", "NetLiquidation"],
        },
        positions_snapshot={
            "selected_account_id": "DU1234567",
            "position_count": 0,
            "ok": True,
            "positions": [],
        },
        open_orders_snapshot={
            "selected_account_id": "DU1234567",
            "open_order_count": 0,
            "ok": True,
            "open_orders": [],
        },
        contract_qualification_report={
            "ok": True,
            "contracts": [],
        },
        market_data_probe_report={
            "ok": False,
            "status": "permission_missing",
        },
    )

    write_ibkr_read_only_artifacts(output_dir=tmp_path, artifacts=artifacts)

    expected_files = {
        "ibkr_read_only_connection_report.json",
        "ibkr_read_only_connection_report.md",
        "ibkr_account_truth_snapshot.json",
        "ibkr_positions_snapshot.json",
        "ibkr_open_orders_snapshot.json",
        "ibkr_contract_qualification_report.json",
        "ibkr_market_data_probe_report.json",
    }
    assert expected_files.issubset({path.name for path in tmp_path.iterdir()})
    report = json.loads((tmp_path / "ibkr_read_only_connection_report.json").read_text(encoding="utf-8"))
    assert report["classification"] == "IBKR_READ_ONLY_PARTIAL"
    markdown = (tmp_path / "ibkr_read_only_connection_report.md").read_text(encoding="utf-8")
    assert "IBKR Read-Only Connection Report" in markdown


def test_read_only_verifier_uses_api_aligned_non_mutating_position_and_order_requests() -> None:
    source = Path("src/mgc_v05l/execution/ibkr_read_only_verifier.py").read_text(encoding="utf-8")

    assert "reqPositions()" in source
    assert "positionEnd" in source
    assert "reqAllOpenOrders()" in source
    assert "openOrderEnd" in source
    assert "reqOpenOrders" not in source
    assert "reqAutoOpenOrders" not in source
    assert "placeOrder" not in source
    assert "cancelOrder" not in source
    assert "reqGlobalCancel" not in source
    assert '"positions_complete": True' in source
    assert '"open_orders_complete": True' in source
    assert '"order_binding_requested": False' in source
    assert '"auto_open_orders_requested": False' in source


def test_partial_market_data_summary_is_rendered_clearly() -> None:
    markdown = render_ibkr_read_only_connection_report_markdown(
        {
            "classification": "IBKR_READ_ONLY_PARTIAL",
            "generated_at": "2026-04-28T12:00:00+00:00",
            "connection_check": {
                "host": "127.0.0.1",
                "port": 7497,
                "client_id": 9071,
                "server_version": 157,
            },
            "environment_lock_check": {
                "configured_mode": "PAPER",
                "configured_host": "127.0.0.1",
                "configured_port": 7497,
                "read_only": True,
                "fail_closed": False,
                "mismatches": [],
                "account_id": "DUM882026",
            },
            "account_truth_check": {"ok": True},
            "position_truth_check": {"ok": True},
            "open_order_truth_check": {"ok": True},
            "contract_qualification_check": {
                "ok": True,
                "contracts": [
                    {"symbol": "GC", "requested_expiry": "202606"},
                    {"symbol": "MGC", "requested_expiry": "202606"},
                ],
            },
            "duplicate_client_id_check": {"safe": True},
            "market_data_check": {
                "ok": False,
                "detail": "Market data permission unavailable: Requested market data is not subscribed. Displaying delayed market data.",
                "errors": [{"code": 10167}],
            },
            "reconnect_check": {"ok": True},
            "next_manual_check": None,
        }
    )

    assert "classification: IBKR_READ_ONLY_PARTIAL" in markdown
    assert "the repo safely connected to TWS paper on 127.0.0.1:7497" in markdown
    assert "environment lock was mode=PAPER, host=127.0.0.1, port=7497, read_only=true" in markdown
    assert "no orders were placed" in markdown
    assert "no orders were staged" in markdown
    assert "no strategy execution was connected" in markdown
    assert "account truth was read successfully: True" in markdown
    assert "positions were read successfully: True" in markdown
    assert "open orders were read successfully: True" in markdown
    assert "GC 202606, MGC 202606 contracts qualified successfully" in markdown
    assert "reconnect behavior passed: True" in markdown
    assert "duplicate client ID was safely rejected: True" in markdown
    assert "market data remained partial due to TWS response 10167 / no snapshot ticks" in markdown
    assert "market data failure did not invalidate read-only truth verification" in markdown


def test_verify_handles_unavailable_tws_gracefully() -> None:
    class UnavailableTransport:
        def __init__(self, **kwargs) -> None:
            del kwargs

        def connect(self) -> None:
            raise ConnectionRefusedError("Connection refused")

    artifacts = verify_ibkr_read_only_connection(
        config=IbkrReadOnlyVerificationConfig(
            repo_root=Path("."),
            mode="PAPER",
            host="127.0.0.1",
            port=7497,
            client_id=9071,
            read_only=True,
            probe_market_data=False,
            probe_duplicate_client_id=False,
            timeout_seconds=0.1,
        ),
        transport_factory=UnavailableTransport,
        sleep_fn=lambda seconds: None,
    )

    assert artifacts.classification == "IBKR_READ_ONLY_BLOCKED"
    assert artifacts.connection_report["account_truth_check"]["ok"] is False
    assert "Verify TWS paper is running" in artifacts.connection_report["next_manual_check"]


def test_connection_ready_wait_allows_late_next_valid_id_after_transient_502() -> None:
    class Collector:
        def __init__(self) -> None:
            self.next_valid_id_ready = threading.Event()
            self.polls = 0

        def latest_error(self, *, codes=None):  # noqa: ANN001
            del codes
            return {"code": 502, "message": "Couldn't connect to TWS"}

    class Transport:
        def is_connected(self) -> bool:
            return True

    collector = Collector()

    def sleep_fn(seconds: float) -> None:
        del seconds
        collector.polls += 1
        collector.next_valid_id_ready.set()

    assert _wait_for_connection_ready(
        transport=Transport(),
        collector=collector,  # type: ignore[arg-type]
        timeout_seconds=0.1,
        sleep_fn=sleep_fn,
    )


def test_connection_ready_wait_still_fails_when_502_and_transport_disconnected() -> None:
    class Collector:
        next_valid_id_ready = threading.Event()

        def latest_error(self, *, codes=None):  # noqa: ANN001
            del codes
            return {"code": 502, "message": "Couldn't connect to TWS"}

    class Transport:
        def is_connected(self) -> bool:
            return False

    assert not _wait_for_connection_ready(
        transport=Transport(),
        collector=Collector(),  # type: ignore[arg-type]
        timeout_seconds=0.1,
        sleep_fn=lambda seconds: None,
    )


def test_event_wait_allows_late_completion_after_transient_502() -> None:
    class Collector:
        def __init__(self) -> None:
            self.polls = 0

        def latest_error(self, *, codes=None):  # noqa: ANN001
            del codes
            return {"code": 502, "message": "Couldn't connect to TWS"}

    event = threading.Event()
    collector = Collector()

    def sleep_fn(seconds: float) -> None:
        del seconds
        collector.polls += 1
        event.set()

    assert _wait_for_event(
        event,
        collector=collector,  # type: ignore[arg-type]
        timeout_seconds=0.1,
        sleep_fn=sleep_fn,
    )
