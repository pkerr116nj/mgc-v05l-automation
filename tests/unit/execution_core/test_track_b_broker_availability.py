from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

from mgc_v05l.execution_core.track_b_broker_availability import (
    BrokerAvailabilityReportConfig,
    build_broker_availability_report,
)


NOW = datetime(2026, 6, 9, 14, 30, tzinfo=UTC)


def test_successful_paper_read_only_connection_is_available(tmp_path: Path) -> None:
    report = _report(tmp_path)

    assert report["classification"] == "BROKER_AVAILABLE"
    assert report["execution_domain"] == "TRACK_B_PAPER"
    assert report["account_id"] == "DUM882026"
    assert report["connected"] is True
    assert report["account_visible"] is True
    assert report["positions_readable"] is True
    assert report["open_orders_readable"] is True
    assert report["retryable"] is False


def test_ibkr_502_is_retryable_broker_unavailable(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_truth_status={
            "classification": "BROKER_TRUTH_REFRESH_FAILED",
            "generated_at": NOW.isoformat(),
            "last_failure": True,
            "last_success": False,
            "last_error": "TWS paper API error 502: Couldn't connect to TWS.",
            "mode": "PAPER",
            "account": "DUM882026",
        },
        connection_report={
            "classification": "IBKR_READ_ONLY_BLOCKED",
            "generated_at": NOW.isoformat(),
            "detail": "TWS paper API error 502: Couldn't connect to TWS.",
        },
        positions_snapshot={},
        open_orders_snapshot={},
    )

    assert report["classification"] == "BROKER_UNAVAILABLE_RETRYABLE"
    assert report["connected"] is False
    assert report["failure_code"] == "502"
    assert "502" in report["failure_message"]
    assert report["retryable"] is True


def test_wrong_account_is_fatal(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_truth_status=_broker_status(account="OTHER"),
        positions_snapshot=_positions_snapshot(account="OTHER"),
        open_orders_snapshot=_open_orders_snapshot(account="OTHER"),
    )

    assert report["classification"] == "BROKER_UNAVAILABLE_FATAL"
    assert report["retryable"] is False
    assert "expected account DUM882026" in report["failure_message"]


def test_stale_or_missing_artifact_is_unknown_not_strategy_failure(tmp_path: Path) -> None:
    stale_time = NOW - timedelta(minutes=10)
    report = _report(
        tmp_path,
        broker_truth_status=_broker_status(generated_at=stale_time),
        connection_report={"classification": "IBKR_READ_ONLY_CONNECTED", "generated_at": stale_time.isoformat()},
        positions_snapshot=_positions_snapshot(generated_at=stale_time),
        open_orders_snapshot=_open_orders_snapshot(generated_at=stale_time),
    )

    assert report["classification"] == "BROKER_AVAILABILITY_UNKNOWN"
    assert report["retryable"] is True
    assert report["strategy_authority_evaluated"] is False
    assert report["strategy_authority_failed"] is False
    assert report["entry_authority_failed"] is False
    assert report["exit_authority_failed"] is False
    assert report["lifecycle_validation_failed"] is False


def test_live_account_detected_in_paper_domain_is_fatal(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        connection_report={
            "classification": "IBKR_READ_ONLY_CONNECTED",
            "generated_at": NOW.isoformat(),
            "environment_lock_check": {"account_type": "LIVE"},
        },
    )

    assert report["classification"] == "BROKER_UNAVAILABLE_FATAL"
    assert report["retryable"] is False
    assert report["failure_message"] == "live account detected in TRACK_B_PAPER broker availability check"


def test_502_does_not_mark_strategy_or_lifecycle_authority_failed(tmp_path: Path) -> None:
    report = _report(
        tmp_path,
        broker_truth_status={
            "classification": "BROKER_TRUTH_REFRESH_FAILED",
            "generated_at": NOW.isoformat(),
            "last_failure": True,
            "last_success": False,
            "latest_error": {"code": 502, "message": "Couldn't connect to TWS."},
            "mode": "PAPER",
            "account": "DUM882026",
        },
        connection_report={"classification": "IBKR_READ_ONLY_BLOCKED", "generated_at": NOW.isoformat()},
        positions_snapshot={},
        open_orders_snapshot={},
    )

    assert report["classification"] == "BROKER_UNAVAILABLE_RETRYABLE"
    assert report["strategy_authority_evaluated"] is False
    assert report["strategy_authority_failed"] is False
    assert report["entry_authority_evaluated"] is False
    assert report["entry_authority_failed"] is False
    assert report["exit_authority_evaluated"] is False
    assert report["exit_authority_failed"] is False
    assert report["lifecycle_validation_failed"] is False


def _report(
    tmp_path: Path,
    *,
    broker_truth_status: dict | None = None,
    connection_report: dict | None = None,
    positions_snapshot: dict | None = None,
    open_orders_snapshot: dict | None = None,
) -> dict:
    config = BrokerAvailabilityReportConfig(repo_root=tmp_path)
    return build_broker_availability_report(
        config=config,
        now=NOW,
        input_overrides={
            "broker_truth_status": broker_truth_status or _broker_status(),
            "connection_report": connection_report or _connection_report(),
            "positions_snapshot": positions_snapshot or _positions_snapshot(),
            "open_orders_snapshot": open_orders_snapshot or _open_orders_snapshot(),
        },
    )


def _broker_status(*, account: str = "DUM882026", generated_at: datetime = NOW) -> dict:
    return {
        "account": account,
        "classification": "BROKER_TRUTH_REFRESH_READY",
        "client_id": 9077,
        "generated_at": generated_at.isoformat(),
        "host": "127.0.0.1",
        "last_success": True,
        "last_success_at": generated_at.isoformat(),
        "mode": "PAPER",
        "open_order_count": 0,
        "open_orders_complete": True,
        "port": 7497,
        "position_count": 2,
        "positions_complete": True,
        "read_only": True,
        "verifier_classification": "IBKR_READ_ONLY_CONNECTED",
    }


def _connection_report(*, generated_at: datetime = NOW) -> dict:
    return {
        "classification": "IBKR_READ_ONLY_CONNECTED",
        "generated_at": generated_at.isoformat(),
    }


def _positions_snapshot(*, account: str = "DUM882026", generated_at: datetime = NOW) -> dict:
    return {
        "account": account,
        "client_id": 9077,
        "generated_at": generated_at.isoformat(),
        "host": "127.0.0.1",
        "port": 7497,
        "position_count": 2,
        "positions": [
            {"account_id": account, "local_symbol": "MESM6", "quantity": "0.0", "security_type": "FUT"},
            {"account_id": account, "local_symbol": "MNQM6", "quantity": "0.0", "security_type": "FUT"},
        ],
        "read_only": True,
        "selected_account_id": account,
    }


def _open_orders_snapshot(*, account: str = "DUM882026", generated_at: datetime = NOW) -> dict:
    return {
        "account": account,
        "client_id": 9077,
        "generated_at": generated_at.isoformat(),
        "host": "127.0.0.1",
        "open_order_count": 0,
        "open_orders": [],
        "port": 7497,
        "read_only": True,
        "selected_account_id": account,
    }
