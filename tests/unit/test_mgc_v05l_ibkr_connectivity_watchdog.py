from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.app import ibkr_connectivity_watchdog as watchdog
from mgc_v05l.execution_core.ibkr_readonly_transport import IbkrReadOnlyTimeoutError


def fixed_now() -> datetime:
    return datetime(2026, 5, 18, 12, 0, tzinfo=timezone.utc)


def listening_tcp(**kwargs: object) -> watchdog.TcpProbeResult:
    return watchdog.TcpProbeResult(listening=True, latency_ms=1.25)


def refused_tcp(**kwargs: object) -> watchdog.TcpProbeResult:
    return watchdog.TcpProbeResult(
        listening=False,
        latency_ms=None,
        failure_reason="connection refused for 127.0.0.1:7497",
    )


def config(tmp_path: Path) -> watchdog.IbkrConnectivityWatchdogConfig:
    return watchdog.IbkrConnectivityWatchdogConfig(
        output_path=tmp_path / "latest_ibkr_connectivity_watchdog.json",
        client_id=9811,
        timeout_seconds=0.01,
    )


def run_with_transport(tmp_path: Path, transport: object) -> dict[str, object]:
    return watchdog.run_ibkr_connectivity_watchdog(
        config=config(tmp_path),
        tcp_checker=listening_tcp,
        transport_factory=lambda: transport,
        now_fn=fixed_now,
    )


def test_tcp_refused_classification(tmp_path: Path) -> None:
    report = watchdog.run_ibkr_connectivity_watchdog(
        config=config(tmp_path),
        tcp_checker=refused_tcp,
        transport_factory=lambda: ReadyTransport(),
        now_fn=fixed_now,
    )

    assert report["classification"] == watchdog.TWS_NOT_LISTENING
    assert report["checks"]["tcp_port"]["ok"] is False
    assert report["submit_authority"] is False
    assert report["live_money_eligible"] is False


def test_handshake_success_fixture(tmp_path: Path) -> None:
    report = run_with_transport(tmp_path, ReadyTransport())

    assert report["classification"] == watchdog.CONNECTED_READ_ONLY
    assert report["client_id"] == 9811
    assert report["checks"]["connect_ack"]["ok"] is True
    assert report["checks"]["next_valid_id"]["ok"] is True
    assert report["checks"]["managed_accounts"]["requested_account_present"] is True
    assert report["checks"]["positions_complete"]["ok"] is True
    assert report["checks"]["open_orders_complete"]["ok"] is True


def test_managed_accounts_timeout(tmp_path: Path) -> None:
    report = run_with_transport(tmp_path, TimeoutTransport(timeout_stage="managedAccounts"))

    assert report["classification"] == watchdog.MANAGED_ACCOUNTS_TIMEOUT
    assert report["checks"]["managed_accounts"]["ok"] is False
    assert "managedAccounts" in report["failure_reason"]


def test_positions_timeout(tmp_path: Path) -> None:
    report = run_with_transport(tmp_path, TimeoutTransport(timeout_stage="positionEnd"))

    assert report["classification"] == watchdog.POSITIONS_TIMEOUT
    assert report["checks"]["positions_complete"]["ok"] is False
    assert "positionEnd" in report["failure_reason"]


def test_open_orders_timeout(tmp_path: Path) -> None:
    report = run_with_transport(tmp_path, TimeoutTransport(timeout_stage="openOrderEnd"))

    assert report["classification"] == watchdog.OPEN_ORDERS_TIMEOUT
    assert report["checks"]["open_orders_complete"]["ok"] is False
    assert "openOrderEnd" in report["failure_reason"]


def test_client_id_collision_suspected(tmp_path: Path) -> None:
    report = run_with_transport(
        tmp_path,
        TimeoutTransport(
            timeout_stage="nextValidId",
            errors=[{"error_code": 326, "error_string": "client id is already in use"}],
        ),
    )

    assert report["classification"] == watchdog.CLIENT_ID_COLLISION_SUSPECTED
    assert report["checks"]["next_valid_id"]["ok"] is False


def test_api_modal_or_blocked_suspected(tmp_path: Path) -> None:
    report = run_with_transport(
        tmp_path,
        TimeoutTransport(
            timeout_stage="nextValidId",
            errors=[{"error_code": 502, "error_string": "could not connect"}],
        ),
    )

    assert report["classification"] == watchdog.API_MODAL_OR_BLOCKED_SUSPECTED
    assert report["checks"]["next_valid_id"]["ok"] is False


def test_incomplete_state_classified(tmp_path: Path) -> None:
    report = run_with_transport(tmp_path, ReadyTransport(connect_ack=False))

    assert report["classification"] == watchdog.CONNECTED_BUT_INCOMPLETE
    assert report["checks"]["connect_ack"]["ok"] is False
    assert report["failure_reason"] == "IBKR read-only callbacks completed but one or more required checks were incomplete"


def test_artifact_writing(tmp_path: Path) -> None:
    output_path = tmp_path / "watchdog" / "latest.json"
    report = run_with_transport(tmp_path, ReadyTransport())

    watchdog.write_watchdog_report(output_path=output_path, report=report)

    payload = json.loads(output_path.read_text(encoding="utf-8"))
    assert payload["schema_version"] == "track_b_ibkr_connectivity_watchdog_v1"
    assert payload["classification"] == watchdog.CONNECTED_READ_ONLY
    assert payload["future_supervised_repair_policy"]["auto_repair_enabled"] is False


def test_default_client_id_uses_high_rotating_range() -> None:
    client_id = watchdog.default_watchdog_client_id(now_fn=fixed_now)

    assert 9800 <= client_id <= 9899


def test_watchdog_source_has_no_broker_order_mutation_apis() -> None:
    source = inspect.getsource(watchdog)

    forbidden = [
        "placeOrder",
        "cancelOrder",
        "globalCancel",
        "reqGlobalCancel",
        "manual_close",
        "submit_limit_order",
    ]
    for token in forbidden:
        assert token not in source


class ReadyTransport:
    def __init__(self, *, connect_ack: bool = True, accounts: tuple[str, ...] = ("DUM882026",)) -> None:
        self.connected_with: tuple[str, int, int, bool] | None = None
        self.disconnected = False
        self.connect_ack = connect_ack
        self.accounts = accounts

    def connect(self, *, host: str, port: int, client_id: int, readonly: bool) -> None:
        self.connected_with = (host, port, client_id, readonly)

    def next_valid_id(self) -> int:
        return 1001

    def managed_accounts(self) -> tuple[str, ...]:
        return self.accounts

    def snapshot_position(self, *, run_id: str, account_id: str, contract_key: str, observed_at: datetime) -> object:
        return {"run_id": run_id, "account_id": account_id, "contract_key": contract_key, "observed_at": observed_at.isoformat()}

    def snapshot_open_orders(self, *, account_id: str, contract_key: str, observed_at: datetime) -> tuple[object, ...]:
        return ()

    def disconnect(self) -> None:
        self.disconnected = True

    def diagnostics_report(self) -> dict[str, object]:
        return {
            "connect_ack_received": self.connect_ack,
            "next_valid_id_received": True,
            "ibkr_errors": [],
            "suspected_causes": [],
        }


class TimeoutTransport(ReadyTransport):
    def __init__(self, *, timeout_stage: str, errors: list[dict[str, object]] | None = None) -> None:
        super().__init__()
        self.timeout_stage = timeout_stage
        self.errors = errors or []

    def next_valid_id(self) -> int:
        if self.timeout_stage == "nextValidId":
            raise IbkrReadOnlyTimeoutError("missing nextValidId callback")
        return 1001

    def managed_accounts(self) -> tuple[str, ...]:
        if self.timeout_stage == "managedAccounts":
            raise IbkrReadOnlyTimeoutError("missing managedAccounts callback")
        return ("DUM882026",)

    def snapshot_position(self, *, run_id: str, account_id: str, contract_key: str, observed_at: datetime) -> object:
        if self.timeout_stage == "positionEnd":
            raise IbkrReadOnlyTimeoutError("missing positionEnd callback")
        return super().snapshot_position(
            run_id=run_id,
            account_id=account_id,
            contract_key=contract_key,
            observed_at=observed_at,
        )

    def snapshot_open_orders(self, *, account_id: str, contract_key: str, observed_at: datetime) -> tuple[object, ...]:
        if self.timeout_stage == "openOrderEnd":
            raise IbkrReadOnlyTimeoutError("missing openOrderEnd callback")
        return ()

    def diagnostics_report(self) -> dict[str, object]:
        return {
            "connect_ack_received": True,
            "next_valid_id_received": self.timeout_stage != "nextValidId",
            "ibkr_errors": self.errors,
            "suspected_causes": ["TWS modal dialog/API-block condition"] if self.errors else [],
        }
