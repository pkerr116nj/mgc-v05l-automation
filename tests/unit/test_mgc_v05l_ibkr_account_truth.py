from __future__ import annotations

import json
from pathlib import Path

import pytest

from mgc_v05l.app.ibkr_account_truth import main
from mgc_v05l.brokers.ibkr import (
    IbkrClient,
    IbkrSession,
    build_default_ibkr_order_id_policy,
)
from mgc_v05l.execution.ibkr_account_truth import (
    _wait_for_api_handshake,
    _wait_for_managed_accounts,
)


def _fixture_payload(*, managed_accounts="DU1234567", selected_account_id: str | None = None) -> dict:
    payload = {
        "managed_accounts": managed_accounts,
        "balances": [
            {
                "account_id": "DU1234567",
                "currency": "USD",
                "cash_balance": "50000",
                "buying_power": "100000",
                "available_funds": "45000",
                "net_liquidation": "55000",
            }
        ],
        "positions": [
            {
                "account_id": "DU1234567",
                "contract": {
                    "con_id": 12345,
                    "symbol": "MES",
                    "local_symbol": "MESM26",
                    "security_type": "FUT",
                    "exchange": "CME",
                    "currency": "USD",
                    "expiry": "202606",
                    "multiplier": "5",
                },
                "quantity": "1",
                "average_cost": "5300.25",
                "market_price": "5301.00",
                "market_value": "26505",
            }
        ],
        "open_orders": [
            {
                "account_id": "DU1234567",
                "broker_order_id": 7001,
                "client_id": 91,
                "perm_id": 8001,
                "contract": {
                    "con_id": 12345,
                    "symbol": "MES",
                    "local_symbol": "MESM26",
                    "security_type": "FUT",
                    "exchange": "CME",
                    "currency": "USD",
                    "expiry": "202606",
                    "multiplier": "5",
                },
                "status": "Submitted",
                "quantity": "1",
                "filled_quantity": "0",
            }
        ],
        "completed_orders": [],
        "executions": [],
    }
    if selected_account_id is not None:
        payload["selected_account_id"] = selected_account_id
    return payload


def test_fixture_mode_generates_normalized_truth_snapshot(tmp_path: Path) -> None:
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(json.dumps(_fixture_payload()), encoding="utf-8")
    output_dir = tmp_path / "truth"

    result = main(
        [
            "--mode",
            "paper",
            "--host",
            "127.0.0.1",
            "--port",
            "7497",
            "--client-id",
            "91",
            "--fixture-json",
            str(fixture_path),
            "--output-dir",
            str(output_dir),
        ]
    )

    assert result == 0
    summary = json.loads((output_dir / "reports" / "ibkr_account_truth_summary.json").read_text(encoding="utf-8"))
    assert summary["status"] == "ready"
    assert "PAPER READ-ONLY MODE ONLY" in summary["operator_safety_message"]
    assert summary["snapshot"]["provider_id"] == "ibkr_execution"
    assert summary["snapshot"]["selected_account_id"] == "DU1234567"
    assert summary["acceptance"]["ready"] is True


def test_paper_live_separation_rejects_live_mode(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="paper mode only|paper mode is rejected|supports paper mode only"):
        main(
            [
                "--mode",
                "live",
                "--output-dir",
                str(tmp_path / "live"),
            ]
        )


def test_real_api_is_blocked_without_explicit_flag(tmp_path: Path) -> None:
    with pytest.raises(RuntimeError, match="blocked unless --allow-real-api"):
        main(
            [
                "--mode",
                "paper",
                "--host",
                "127.0.0.1",
                "--port",
                "7497",
                "--client-id",
                "91",
                "--output-dir",
                str(tmp_path / "blocked"),
            ]
        )


def test_managed_account_parsing_fails_closed_when_ambiguous(tmp_path: Path) -> None:
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(
        json.dumps(_fixture_payload(managed_accounts="DU1111111,DU2222222")),
        encoding="utf-8",
    )
    with pytest.raises(RuntimeError, match="ambiguous|Provide --account-id"):
        main(
            [
                "--mode",
                "paper",
                "--fixture-json",
                str(fixture_path),
                "--output-dir",
                str(tmp_path / "ambiguous"),
            ]
        )


def test_fixture_mode_accepts_explicit_account_id_when_managed_accounts_are_multiple(tmp_path: Path) -> None:
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(
        json.dumps(_fixture_payload(managed_accounts=["DU1111111", "DU1234567"])),
        encoding="utf-8",
    )
    output_dir = tmp_path / "explicit"
    result = main(
        [
            "--mode",
            "paper",
            "--account-id",
            "DU1234567",
            "--fixture-json",
            str(fixture_path),
            "--output-dir",
            str(output_dir),
        ]
    )
    assert result == 0
    snapshot = json.loads((output_dir / "reports" / "ibkr_account_truth_snapshot.json").read_text(encoding="utf-8"))
    assert snapshot["selected_account_id"] == "DU1234567"


def test_failure_writes_safety_audit_artifact(tmp_path: Path) -> None:
    fixture_path = tmp_path / "fixture.json"
    fixture_path.write_text(json.dumps(_fixture_payload(managed_accounts=[])), encoding="utf-8")
    output_dir = tmp_path / "failure"
    with pytest.raises(RuntimeError, match="returned no accounts"):
        main(
            [
                "--mode",
                "paper",
                "--fixture-json",
                str(fixture_path),
                "--output-dir",
                str(output_dir),
            ]
        )
    audit = json.loads((output_dir / "reports" / "ibkr_account_truth_audit.json").read_text(encoding="utf-8"))
    assert audit["status"] == "failed"
    assert audit["orders_allowed"] is False


def test_real_api_wait_accepts_managed_accounts_seen_during_handshake() -> None:
    client = IbkrClient(
        IbkrSession(
            host="127.0.0.1",
            port=7497,
            client_id=91,
            account_id="DU1234567",
            gateway_mode="paper",
            read_only=True,
            order_id_policy=build_default_ibkr_order_id_policy(client_id=91, live_orders_enabled=False),
        )
    )
    client.record_managed_accounts(("DU1234567",))

    _wait_for_api_handshake(client=client, timeout_seconds=0.01, sleep_fn=lambda _: None)
    _wait_for_managed_accounts(client=client, timeout_seconds=0.01, sleep_fn=lambda _: None)

    assert client.connection_state().managed_accounts == ("DU1234567",)
