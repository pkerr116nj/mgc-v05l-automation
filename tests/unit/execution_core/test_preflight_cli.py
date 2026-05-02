from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from mgc_v05l.execution_core import preflight_cli
from mgc_v05l.execution_core.ibkr_readonly_transport import IbkrReadOnlyTransportConfig


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


class CliFakeTransport:
    def __init__(self) -> None:
        self.connected_with: dict[str, Any] | None = None

    def connect(self, *, host: str, port: int, client_id: int, readonly: bool) -> None:
        self.connected_with = {"host": host, "port": port, "client_id": client_id, "readonly": readonly}

    def disconnect(self) -> None:
        return None

    def managed_accounts(self) -> Sequence[str]:
        return ("DUM882026",)

    def next_valid_id(self) -> int | None:
        return 1001

    def qualify_contract(self, *, contract_key: str, allowlist_entry: Mapping[str, Any]) -> Mapping[str, Any]:
        return {"contract_key": contract_key, **dict(allowlist_entry)}

    def snapshot_position(
        self,
        *,
        run_id: str,
        account_id: str,
        contract_key: str,
        observed_at: datetime,
    ) -> Mapping[str, Any]:
        return {"account_id": account_id, "contract_key": contract_key, "signed_quantity": 0}

    def snapshot_open_orders(
        self,
        *,
        account_id: str,
        contract_key: str,
        observed_at: datetime,
    ) -> Sequence[Mapping[str, Any]]:
        return ()

    def observe_quote(
        self,
        *,
        run_id: str,
        contract_key: str,
        observed_at: datetime,
    ) -> Mapping[str, Any]:
        return {"run_id": run_id, "contract_key": contract_key, "bid": "2345.0", "ask": "2345.1", "last": "2345.05"}


def cli_args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "--mode",
        "PAPER",
        "--host",
        "127.0.0.1",
        "--port",
        "7497",
        "--client-id",
        "17077",
        "--account-id",
        "DUM882026",
        "--contract-key",
        "MGC-202606",
        "--output-root",
        str(tmp_path / "preflight"),
        *extra,
    ]


def test_cli_fake_preflight_path_prints_ready_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    created: list[IbkrReadOnlyTransportConfig] = []

    def factory(config: IbkrReadOnlyTransportConfig) -> CliFakeTransport:
        created.append(config)
        return CliFakeTransport()

    exit_code = preflight_cli.main(cli_args(tmp_path), transport_factory=factory)
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["classification"] == "READY_READ_ONLY"
    assert Path(output["report_json"]).exists()
    assert created[0].request_timeout_seconds == 10.0


@pytest.mark.parametrize(
    "args, message",
    [
        (["--mode", "LIVE"], "--mode must be PAPER"),
        (["--port", "7496"], "--port must be 7497"),
        (["--port", "4001"], "--port must be 7497"),
        (["--port", "4002"], "--port must be 7497"),
        (["--host", "localhost"], "--host must be 127.0.0.1"),
    ],
)
def test_cli_rejects_non_paper_or_live_connection_args(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
    args: list[str],
    message: str,
) -> None:
    base = cli_args(tmp_path)
    for index in range(0, len(base), 2):
        if base[index] == args[0]:
            base[index + 1] = args[1]
            break

    with pytest.raises(SystemExit):
        preflight_cli.main(base, transport_factory=lambda config: CliFakeTransport())

    assert message in capsys.readouterr().err


@pytest.mark.parametrize("missing_option", ["--account-id", "--client-id"])
def test_cli_requires_explicit_account_id_and_client_id(tmp_path: Path, missing_option: str) -> None:
    args = cli_args(tmp_path)
    index = args.index(missing_option)
    del args[index : index + 2]

    with pytest.raises(SystemExit):
        preflight_cli.main(args, transport_factory=lambda config: CliFakeTransport())


def test_cli_source_has_no_order_submission_path() -> None:
    source = inspect.getsource(preflight_cli)

    assert "placeOrder" not in source
    assert "submit_limit_order" not in source
    assert "submit_enabled=True" not in source
    assert "transmit=True" not in source
