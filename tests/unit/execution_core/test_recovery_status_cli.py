from __future__ import annotations

import inspect
import json
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from mgc_v05l.execution_core import recovery_status_cli
from mgc_v05l.execution_core.ibkr_readonly_transport import IbkrReadOnlyTransportConfig


class CliFakeTransport:
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

    def snapshot_position(self, *, run_id: str, account_id: str, contract_key: str, observed_at) -> Mapping[str, Any]:  # noqa: ANN001
        return {"account_id": account_id, "contract_key": contract_key, "signed_quantity": 0}

    def snapshot_open_orders(self, *, account_id: str, contract_key: str, observed_at) -> Sequence[Mapping[str, Any]]:  # noqa: ANN001
        return ()

    def observe_quote(self, *, run_id: str, contract_key: str, observed_at) -> None:  # noqa: ANN001
        return None


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
        "--broker-order-id",
        "1",
        "--perm-id",
        "736787312",
        "--output-root",
        str(tmp_path / "recovery_status"),
        *extra,
    ]


def test_cli_fake_recovery_status_path_prints_ready_report(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    created: list[IbkrReadOnlyTransportConfig] = []

    def factory(config: IbkrReadOnlyTransportConfig) -> CliFakeTransport:
        created.append(config)
        return CliFakeTransport()

    exit_code = recovery_status_cli.main(cli_args(tmp_path), transport_factory=factory)
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["classification"] == "RECOVERY_READY_CLEAN"
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
        recovery_status_cli.main(base, transport_factory=lambda config: CliFakeTransport())

    assert message in capsys.readouterr().err


def test_cli_source_has_no_order_mutation_path() -> None:
    source = inspect.getsource(recovery_status_cli)

    assert "placeOrder" not in source
    assert "submit_limit_order" not in source
    assert "cancelOrder" not in source
    assert "transmit=True" not in source
