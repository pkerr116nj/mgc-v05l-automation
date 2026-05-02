from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core import paper_proof_cli
from mgc_v05l.execution_core.preflight import PreflightClassification, PreflightResult


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


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
        str(tmp_path / "paper_proof"),
        "--submit-enabled",
        "--confirm-paper-submit",
        "--allow-delayed-data-paper-proof",
        *extra,
    ]


def ready_preflight(tmp_path: Path) -> PreflightResult:
    return PreflightResult(
        run_id="preflight-cli",
        classification=PreflightClassification.READY_READ_ONLY,
        report_json=tmp_path / "preflight.json",
        report_md=tmp_path / "preflight.md",
        report={
            "classification": "READY_READ_ONLY",
            "config": {"account_id": "DUM882026"},
            "contract_key": "MGC-202606",
            "checks": [
                {"name": "managed_account_exact_match", "passed": True},
                {"name": "contract_qualified", "passed": True},
            ],
            "position": {"signed_quantity": 0},
            "open_orders": [],
            "quote_observed": True,
            "market_data_mode": "DELAYED",
            "market_data_provider": "IBKR",
            "market_data_role": "DIAGNOSTIC",
            "delayed_data_warning_seen": True,
            "paper_route_readiness": True,
            "production_live_money_readiness": False,
        },
    )


class FakeTransport:
    def connect(self, *, host: str, port: int, client_id: int, readonly: bool) -> None:
        return None

    def disconnect(self) -> None:
        return None

    def managed_accounts(self):
        return ("DUM882026",)

    def next_valid_id(self):
        return 1

    def qualify_contract(self, *, contract_key, allowlist_entry):
        return {"contract_key": contract_key, **dict(allowlist_entry)}

    def snapshot_position(self, *, run_id, account_id, contract_key, observed_at):
        return {"account_id": account_id, "contract_key": contract_key, "signed_quantity": 0}

    def snapshot_open_orders(self, *, account_id, contract_key, observed_at):
        return ()

    def observe_quote(self, *, run_id, contract_key, observed_at):
        return {
            "run_id": run_id,
            "contract_key": contract_key,
            "bid": "2345.0",
            "ask": "2345.1",
            "last": "2345.05",
            "market_data_provider": "IBKR",
            "market_data_mode": "DELAYED",
            "market_data_role": "DIAGNOSTIC",
            "delayed_data_warning_seen": True,
        }


@pytest.mark.parametrize(
    ("remove", "message"),
    [
        ("--confirm-paper-submit", "--confirm-paper-submit is required"),
        ("--submit-enabled", "--submit-enabled is required"),
    ],
)
def test_cli_rejects_missing_operator_submit_flags(tmp_path: Path, remove: str, message: str, capsys: pytest.CaptureFixture[str]) -> None:
    args = cli_args(tmp_path)
    args.remove(remove)

    with pytest.raises(SystemExit):
        paper_proof_cli.main(args, transport_factory=lambda cfg: FakeTransport())

    assert message in capsys.readouterr().err


@pytest.mark.parametrize(
    ("option", "value", "message"),
    [
        ("--mode", "LIVE", "--mode must be PAPER"),
        ("--host", "localhost", "--host must be 127.0.0.1"),
        ("--port", "7496", "--port must be 7497"),
        ("--port", "4001", "--port must be 7497"),
        ("--port", "4002", "--port must be 7497"),
    ],
)
def test_cli_rejects_non_paper_or_live_connection_args(
    tmp_path: Path,
    option: str,
    value: str,
    message: str,
    capsys: pytest.CaptureFixture[str],
) -> None:
    args = cli_args(tmp_path)
    args[args.index(option) + 1] = value

    with pytest.raises(SystemExit):
        paper_proof_cli.main(args, transport_factory=lambda cfg: FakeTransport())

    assert message in capsys.readouterr().err


def test_cli_runs_with_fake_transport_and_fake_proof_runner(tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
    def proof_runner(config, run_id):  # type: ignore[no-untyped-def]
        from mgc_v05l.execution_core.fake_adapter import FakePaperAdapter
        from mgc_v05l.execution_core.harness import run_fake_paper_proof

        return run_fake_paper_proof(
            config=config,
            adapter=FakePaperAdapter(account_id=config.account_id, contract_key=config.contract_key, scenario="pass"),
            run_id=run_id,
            now=aware_now(),
        )

    exit_code = paper_proof_cli.main(
        cli_args(tmp_path),
        transport_factory=lambda cfg: FakeTransport(),
        proof_runner=proof_runner,
    )
    output = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert output["classification"] == "TRACK_B_PAPER_PROOF_PASSED"
    assert Path(output["report_json"]).exists()
