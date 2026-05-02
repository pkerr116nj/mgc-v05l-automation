from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.execution_core import databento_quote_cli
from mgc_v05l.execution_core.databento_quote_provider import DatabentoQuoteProviderConfig
from mgc_v05l.execution_core.pricing import MarketDataMode, MarketDataRole
from mgc_v05l.execution_core.quote_provider import QuoteSnapshot


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


def cli_args(tmp_path: Path, *extra: str) -> list[str]:
    return [
        "--contract-key",
        "MGC-202606",
        "--databento-symbol",
        "MGCM6",
        "--tick-size",
        "0.1",
        "--exchange",
        "COMEX",
        "--currency",
        "USD",
        "--output-root",
        str(tmp_path / "quotes"),
        *extra,
    ]


class FakeProvider:
    def __init__(self, config: DatabentoQuoteProviderConfig) -> None:
        self.config = config

    def get_quote(self, contract_key: str) -> QuoteSnapshot:
        return QuoteSnapshot(
            provider="DATABENTO",
            mode=MarketDataMode.REALTIME,
            role=MarketDataRole.PRIMARY,
            contract_key=contract_key,
            provider_symbol=self.config.databento_symbol,
            bid="4626.0",
            ask="4626.1",
            last="4626.0",
            timestamp=datetime.now(timezone.utc),
            tick_size=self.config.tick_size,
            exchange=self.config.exchange,
            currency=self.config.currency,
        )


def test_cli_requires_databento_api_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)

    with pytest.raises(SystemExit):
        databento_quote_cli.main(cli_args(tmp_path), provider_factory=FakeProvider)

    assert "DATABENTO_API_KEY is required" in capsys.readouterr().err


def test_cli_requires_explicit_databento_symbol(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    args = cli_args(tmp_path)
    index = args.index("--databento-symbol")
    del args[index : index + 2]

    with pytest.raises(SystemExit):
        databento_quote_cli.main(args, provider_factory=FakeProvider)


def test_cli_prints_and_writes_read_only_quote_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")

    exit_code = databento_quote_cli.main(cli_args(tmp_path), provider_factory=FakeProvider)
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["provider"] == "DATABENTO"
    assert payload["databento_symbol"] == "MGCM6"
    assert payload["bid"] == "4626.0"
    assert payload["ask"] == "4626.1"
    assert payload["last"] == "4626.0"
    assert report["api_key_present"] is True
    assert report["api_key_value"] is None
    assert report["submit_enabled"] is False
    assert report["place_order_called"] is False
    assert report["cancel_called"] is False


def test_cli_source_has_no_broker_mutation_or_track_a_paths() -> None:
    source = inspect.getsource(databento_quote_cli)

    assert "placeOrder" not in source
    assert "submit_limit_order" not in source
    assert "cancelOrder" not in source
    assert "paper_proof_cli" not in source
    assert "live_feed" not in source
    assert "schwab" not in source.lower()
    assert "dashboard" not in source
