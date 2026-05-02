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
        "--databento-continuous-symbol",
        "MGC.v.0",
        "--allowlisted-local-symbol",
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
        raw_symbol = self.config.databento_symbol or "MGCM6"
        return QuoteSnapshot(
            provider="DATABENTO",
            mode=MarketDataMode.REALTIME,
            role=MarketDataRole.PRIMARY,
            contract_key=contract_key,
            provider_symbol=raw_symbol,
            bid="4626.0",
            ask="4626.1",
            last="4626.0",
            timestamp=datetime.now(timezone.utc),
            tick_size=self.config.tick_size,
            exchange=self.config.exchange,
            currency=self.config.currency,
            raw={
                "symbol_source": "MANUAL_PROVIDER_SYMBOL_OVERRIDE" if self.config.databento_symbol else "CONTINUOUS_SYMBOL_RESOLUTION",
                "requested_continuous_symbol": self.config.databento_continuous_symbol,
                "resolved_instrument_id": "123456" if self.config.databento_continuous_symbol else None,
                "resolved_raw_symbol": raw_symbol if self.config.databento_continuous_symbol else None,
                "resolution_status": "RESOLVED" if self.config.databento_continuous_symbol else None,
                "resolution_path": "continuous->instrument_id" if self.config.databento_continuous_symbol else None,
                "requested_resolution_date": self.config.resolution_date or "2026-05-02",
                "actual_resolution_date_used": "2026-05-01" if self.config.allow_prior_session_resolution else self.config.resolution_date or "2026-05-02",
                "prior_session_fallback_used": self.config.allow_prior_session_resolution,
                "fallback_lookback_days": self.config.prior_session_resolution_lookback_days,
                "resolution_session_type": "PRIOR_SESSION_RESOLUTION_FALLBACK"
                if self.config.allow_prior_session_resolution
                else "CURRENT_SESSION",
                "resolution_date": self.config.resolution_date or "2026-05-02",
                "resolution_start": self.config.resolution_start or "2026-05-02",
                "resolution_end": self.config.resolution_end or "2026-05-03",
                "mapping_intervals": [{"d0": "2026-04-01", "d1": "2026-06-01", "s": "123456"}],
                "active_mapping": {"d0": "2026-04-01", "d1": "2026-06-01", "s": "123456"},
                "raw_symbol_lookup_path": "instrument_id->raw_symbol" if self.config.databento_continuous_symbol else None,
                "raw_symbol_resolution_status": "RESOLVED" if self.config.databento_continuous_symbol else None,
                "raw_symbol_match_status": "MATCH" if self.config.databento_continuous_symbol else "MANUAL_OVERRIDE_OPERATOR_REVIEW",
                "quote_request_symbol": "123456" if self.config.databento_continuous_symbol else raw_symbol,
                "quote_request_stype_in": "instrument_id" if self.config.databento_continuous_symbol else self.config.stype_in,
                "execution_contract_validation_status": "MATCHED_ALLOWLISTED_LOCAL_SYMBOL"
                if self.config.databento_continuous_symbol
                else "MANUAL_OVERRIDE_OPERATOR_REVIEW",
            },
        )


def test_cli_requires_databento_api_key(tmp_path: Path, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]) -> None:
    monkeypatch.delenv("DATABENTO_API_KEY", raising=False)

    with pytest.raises(SystemExit):
        databento_quote_cli.main(cli_args(tmp_path), provider_factory=FakeProvider)

    assert "DATABENTO_API_KEY is required" in capsys.readouterr().err


def test_cli_requires_exactly_one_databento_symbol_source(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    args = cli_args(tmp_path)
    index = args.index("--databento-continuous-symbol")
    del args[index : index + 2]

    with pytest.raises(SystemExit):
        databento_quote_cli.main(args, provider_factory=FakeProvider)

    args = cli_args(tmp_path, "--databento-symbol", "MGCM6")
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
    assert payload["databento_continuous_symbol"] == "MGC.v.0"
    assert payload["manual_provider_symbol_override"] is False
    assert payload["resolved_instrument_id"] == "123456"
    assert payload["resolved_raw_symbol"] == "MGCM6"
    assert payload["resolution_path"] == "continuous->instrument_id"
    assert payload["requested_resolution_date"] == "2026-05-02"
    assert payload["actual_resolution_date_used"] == "2026-05-02"
    assert payload["prior_session_fallback_used"] is False
    assert payload["fallback_lookback_days"] == 3
    assert payload["resolution_session_type"] == "CURRENT_SESSION"
    assert payload["resolution_date"] == "2026-05-02"
    assert payload["resolution_start"] == "2026-05-02"
    assert payload["resolution_end"] == "2026-05-03"
    assert payload["raw_symbol_lookup_path"] == "instrument_id->raw_symbol"
    assert payload["raw_symbol_match_status"] == "MATCH"
    assert payload["quote_request_symbol"] == "123456"
    assert payload["quote_request_stype_in"] == "instrument_id"
    assert payload["execution_contract_validation_status"] == "MATCHED_ALLOWLISTED_LOCAL_SYMBOL"
    assert payload["bid"] == "4626.0"
    assert payload["ask"] == "4626.1"
    assert payload["last"] == "4626.0"
    assert report["api_key_present"] is True
    assert report["api_key_value"] is None
    assert report["databento_continuous_symbol"] == "MGC.v.0"
    assert report["databento_symbol"] is None
    assert report["manual_provider_symbol_override"] is False
    assert report["resolution"]["requested_continuous_symbol"] == "MGC.v.0"
    assert report["resolution"]["resolved_instrument_id"] == "123456"
    assert report["resolution"]["resolved_raw_symbol"] == "MGCM6"
    assert report["resolution"]["resolution_path"] == "continuous->instrument_id"
    assert report["resolution"]["requested_resolution_date"] == "2026-05-02"
    assert report["resolution"]["actual_resolution_date_used"] == "2026-05-02"
    assert report["resolution"]["prior_session_fallback_used"] is False
    assert report["resolution"]["fallback_lookback_days"] == 3
    assert report["resolution"]["resolution_session_type"] == "CURRENT_SESSION"
    assert report["resolution"]["resolution_date"] == "2026-05-02"
    assert report["resolution"]["resolution_start"] == "2026-05-02"
    assert report["resolution"]["resolution_end"] == "2026-05-03"
    assert report["resolution"]["mapping_intervals"] == [{"d0": "2026-04-01", "d1": "2026-06-01", "s": "123456"}]
    assert report["resolution"]["active_mapping"] == {"d0": "2026-04-01", "d1": "2026-06-01", "s": "123456"}
    assert report["resolution"]["raw_symbol_lookup_path"] == "instrument_id->raw_symbol"
    assert report["resolution"]["raw_symbol_match_status"] == "MATCH"
    assert report["resolution"]["quote_request_symbol"] == "123456"
    assert report["resolution"]["quote_request_stype_in"] == "instrument_id"
    assert report["resolution"]["execution_contract_validation_status"] == "MATCHED_ALLOWLISTED_LOCAL_SYMBOL"
    assert report["submit_enabled"] is False
    assert report["place_order_called"] is False
    assert report["cancel_called"] is False


def test_cli_accepts_resolution_date(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")

    exit_code = databento_quote_cli.main(cli_args(tmp_path, "--resolution-date", "2026-05-02"), provider_factory=FakeProvider)
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert report["resolution"]["resolution_date"] == "2026-05-02"


def test_cli_accepts_prior_session_resolution_fallback_flag(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")

    exit_code = databento_quote_cli.main(
        cli_args(tmp_path, "--resolution-date", "2026-05-02", "--allow-prior-session-resolution"),
        provider_factory=FakeProvider,
    )
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["requested_resolution_date"] == "2026-05-02"
    assert payload["actual_resolution_date_used"] == "2026-05-01"
    assert payload["prior_session_fallback_used"] is True
    assert payload["resolution_session_type"] == "PRIOR_SESSION_RESOLUTION_FALLBACK"
    assert report["resolution"]["prior_session_fallback_used"] is True


def test_cli_manual_databento_symbol_override_remains_supported(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    args = cli_args(tmp_path)
    index = args.index("--databento-continuous-symbol")
    del args[index : index + 2]
    args.extend(["--databento-symbol", "MGCM6"])

    exit_code = databento_quote_cli.main(args, provider_factory=FakeProvider)
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 0
    assert payload["databento_continuous_symbol"] is None
    assert payload["manual_provider_symbol_override"] is True
    assert report["databento_symbol"] == "MGCM6"
    assert report["manual_provider_symbol_override"] is True
    assert report["resolution"]["execution_contract_validation_status"] == "MANUAL_OVERRIDE_OPERATOR_REVIEW"


def test_cli_source_has_no_broker_mutation_or_track_a_paths() -> None:
    source = inspect.getsource(databento_quote_cli)

    assert "placeOrder" not in source
    assert "submit_limit_order" not in source
    assert "cancelOrder" not in source
    assert "paper_proof_cli" not in source
    assert "live_feed" not in source
    assert "schwab" not in source.lower()
    assert "dashboard" not in source
