from __future__ import annotations

import inspect
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

import pytest

from mgc_v05l.execution_core import databento_quote_cli
from mgc_v05l.execution_core.databento_quote_provider import DatabentoAvailableEndError, DatabentoQuoteParseError, DatabentoQuoteProviderConfig
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
            mode=MarketDataMode.UNKNOWN if self.config.allow_available_end_fallback else MarketDataMode.REALTIME,
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
                "requested_quote_start": "2026-05-02T11:55:00+00:00",
                "requested_quote_end": "2026-05-02T12:00:00+00:00",
                "actual_quote_start": "2026-05-01T23:54:00+00:00" if self.config.allow_available_end_fallback else "2026-05-02T11:55:00+00:00",
                "actual_quote_end": "2026-05-01T23:59:00+00:00" if self.config.allow_available_end_fallback else "2026-05-02T12:00:00+00:00",
                "provider_available_end": "2026-05-01T23:59:00+00:00" if self.config.allow_available_end_fallback else None,
                "provider_available_end_initial": "2026-05-01T23:59:00+00:00" if self.config.allow_available_end_fallback else None,
                "provider_available_end_final": "2026-05-01T23:59:00+00:00" if self.config.allow_available_end_fallback else None,
                "available_end_fallback_used": self.config.allow_available_end_fallback,
                "available_end_buffer_seconds": self.config.available_end_buffer_seconds,
                "allow_available_end_fallback_requested": self.config.allow_available_end_fallback,
                "allow_available_end_fallback_effective": self.config.allow_available_end_fallback,
                "native_databento_path_used": self.config.allow_available_end_fallback,
                "schemas_attempted": ["mbp-1", "trades"],
                "available_end_retry_attempted": self.config.allow_available_end_fallback,
                "available_end_retry_count": 1 if self.config.allow_available_end_fallback else 0,
                "available_end_retry_reason": "initial_data_start_after_available_end"
                if self.config.allow_available_end_fallback
                else None,
                "quote_age_seconds": "0.0",
                "usable_for_paper_pricing": not self.config.allow_available_end_fallback,
                "usable_for_live_money_readiness": not self.config.allow_available_end_fallback,
                "quote_temporal_scope": "HISTORICAL_WINDOW" if self.config.quote_end_timestamp else "UNKNOWN",
                "active_session_quote": self.config.quote_end_timestamp is None and not self.config.allow_available_end_fallback,
                "current_executable_quote": self.config.quote_end_timestamp is None and not self.config.allow_available_end_fallback,
                "session_closed_or_no_records": False,
                "no_records_reason": None,
                "quote_usable_for_paper_pricing": self.config.quote_end_timestamp is None and not self.config.allow_available_end_fallback,
                "quote_usable_for_live_money_readiness": self.config.quote_end_timestamp is None and not self.config.allow_available_end_fallback,
                "databento_schema": {"bid_ask": "mbp-1", "last": "trades"},
                "records_returned": {"bid_ask": 1, "trades": 1},
                "first_raw_record_keys_or_shape": {
                    "bid_ask": {"root_keys": ["levels", "ts_event"], "levels_count": 1, "levels[0]_keys": ["ask_px", "bid_px"]},
                    "trades": {"root_keys": ["price", "ts_event"]},
                },
                "parser_bid_field_source": "levels[0].bid_px",
                "parser_ask_field_source": "levels[0].ask_px",
                "parser_last_field_source": "price",
                "no_quote_records_reason": None,
            },
        )


class FailingAvailableEndProvider:
    def __init__(self, config: DatabentoQuoteProviderConfig) -> None:
        self.config = config

    def get_quote(self, contract_key: str) -> QuoteSnapshot:
        raise DatabentoAvailableEndError(
            "requested quote window is after Databento available_end; rerun with --allow-available-end-fallback or earlier --quote-end-timestamp",
            provider_available_end=datetime(2026, 5, 1, 23, 59, tzinfo=timezone.utc),
            detail="sanitized detail",
            diagnostics={
                "resolved_instrument_id": "123456",
                "resolved_symbol_stype": "instrument_id",
                "quote_request_symbol": "123456",
                "quote_request_stype_in": "instrument_id",
                "dataset": "GLBX.MDP3",
                "schema": {"bid_ask": "mbp-1", "last": "trades"},
                "start": "2026-05-02T11:55:00+00:00",
                "end": "2026-05-02T12:00:00+00:00",
                "requested_quote_start": "2026-05-02T11:55:00+00:00",
                "requested_quote_end": "2026-05-02T12:00:00+00:00",
                "actual_quote_start": "2026-05-02T11:55:00+00:00",
                "actual_quote_end": "2026-05-02T12:00:00+00:00",
                "available_end_fallback_used": False,
                "provider_available_end_initial": "2026-05-01T23:59:00+00:00",
                "provider_available_end_final": "2026-05-01T23:59:00+00:00",
                "available_end_buffer_seconds": 300,
                "allow_available_end_fallback_requested": False,
                "allow_available_end_fallback_effective": False,
                "native_databento_path_used": True,
                "schemas_attempted": ["mbp-1", "trades"],
                "available_end_retry_attempted": False,
                "available_end_retry_count": 0,
                "available_end_retry_reason": "fallback_not_enabled",
                "encoding": "json",
                "request_details": {
                    "bid_ask": {"schema": "mbp-1", "symbol": "123456", "stype_in": "instrument_id"},
                    "trades": {"schema": "trades", "symbol": "123456", "stype_in": "instrument_id"},
                },
                "raw_provider_error": "422 data_start_after_available_end sanitized detail",
                "native_databento_error_code": "data_start_after_available_end",
                "native_databento_error_message": "422 data_start_after_available_end sanitized detail",
                "native_exception_class": "RuntimeError",
                "native_exception_message_sanitized": "422 data_start_after_available_end sanitized detail",
            },
        )


class FailingParseProvider:
    def __init__(self, config: DatabentoQuoteProviderConfig) -> None:
        self.config = config

    def get_quote(self, contract_key: str) -> QuoteSnapshot:
        raise DatabentoQuoteParseError(
            "Databento returned no bid/ask quote records for schema mbp-1",
            diagnostics={
                "databento_schema": {"bid_ask": "mbp-1", "last": "trades"},
                "records_returned": {"bid_ask": 0, "trades": 1},
                "first_raw_record_keys_or_shape": {"bid_ask": None, "trades": {"root_keys": ["price", "ts_event"]}},
                "parser_bid_field_source": None,
                "parser_ask_field_source": None,
                "parser_last_field_source": "price",
                "no_quote_records_reason": "no records returned for schema mbp-1",
                "resolved_instrument_id": "123456",
                "resolved_symbol_stype": "instrument_id",
                "quote_request_symbol": "123456",
                "quote_request_stype_in": "instrument_id",
                "dataset": "GLBX.MDP3",
                "schema": {"bid_ask": "mbp-1", "last": "trades"},
                "start": "2026-05-02T11:55:00+00:00",
                "end": "2026-05-02T12:00:00+00:00",
                "encoding": "json",
                "request_details": {
                    "bid_ask": {
                        "endpoint": "https://hist.databento.com/v0/timeseries.get_range",
                        "dataset": "GLBX.MDP3",
                        "schema": "mbp-1",
                        "symbol": "123456",
                        "stype_in": "instrument_id",
                        "start": "2026-05-02T11:55:00+00:00",
                        "end": "2026-05-02T12:00:00+00:00",
                        "encoding": "json",
                        "compression": "none",
                        "limit": 1000,
                    },
                    "trades": {
                        "endpoint": "https://hist.databento.com/v0/timeseries.get_range",
                        "dataset": "GLBX.MDP3",
                        "schema": "trades",
                        "symbol": "123456",
                        "stype_in": "instrument_id",
                        "start": "2026-05-02T11:55:00+00:00",
                        "end": "2026-05-02T12:00:00+00:00",
                        "encoding": "json",
                        "compression": "none",
                        "limit": 1000,
                    },
                },
                "raw_provider_error": None,
            },
        )


class FailingWeekendNoRecordsProvider:
    def __init__(self, config: DatabentoQuoteProviderConfig) -> None:
        self.config = config

    def get_quote(self, contract_key: str) -> QuoteSnapshot:
        raise DatabentoQuoteParseError(
            "Databento returned no bid/ask quote records for schema mbp-1",
            diagnostics={
                "resolved_instrument_id": "42008160",
                "resolved_symbol_stype": "instrument_id",
                "quote_request_symbol": "42008160",
                "quote_request_stype_in": "instrument_id",
                "dataset": "GLBX.MDP3",
                "schema": {"bid_ask": "mbp-1", "last": "trades"},
                "start": "2026-05-03T04:20:00+00:00",
                "end": "2026-05-03T04:25:00+00:00",
                "requested_quote_start": "2026-05-03T04:32:31+00:00",
                "requested_quote_end": "2026-05-03T04:37:31+00:00",
                "actual_quote_start": "2026-05-03T04:20:00+00:00",
                "actual_quote_end": "2026-05-03T04:25:00+00:00",
                "available_end_fallback_used": True,
                "allow_available_end_fallback_requested": True,
                "allow_available_end_fallback_effective": True,
                "native_databento_path_used": True,
                "schemas_attempted": ["mbp-1", "trades"],
                "available_end_retry_attempted": True,
                "available_end_retry_count": 1,
                "available_end_retry_reason": "initial_data_start_after_available_end",
                "databento_schema": {"bid_ask": "mbp-1", "last": "trades"},
                "records_returned": {"bid_ask": 0, "trades": 0},
                "first_raw_record_keys_or_shape": {"bid_ask": None, "trades": None},
                "parser_bid_field_source": None,
                "parser_ask_field_source": None,
                "parser_last_field_source": None,
                "no_quote_records_reason": "no records returned for schema mbp-1",
                "quote_temporal_scope": "CURRENT_AVAILABLE_END",
                "active_session_quote": False,
                "current_executable_quote": False,
                "session_closed_or_no_records": True,
                "no_records_reason": "WEEKEND_OR_CLOSED_SESSION",
                "quote_usable_for_paper_pricing": False,
                "quote_usable_for_live_money_readiness": False,
            },
        )


class FakeDiagnosticTransport:
    def __init__(self, records_by_schema: Mapping[str, Sequence[Mapping[str, Any]]]) -> None:
        self.records_by_schema = records_by_schema
        self.requests: list[dict[str, Any]] = []

    def request_records(self, **kwargs: Any) -> Sequence[Mapping[str, Any]]:
        self.requests.append(dict(kwargs))
        return tuple(self.records_by_schema.get(str(kwargs["schema"]), ()))


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
    assert payload["quote_observed"] is True
    assert payload["available_end_fallback_used"] is False
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
    assert report["quote_observed"] is True
    assert report["quote_window"]["requested_quote_start"] == "2026-05-02T11:55:00+00:00"
    assert report["quote_window"]["available_end_fallback_used"] is False
    assert report["databento_schema"] == {"bid_ask": "mbp-1", "last": "trades"}
    assert report["records_returned"] == {"bid_ask": 1, "trades": 1}
    assert report["first_raw_record_keys_or_shape"]["bid_ask"]["levels[0]_keys"] == ["ask_px", "bid_px"]
    assert report["parser_bid_field_source"] == "levels[0].bid_px"
    assert report["parser_ask_field_source"] == "levels[0].ask_px"
    assert report["parser_last_field_source"] == "price"
    assert report["no_quote_records_reason"] is None
    assert report["quote_temporal_scope"] == "UNKNOWN"
    assert report["active_session_quote"] is True
    assert report["current_executable_quote"] is True
    assert report["session_closed_or_no_records"] is False
    assert report["quote_usable_for_paper_pricing"] is True
    assert report["quote_usable_for_live_money_readiness"] is True


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


def test_cli_accepts_quote_window_options(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")

    exit_code = databento_quote_cli.main(
        cli_args(
            tmp_path,
            "--quote-lookback-seconds",
            "600",
            "--quote-end-timestamp",
            "2026-05-01T23:59:00+00:00",
            "--allow-available-end-fallback",
        ),
        provider_factory=FakeProvider,
    )
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert payload["available_end_fallback_used"] is True
    assert payload["quote_temporal_scope"] == "HISTORICAL_WINDOW"
    assert payload["active_session_quote"] is False
    assert payload["quote_usable_for_paper_pricing"] is False
    assert payload["quote_usable_for_live_money_readiness"] is False
    assert report["quote_window"]["provider_available_end"] == "2026-05-01T23:59:00+00:00"
    assert report["live_money_quote_ready"] is False


def test_cli_passes_available_end_fallback_flag_to_provider(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    captured: dict[str, bool] = {}

    class CapturingProvider(FakeProvider):
        def __init__(self, config: DatabentoQuoteProviderConfig) -> None:
            captured["allow_available_end_fallback"] = config.allow_available_end_fallback
            super().__init__(config)

    exit_code = databento_quote_cli.main(
        cli_args(tmp_path, "--allow-available-end-fallback"),
        provider_factory=CapturingProvider,
    )
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert captured["allow_available_end_fallback"] is True
    assert payload["allow_available_end_fallback_requested"] is True
    assert report["quote_window"]["allow_available_end_fallback_requested"] is True


def test_cli_available_end_failure_is_clean_report(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")

    exit_code = databento_quote_cli.main(cli_args(tmp_path), provider_factory=FailingAvailableEndProvider)
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert payload["classification"] == "FAILED_BEFORE_QUOTE"
    assert payload["quote_observed"] is False
    assert payload["provider_available_end"] == "2026-05-01T23:59:00+00:00"
    assert "allow-available-end-fallback" in payload["corrective_message"]
    assert payload["resolved_instrument_id"] == "123456"
    assert payload["quote_request_symbol"] == "123456"
    assert payload["requested_quote_start"] == "2026-05-02T11:55:00+00:00"
    assert payload["actual_quote_end"] == "2026-05-02T12:00:00+00:00"
    assert payload["available_end_fallback_used"] is False
    assert payload["allow_available_end_fallback_requested"] is False
    assert payload["allow_available_end_fallback_effective"] is False
    assert payload["native_databento_path_used"] is True
    assert payload["schemas_attempted"] == ["mbp-1", "trades"]
    assert payload["available_end_retry_attempted"] is False
    assert payload["available_end_retry_reason"] == "fallback_not_enabled"
    assert payload["native_databento_error_code"] == "data_start_after_available_end"
    assert payload["native_exception_class"] == "RuntimeError"
    assert report["classification"] == "FAILED_BEFORE_QUOTE"
    assert report["quote_observed"] is False
    assert report["api_key_value"] is None


def test_cli_quote_parse_failure_includes_parser_diagnostics(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")

    exit_code = databento_quote_cli.main(cli_args(tmp_path), provider_factory=FailingParseProvider)
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert payload["classification"] == "FAILED_BEFORE_QUOTE"
    assert payload["databento_schema"] == {"bid_ask": "mbp-1", "last": "trades"}
    assert payload["records_returned"] == {"bid_ask": 0, "trades": 1}
    assert payload["parser_bid_field_source"] is None
    assert payload["parser_ask_field_source"] is None
    assert payload["parser_last_field_source"] == "price"
    assert payload["no_quote_records_reason"] == "no records returned for schema mbp-1"
    assert payload["resolved_instrument_id"] == "123456"
    assert payload["quote_request_symbol"] == "123456"
    assert payload["quote_request_stype_in"] == "instrument_id"
    assert payload["request_details"]["bid_ask"]["schema"] == "mbp-1"
    assert payload["request_details"]["bid_ask"]["symbol"] == "123456"
    assert report["first_raw_record_keys_or_shape"]["trades"]["root_keys"] == ["price", "ts_event"]


def test_cli_weekend_zero_records_classifies_no_active_session(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")

    exit_code = databento_quote_cli.main(cli_args(tmp_path), provider_factory=FailingWeekendNoRecordsProvider)
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert payload["classification"] == "NO_ACTIVE_SESSION_OR_NO_RECORDS"
    assert payload["quote_observed"] is False
    assert payload["resolved_instrument_id"] == "42008160"
    assert payload["records_returned"] == {"bid_ask": 0, "trades": 0}
    assert payload["session_closed_or_no_records"] is True
    assert payload["no_records_reason"] == "WEEKEND_OR_CLOSED_SESSION"
    assert payload["quote_temporal_scope"] == "CURRENT_AVAILABLE_END"
    assert payload["quote_usable_for_paper_pricing"] is False
    assert payload["quote_usable_for_live_money_readiness"] is False
    assert report["classification"] == "NO_ACTIVE_SESSION_OR_NO_RECORDS"


def test_cli_diagnostic_mode_reports_zero_records_without_traceback(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    args = cli_args(tmp_path)
    index = args.index("--databento-continuous-symbol")
    del args[index : index + 2]
    args.extend(
        [
            "--databento-symbol",
            "MGCM6",
            "--diagnose-records",
            "--diagnostic-start",
            "2026-05-01T13:30:00+00:00",
            "--diagnostic-end",
            "2026-05-01T20:00:00+00:00",
        ]
    )

    exit_code = databento_quote_cli.main(args, diagnostic_transport_factory=lambda: FakeDiagnosticTransport({}))
    payload = json.loads(capsys.readouterr().out)
    report = json.loads(Path(payload["report_json"]).read_text(encoding="utf-8"))

    assert exit_code == 2
    assert payload["classification"] == "ZERO_RECORDS_OR_PROVIDER_ERROR"
    assert report["symbol"] == "MGCM6"
    assert report["stype_in"] == "raw_symbol"
    assert {result["request"]["schema"] for result in report["results"]} == {"mbp-1", "trades"}
    assert all(result["records_returned"] == 0 for result in report["results"])


def test_cli_diagnostic_mode_reports_record_timestamps(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    monkeypatch.setenv("DATABENTO_API_KEY", "test-key")
    args = cli_args(tmp_path)
    index = args.index("--databento-continuous-symbol")
    del args[index : index + 2]
    args.extend(
        [
            "--databento-symbol",
            "42008160",
            "--stype-in",
            "instrument_id",
            "--diagnose-records",
            "--diagnostic-start",
            "2026-05-01T13:30:00+00:00",
            "--diagnostic-end",
            "2026-05-01T20:00:00+00:00",
            "--diagnostic-schema",
            "trades",
        ]
    )

    exit_code = databento_quote_cli.main(
        args,
        diagnostic_transport_factory=lambda: FakeDiagnosticTransport(
            {"trades": ({"ts_event": "2026-05-01T13:31:00+00:00", "price": "4620.0"},)}
        ),
    )
    payload = json.loads(capsys.readouterr().out)

    assert exit_code == 0
    assert payload["classification"] == "RECORDS_FOUND"
    assert payload["results"][0]["request"]["symbol"] == "42008160"
    assert payload["results"][0]["request"]["stype_in"] == "instrument_id"
    assert payload["results"][0]["first_record_timestamp"] == "2026-05-01T13:31:00+00:00"
    assert payload["results"][0]["first_raw_record_keys_or_shape"] == {"root_keys": ["price", "ts_event"]}


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
