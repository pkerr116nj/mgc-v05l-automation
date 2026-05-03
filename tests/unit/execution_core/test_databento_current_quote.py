from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.databento_current_quote import (
    CurrentQuoteClassification,
    DatabentoCurrentQuoteConfig,
    DatabentoCurrentQuoteProvider,
)


def aware_now() -> datetime:
    return datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc)


def config(tmp_path: Path, **overrides: object) -> DatabentoCurrentQuoteConfig:
    kwargs = {
        "contract_key": "MGC-202606",
        "databento_continuous_symbol": "MGC.v.0",
        "allowlisted_local_symbol": "MGCM6",
        "output_root": tmp_path / "current_quotes",
    }
    kwargs.update(overrides)
    return DatabentoCurrentQuoteConfig(**kwargs)


class FakeCurrentQuoteTransport:
    def __init__(self, quote: Mapping[str, Any] | None = None, *, error: Exception | None = None) -> None:
        self.quote = quote
        self.error = error
        self.requests: list[dict[str, Any]] = []

    def get_current_quote(self, **kwargs: Any) -> Mapping[str, Any] | None:
        self.requests.append(dict(kwargs))
        if self.error is not None:
            raise self.error
        return self.quote


def raw_quote(**overrides: object) -> dict[str, object]:
    quote = {
        "provider_symbol": "MGC.v.0",
        "bid": "4623.1",
        "ask": "4623.3",
        "last": "4623.0",
        "timestamp": aware_now().isoformat(),
    }
    quote.update(overrides)
    return quote


def read_report(result) -> dict[str, Any]:  # type: ignore[no-untyped-def]
    return json.loads(result.report_json.read_text(encoding="utf-8"))


def test_fresh_current_quote_is_provider_agnostic_and_available(tmp_path: Path) -> None:
    transport = FakeCurrentQuoteTransport(raw_quote())
    provider = DatabentoCurrentQuoteProvider(config=config(tmp_path), transport=transport)

    result = provider.fetch_current_quote(run_id="current-fresh", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.AVAILABLE
    assert result.quote is not None
    assert result.quote.provider == "DATABENTO"
    assert result.quote.contract_key == "MGC-202606"
    assert payload["classification"] == "CURRENT_QUOTE_AVAILABLE"
    assert payload["quote_status"] == "CURRENT_QUOTE_AVAILABLE"
    assert payload["bid"] == "4623.1"
    assert payload["ask"] == "4623.3"
    assert payload["last"] == "4623.0"
    assert payload["timestamp"] == aware_now().isoformat()
    assert payload["market_data_provider"] == "DATABENTO"
    assert payload["market_data_mode"] == "REALTIME"
    assert payload["market_data_role"] == "PRIMARY"
    assert payload["current_quote_available"] is True
    assert payload["quote_usable_for_paper_pricing"] is True
    assert payload["quote_usable_for_live_money_readiness"] is False
    assert payload["production_live_money_readiness"] is False
    assert payload["local_execution_contract_key"] == "MGC-202606"
    assert payload["ibkr_allowlist_remains_execution_authority"] is True
    assert payload["databento_continuous_symbol_is_execution_authority"] is False
    assert transport.requests == [
        {
            "dataset": "GLBX.MDP3",
            "symbol": "MGC.v.0",
            "stype_in": "continuous",
            "schema": "mbp-1",
            "contract_key": "MGC-202606",
        }
    ]


def test_stale_current_quote_is_classified_stale(tmp_path: Path) -> None:
    provider = DatabentoCurrentQuoteProvider(
        config=config(tmp_path, max_age_seconds=15),
        transport=FakeCurrentQuoteTransport(raw_quote(timestamp=(aware_now() - timedelta(seconds=16)).isoformat())),
    )

    result = provider.fetch_current_quote(run_id="current-stale", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.STALE
    assert payload["classification"] == "CURRENT_QUOTE_STALE"
    assert payload["current_quote_available"] is False
    assert payload["quote_usable_for_paper_pricing"] is False
    assert payload["quote_usable_for_live_money_readiness"] is False


def test_no_record_current_quote_is_market_closed_or_no_records(tmp_path: Path) -> None:
    provider = DatabentoCurrentQuoteProvider(config=config(tmp_path), transport=FakeCurrentQuoteTransport(None))

    result = provider.fetch_current_quote(run_id="current-none", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.MARKET_CLOSED_OR_NO_RECORDS
    assert payload["classification"] == "CURRENT_QUOTE_MARKET_CLOSED_OR_NO_RECORDS"
    assert payload["quote_observed"] is False
    assert payload["no_records_reason"] == "MARKET_CLOSED_OR_NO_RECORDS"
    assert payload["provider_error"] is None


def test_provider_exception_becomes_provider_error_report(tmp_path: Path) -> None:
    provider = DatabentoCurrentQuoteProvider(
        config=config(tmp_path),
        transport=FakeCurrentQuoteTransport(error=RuntimeError("provider temporarily unavailable")),
    )

    result = provider.fetch_current_quote(run_id="current-error", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.PROVIDER_ERROR
    assert payload["classification"] == "CURRENT_QUOTE_PROVIDER_ERROR"
    assert payload["provider_error"] == "provider temporarily unavailable"
    assert payload["quote_observed"] is False


def test_incomplete_current_quote_is_unavailable_without_crashing(tmp_path: Path) -> None:
    provider = DatabentoCurrentQuoteProvider(
        config=config(tmp_path),
        transport=FakeCurrentQuoteTransport(raw_quote(ask=None)),
    )

    result = provider.fetch_current_quote(run_id="current-incomplete", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.UNAVAILABLE
    assert payload["classification"] == "CURRENT_QUOTE_UNAVAILABLE"
    assert payload["provider_error"] == "ask is required"
    assert payload["no_records_reason"] == "UNAVAILABLE_OR_INCOMPLETE_QUOTE"


def test_raw_symbol_override_remains_market_data_selector_only(tmp_path: Path) -> None:
    provider = DatabentoCurrentQuoteProvider(
        config=config(
            tmp_path,
            databento_continuous_symbol=None,
            databento_symbol="MGCM6",
            stype_in="raw_symbol",
        ),
        transport=FakeCurrentQuoteTransport(raw_quote(provider_symbol="MGCM6")),
    )

    result = provider.fetch_current_quote(run_id="current-raw-symbol", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.AVAILABLE
    assert payload["symbol_selector"] == "MGCM6"
    assert payload["symbol_selector_source"] == "DATABENTO_RAW_SYMBOL_OVERRIDE"
    assert payload["local_execution_contract_key"] == "MGC-202606"
    assert payload["ibkr_allowlist_remains_execution_authority"] is True
    assert payload["databento_continuous_symbol_is_execution_authority"] is False
