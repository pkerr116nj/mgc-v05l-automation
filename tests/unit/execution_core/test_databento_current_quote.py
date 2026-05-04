from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping

from mgc_v05l.execution_core.databento_current_quote import (
    CurrentQuoteClassification,
    DatabentoCurrentQuoteConfig,
    DatabentoCurrentQuoteProvider,
    DatabentoQuoteProviderCurrentQuoteTransport,
)
from mgc_v05l.execution_core.quote_provider import QuoteSnapshot


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


def test_available_end_provider_error_preserves_window_diagnostics(tmp_path: Path) -> None:
    error = RuntimeError("requested quote window is after Databento available_end")
    error.diagnostics = {  # type: ignore[attr-defined]
        "requested_quote_start": "2026-05-03T03:55:00+00:00",
        "requested_quote_end": "2026-05-03T04:00:00+00:00",
        "actual_quote_start": "2026-05-03T03:55:00+00:00",
        "actual_quote_end": "2026-05-03T04:00:00+00:00",
        "provider_available_end": "2026-05-03T03:50:00+00:00",
        "provider_available_end_final": "2026-05-03T03:50:00+00:00",
        "available_end_fallback_used": False,
        "allow_available_end_fallback_requested": False,
        "allow_available_end_fallback_effective": False,
        "available_end_retry_attempted": False,
        "available_end_retry_count": 0,
        "available_end_retry_reason": "fallback_not_enabled",
        "native_databento_error_code": "data_start_after_available_end",
    }
    provider = DatabentoCurrentQuoteProvider(
        config=config(tmp_path),
        transport=FakeCurrentQuoteTransport(error=error),
    )

    result = provider.fetch_current_quote(run_id="current-available-end-error", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.PROVIDER_ERROR
    assert payload["classification"] == "CURRENT_QUOTE_PROVIDER_ERROR"
    assert payload["provider_error"] == "requested quote window is after Databento available_end"
    assert payload["requested_quote_end"] == "2026-05-03T04:00:00+00:00"
    assert payload["actual_quote_end"] == "2026-05-03T04:00:00+00:00"
    assert payload["provider_available_end"] == "2026-05-03T03:50:00+00:00"
    assert payload["available_end_fallback_used"] is False
    assert payload["available_end_retry_reason"] == "fallback_not_enabled"
    assert payload["native_databento_error_code"] == "data_start_after_available_end"
    assert payload["current_quote_available"] is False
    assert payload["quote_usable_for_paper_pricing"] is False
    assert payload["quote_usable_for_live_money_readiness"] is False
    assert payload["place_order_called"] is False
    assert payload["cancel_called"] is False


def test_available_end_fallback_diagnostics_are_preserved_without_readiness(tmp_path: Path) -> None:
    provider = DatabentoCurrentQuoteProvider(
        config=config(tmp_path, max_age_seconds=15),
        transport=FakeCurrentQuoteTransport(
            raw_quote(
                timestamp=(aware_now() - timedelta(minutes=5)).isoformat(),
                raw={
                    "requested_quote_start": "2026-05-03T03:55:00+00:00",
                    "requested_quote_end": "2026-05-03T04:00:00+00:00",
                    "actual_quote_start": "2026-05-03T03:40:00+00:00",
                    "actual_quote_end": "2026-05-03T03:45:00+00:00",
                    "provider_available_end": "2026-05-03T03:50:00+00:00",
                    "provider_available_end_final": "2026-05-03T03:50:00+00:00",
                    "available_end_fallback_used": True,
                    "allow_available_end_fallback_requested": True,
                    "allow_available_end_fallback_effective": True,
                    "available_end_retry_attempted": True,
                    "available_end_retry_count": 1,
                    "available_end_retry_reason": "initial_data_start_after_available_end",
                    "quote_temporal_scope": "CURRENT_AVAILABLE_END",
                    "active_session_quote": False,
                },
            )
        ),
    )

    result = provider.fetch_current_quote(run_id="current-available-end-fallback", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.STALE
    assert payload["current_quote_available"] is False
    assert payload["requested_quote_end"] == "2026-05-03T04:00:00+00:00"
    assert payload["actual_quote_end"] == "2026-05-03T03:45:00+00:00"
    assert payload["provider_available_end"] == "2026-05-03T03:50:00+00:00"
    assert payload["available_end_fallback_used"] is True
    assert payload["available_end_retry_count"] == 1
    assert payload["quote_temporal_scope"] == "CURRENT_AVAILABLE_END"
    assert payload["active_session_quote"] is False
    assert payload["quote_usable_for_paper_pricing"] is False
    assert payload["quote_usable_for_live_money_readiness"] is False
    assert payload["quote_freshness_verdict"] == "CURRENT_QUOTE_FRESHNESS_BLOCKED_FALLBACK_WITHOUT_EXPLICIT_TOLERANCE"


def test_available_end_fallback_within_explicit_freshness_tolerance_is_current_enough(tmp_path: Path) -> None:
    provider = DatabentoCurrentQuoteProvider(
        config=config(tmp_path, max_age_seconds=15, max_current_quote_age_seconds=300),
        transport=FakeCurrentQuoteTransport(
            raw_quote(
                timestamp=(aware_now() - timedelta(minutes=5)).isoformat(),
                raw={
                    "requested_quote_end": "2026-05-01T20:00:00+00:00",
                    "actual_quote_end": "2026-05-01T19:56:00+00:00",
                    "provider_available_end": "2026-05-01T19:56:00+00:00",
                    "provider_available_end_final": "2026-05-01T19:56:00+00:00",
                    "available_end_fallback_used": True,
                    "allow_available_end_fallback_requested": True,
                    "quote_temporal_scope": "CURRENT_AVAILABLE_END",
                    "active_session_quote": False,
                },
            )
        ),
    )

    result = provider.fetch_current_quote(run_id="current-available-end-fresh-enough", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.AVAILABLE
    assert payload["classification"] == "CURRENT_QUOTE_AVAILABLE"
    assert payload["current_quote_available"] is True
    assert payload["quote_usable_for_paper_pricing"] is True
    assert payload["quote_usable_for_live_money_readiness"] is False
    assert payload["max_current_quote_age_seconds"] == 300
    assert payload["quote_age_seconds"] == "240.0"
    assert payload["quote_freshness_verdict"] == "CURRENT_QUOTE_FRESHNESS_ACCEPTED_AVAILABLE_END_WITHIN_TOLERANCE"
    assert payload["provider_available_end"] == "2026-05-01T19:56:00+00:00"
    assert payload["requested_quote_end"] == "2026-05-01T20:00:00+00:00"
    assert payload["submit_attempted"] is False
    assert payload["live_money_readiness"] is False


def test_available_end_fallback_outside_explicit_freshness_tolerance_blocks(tmp_path: Path) -> None:
    provider = DatabentoCurrentQuoteProvider(
        config=config(tmp_path, max_age_seconds=15, max_current_quote_age_seconds=300),
        transport=FakeCurrentQuoteTransport(
            raw_quote(
                timestamp=(aware_now() - timedelta(minutes=8)).isoformat(),
                raw={
                    "requested_quote_end": "2026-05-01T20:00:00+00:00",
                    "actual_quote_end": "2026-05-01T19:54:00+00:00",
                    "provider_available_end": "2026-05-01T19:54:00+00:00",
                    "provider_available_end_final": "2026-05-01T19:54:00+00:00",
                    "available_end_fallback_used": True,
                    "allow_available_end_fallback_requested": True,
                },
            )
        ),
    )

    result = provider.fetch_current_quote(run_id="current-available-end-too-old", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.STALE
    assert payload["current_quote_available"] is False
    assert payload["quote_age_seconds"] == "360.0"
    assert payload["quote_freshness_verdict"] == "CURRENT_QUOTE_FRESHNESS_BLOCKED_AVAILABLE_END_OUTSIDE_TOLERANCE"
    assert payload["quote_usable_for_paper_pricing"] is False


def test_available_end_fallback_with_missing_available_end_blocks_even_with_tolerance(tmp_path: Path) -> None:
    provider = DatabentoCurrentQuoteProvider(
        config=config(tmp_path, max_age_seconds=15, max_current_quote_age_seconds=300),
        transport=FakeCurrentQuoteTransport(
            raw_quote(
                timestamp=(aware_now() - timedelta(minutes=1)).isoformat(),
                raw={
                    "requested_quote_end": "2026-05-01T20:00:00+00:00",
                    "available_end_fallback_used": True,
                    "allow_available_end_fallback_requested": True,
                },
            )
        ),
    )

    result = provider.fetch_current_quote(run_id="current-available-end-missing", now=aware_now())
    payload = read_report(result)

    assert result.classification == CurrentQuoteClassification.STALE
    assert payload["current_quote_available"] is False
    assert payload["quote_freshness_verdict"] == "CURRENT_QUOTE_FRESHNESS_BLOCKED_MISSING_PROVIDER_AVAILABLE_END"


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


def test_current_quote_transport_reuses_databento_quote_provider_config() -> None:
    requests: list[Any] = []

    class FakeQuoteProvider:
        def __init__(self, cfg: Any) -> None:
            requests.append(cfg)

        def get_quote(self, contract_key: str) -> QuoteSnapshot:
            assert contract_key == "MGC-202606"
            return QuoteSnapshot(
                provider="DATABENTO",
                mode="REALTIME",
                role="PRIMARY",
                contract_key="MGC-202606",
                provider_symbol="MGCM6",
                bid="4623.1",
                ask="4623.3",
                last="4623.2",
                timestamp=aware_now(),
                tick_size="0.1",
                exchange="COMEX",
                currency="USD",
                provider_warnings=("market data only",),
                delayed_data_warning_seen=False,
                raw={"resolved_raw_symbol": "MGCM6"},
            )

    transport = DatabentoQuoteProviderCurrentQuoteTransport(
        api_key="not-printed",
        allowlisted_local_symbol="MGCM6",
        tick_size="0.1",
        exchange="COMEX",
        currency="USD",
        max_age_seconds=15,
        lookback_seconds=120,
        quote_provider_factory=FakeQuoteProvider,
    )

    quote = transport.get_current_quote(
        dataset="GLBX.MDP3",
        symbol="MGC.v.0",
        stype_in="continuous",
        schema="mbp-1",
        contract_key="MGC-202606",
    )

    assert quote is not None
    assert quote["provider_symbol"] == "MGCM6"
    assert quote["bid"] == "4623.1"
    assert quote["ask"] == "4623.3"
    assert quote["last"] == "4623.2"
    assert quote["timestamp"] == aware_now().isoformat()
    assert requests[0].contract_key == "MGC-202606"
    assert requests[0].databento_continuous_symbol == "MGC.v.0"
    assert requests[0].databento_symbol is None
    assert requests[0].allowlisted_local_symbol == "MGCM6"
    assert requests[0].lookback_seconds == 120
    assert requests[0].api_key == "not-printed"
