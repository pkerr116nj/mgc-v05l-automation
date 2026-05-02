from __future__ import annotations

import inspect
from io import BytesIO
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence
from urllib.error import HTTPError

import pytest

from mgc_v05l.execution_core.databento_quote_provider import (
    DatabentoResolutionStatus,
    DatabentoQuoteProvider,
    DatabentoQuoteProviderConfig,
    DatabentoQuoteProviderError,
    DatabentoSymbolResolution,
    DatabentoSymbolResolutionRequest,
    UrllibDatabentoSymbolResolver,
)
from mgc_v05l.execution_core.pricing import MarketDataMode
from mgc_v05l.execution_core.quote_provider import validate_quote_for_pricing
import mgc_v05l.execution_core.databento_quote_provider as provider_module


def aware_now() -> datetime:
    return datetime(2026, 5, 2, 12, 0, tzinfo=timezone.utc)


class FakeTransport:
    def __init__(
        self,
        *,
        bbo_records: Sequence[Mapping[str, Any]] | None = None,
        trade_records: Sequence[Mapping[str, Any]] | None = None,
    ) -> None:
        self.bbo_records = tuple(
            (
                {
                    "ts_event": "2026-05-02T12:00:00+00:00",
                    "levels": [{"bid_px": "4626.0", "ask_px": "4626.1"}],
                },
            )
            if bbo_records is None
            else bbo_records
        )
        self.trade_records = tuple(({"ts_event": "2026-05-02T12:00:00+00:00", "price": "4626.0"},) if trade_records is None else trade_records)
        self.requests: list[dict[str, Any]] = []

    def request_records(self, **kwargs: Any) -> Sequence[Mapping[str, Any]]:
        self.requests.append(dict(kwargs))
        return self.trade_records if kwargs["schema"] == "trades" else self.bbo_records


class FakeResolver:
    def __init__(self, *resolutions: DatabentoSymbolResolution) -> None:
        self.resolutions = list(resolutions)
        self.requests: list[DatabentoSymbolResolutionRequest] = []

    def resolve(self, *, request: DatabentoSymbolResolutionRequest) -> DatabentoSymbolResolution:
        self.requests.append(request)
        if self.resolutions:
            return self.resolutions.pop(0)
        return resolution(resolved_instrument_id=None, raw_symbol=None, resolution_status=DatabentoResolutionStatus.NOT_FOUND)


def resolution(
    *,
    requested_symbol: str = "MGC.v.0",
    resolved_instrument_id: str | None = "123456",
    raw_symbol: str | None = "MGCM6",
    resolution_status: str = DatabentoResolutionStatus.RESOLVED,
) -> DatabentoSymbolResolution:
    return DatabentoSymbolResolution(
        requested_symbol=requested_symbol,
        dataset="GLBX.MDP3",
        stype_in="continuous",
        stype_out="instrument_id",
        resolved_instrument_id=resolved_instrument_id,
        raw_symbol=raw_symbol,
        resolution_date=date(2026, 5, 2),
        resolution_start=date(2026, 5, 2),
        resolution_end=date(2026, 5, 3),
        resolution_status=resolution_status,
    )


def config(**overrides: object) -> DatabentoQuoteProviderConfig:
    kwargs = {
        "contract_key": "MGC-202606",
        "databento_symbol": "MGCM6",
        "tick_size": "0.1",
        "exchange": "COMEX",
        "currency": "USD",
        "api_key": "test-key",
    }
    kwargs.update(overrides)
    return DatabentoQuoteProviderConfig(**kwargs)


def test_fake_transport_valid_realtime_quote_returns_track_b_snapshot() -> None:
    transport = FakeTransport()
    provider = DatabentoQuoteProvider(config=config(), transport=transport, now=aware_now())

    snapshot = provider.get_quote("MGC-202606")

    assert snapshot.provider == "DATABENTO"
    assert snapshot.mode == MarketDataMode.REALTIME
    assert snapshot.role == "PRIMARY"
    assert snapshot.contract_key == "MGC-202606"
    assert snapshot.provider_symbol == "MGCM6"
    assert snapshot.raw["symbol_source"] == "MANUAL_PROVIDER_SYMBOL_OVERRIDE"
    assert snapshot.raw["execution_contract_validation_status"] == "MANUAL_OVERRIDE_OPERATOR_REVIEW"
    assert str(snapshot.bid) == "4626.0"
    assert str(snapshot.ask) == "4626.1"
    assert str(snapshot.last) == "4626.0"
    assert snapshot.exchange == "COMEX"
    assert snapshot.currency == "USD"
    assert snapshot.delayed_data_warning_seen is False
    assert [request["schema"] for request in transport.requests] == ["mbp-1", "trades"]
    validate_quote_for_pricing(snapshot, now=aware_now(), max_age_seconds=15, allow_delayed_for_paper=False, live_money=True)


def test_exactly_one_symbol_source_is_required() -> None:
    with pytest.raises(DatabentoQuoteProviderError, match="exactly one"):
        DatabentoQuoteProvider(config=config(databento_symbol="", databento_continuous_symbol=""), transport=FakeTransport(), now=aware_now())
    with pytest.raises(DatabentoQuoteProviderError, match="exactly one"):
        DatabentoQuoteProvider(
            config=config(databento_symbol="MGCM6", databento_continuous_symbol="MGC.v.0"),
            transport=FakeTransport(),
            now=aware_now(),
        )


def test_fake_resolver_resolves_continuous_symbol_and_provider_uses_resolved_instrument() -> None:
    transport = FakeTransport()
    resolver = FakeResolver(resolution(raw_symbol=None), resolution(requested_symbol="123456", resolved_instrument_id=None, raw_symbol="MGCM6"))
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allowlisted_local_symbol="MGCM6"),
        transport=transport,
        resolver=resolver,
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert resolver.requests[0].requested_symbol == "MGC.v.0"
    assert resolver.requests[0].stype_in == "continuous"
    assert resolver.requests[0].stype_out == "instrument_id"
    assert resolver.requests[1].requested_symbol == "123456"
    assert resolver.requests[1].stype_in == "instrument_id"
    assert resolver.requests[1].stype_out == "raw_symbol"
    assert {request["symbol"] for request in transport.requests} == {"123456"}
    assert {request["stype_in"] for request in transport.requests} == {"instrument_id"}
    assert snapshot.provider_symbol == "MGCM6"
    assert snapshot.raw["symbol_source"] == "CONTINUOUS_SYMBOL_RESOLUTION"
    assert snapshot.raw["requested_continuous_symbol"] == "MGC.v.0"
    assert snapshot.raw["resolution_path"] == "continuous->instrument_id"
    assert snapshot.raw["resolution_date"] == "2026-05-02"
    assert snapshot.raw["resolution_start"] == "2026-05-02"
    assert snapshot.raw["resolution_end"] == "2026-05-03"
    assert snapshot.raw["raw_symbol_lookup_path"] == "instrument_id->raw_symbol"
    assert snapshot.raw["resolved_instrument_id"] == "123456"
    assert snapshot.raw["resolved_raw_symbol"] == "MGCM6"
    assert snapshot.raw["raw_symbol_match_status"] == "MATCH"
    assert snapshot.raw["quote_request_symbol"] == "123456"
    assert snapshot.raw["quote_request_stype_in"] == "instrument_id"
    assert snapshot.raw["execution_contract_validation_status"] == "MATCHED_ALLOWLISTED_LOCAL_SYMBOL"


def test_date_aware_resolver_selects_active_mapping_for_resolution_date() -> None:
    request = DatabentoSymbolResolutionRequest(
        requested_symbol="MGC.v.0",
        dataset="GLBX.MDP3",
        stype_in="continuous",
        stype_out="instrument_id",
        resolution_date=date(2026, 5, 2),
        resolution_start=date(2026, 5, 1),
        resolution_end=date(2026, 5, 3),
    )
    payload = {
        "result": {
            "MGC.v.0": [
                {"d0": "2026-03-01", "d1": "2026-04-01", "s": "111"},
                {"d0": "2026-04-01", "d1": "2026-06-01", "s": "222"},
                {"d0": "2026-06-01", "d1": "2026-08-01", "s": "333"},
            ]
        }
    }

    resolved = provider_module._resolution_from_payload(request=request, payload=payload)

    assert resolved.resolution_status == DatabentoResolutionStatus.RESOLVED
    assert resolved.resolved_instrument_id == "222"
    assert resolved.resolution_date == date(2026, 5, 2)
    assert resolved.resolution_start == date(2026, 5, 1)
    assert resolved.resolution_end == date(2026, 5, 3)
    assert resolved.active_mapping == {"d0": "2026-04-01", "d1": "2026-06-01", "s": "222"}
    assert len(resolved.mapping_intervals) == 3


def test_date_aware_resolver_reports_no_active_mapping_for_date() -> None:
    request = DatabentoSymbolResolutionRequest(
        requested_symbol="MGC.v.0",
        dataset="GLBX.MDP3",
        stype_in="continuous",
        stype_out="instrument_id",
        resolution_date=date(2026, 5, 2),
    )
    payload = {"result": {"MGC.v.0": [{"d0": "2026-01-01", "d1": "2026-02-01", "s": "111"}]}}

    resolved = provider_module._resolution_from_payload(request=request, payload=payload)

    assert resolved.resolution_status == DatabentoResolutionStatus.NOT_FOUND
    assert resolved.resolved_instrument_id is None
    assert "no active mapping" in resolved.warnings[0]


def test_default_resolution_date_is_applied_from_provider_clock() -> None:
    resolver = FakeResolver(resolution(raw_symbol=None), resolution(requested_symbol="123456", resolved_instrument_id=None, raw_symbol=None))
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allowlisted_local_symbol="MGCM6"),
        transport=FakeTransport(),
        resolver=resolver,
        now=aware_now(),
    )

    provider.get_quote("MGC-202606")

    assert resolver.requests[0].resolution_date == date(2026, 5, 2)
    assert resolver.requests[0].resolution_start is None
    assert resolver.requests[0].resolution_end is None


def test_missing_resolution_blocks_quote() -> None:
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allowlisted_local_symbol="MGCM6"),
        transport=FakeTransport(),
        resolver=FakeResolver(resolution(resolved_instrument_id=None, raw_symbol=None, resolution_status=DatabentoResolutionStatus.NOT_FOUND)),
        now=aware_now(),
    )

    with pytest.raises(DatabentoQuoteProviderError, match="did not resolve"):
        provider.get_quote("MGC-202606")


def test_partial_instrument_only_resolution_reports_incomplete_execution_validation() -> None:
    transport = FakeTransport()
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allowlisted_local_symbol="MGCM6"),
        transport=transport,
        resolver=FakeResolver(resolution(resolved_instrument_id="123456", raw_symbol=None)),
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert {request["symbol"] for request in transport.requests} == {"123456"}
    assert snapshot.provider_symbol == "123456"
    assert snapshot.raw["raw_symbol_match_status"] == "UNAVAILABLE"
    assert snapshot.raw["execution_contract_validation_status"] == "INCOMPLETE_NO_RAW_SYMBOL"
    assert any("incomplete" in warning.lower() for warning in snapshot.provider_warnings)


def test_continuous_raw_symbol_primary_resolution_is_not_attempted() -> None:
    resolver = FakeResolver(resolution(raw_symbol=None), resolution(requested_symbol="123456", resolved_instrument_id=None, raw_symbol="MGCM6"))
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allowlisted_local_symbol="MGCM6"),
        transport=FakeTransport(),
        resolver=resolver,
        now=aware_now(),
    )

    provider.get_quote("MGC-202606")

    assert resolver.requests[0].stype_in == "continuous"
    assert resolver.requests[0].stype_out == "instrument_id"
    assert not (resolver.requests[0].stype_in == "continuous" and resolver.requests[0].stype_out == "raw_symbol")


def test_optional_raw_symbol_lookup_unavailable_does_not_break_instrument_quote_path() -> None:
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allowlisted_local_symbol="MGCM6"),
        transport=FakeTransport(),
        resolver=FakeResolver(
            resolution(resolved_instrument_id="123456", raw_symbol=None),
            resolution(requested_symbol="123456", resolved_instrument_id=None, raw_symbol=None, resolution_status=DatabentoResolutionStatus.NOT_FOUND),
        ),
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert snapshot.raw["quote_request_symbol"] == "123456"
    assert snapshot.raw["raw_symbol_match_status"] == "UNAVAILABLE"
    assert snapshot.raw["execution_contract_validation_status"] == "INCOMPLETE_NO_RAW_SYMBOL"
    assert snapshot.raw["raw_symbol_resolution_status"] == "NOT_FOUND"


def test_raw_symbol_mismatch_against_allowlisted_local_symbol_blocks_quote_readiness() -> None:
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allowlisted_local_symbol="MGCM6"),
        transport=FakeTransport(),
        resolver=FakeResolver(resolution(raw_symbol=None), resolution(requested_symbol="123456", resolved_instrument_id=None, raw_symbol="MGCQ6")),
        now=aware_now(),
    )

    with pytest.raises(DatabentoQuoteProviderError, match="conflicts with IBKR allowlisted local symbol"):
        provider.get_quote("MGC-202606")


def test_manual_raw_symbol_override_remains_supported_and_reported() -> None:
    transport = FakeTransport()
    provider = DatabentoQuoteProvider(config=config(databento_symbol="MGCM6", databento_continuous_symbol=None), transport=transport, now=aware_now())

    snapshot = provider.get_quote("MGC-202606")

    assert {request["symbol"] for request in transport.requests} == {"MGCM6"}
    assert {request["stype_in"] for request in transport.requests} == {"raw_symbol"}
    assert snapshot.raw["symbol_source"] == "MANUAL_PROVIDER_SYMBOL_OVERRIDE"
    assert snapshot.raw["manual_provider_symbol_override"] == "MGCM6"
    assert any("override" in warning.lower() for warning in snapshot.provider_warnings)


def test_unsupported_continuous_to_raw_symbol_http_error_has_corrective_message(monkeypatch: pytest.MonkeyPatch) -> None:
    def fake_urlopen(*args: object, **kwargs: object) -> object:
        raise HTTPError(
            url="https://hist.databento.com/v0/symbology.resolve",
            code=422,
            msg="Unprocessable Entity",
            hdrs={},
            fp=BytesIO(b"unsupported mapping"),
        )

    monkeypatch.setattr(provider_module, "urlopen", fake_urlopen)
    resolver = UrllibDatabentoSymbolResolver(api_key="test-key")

    with pytest.raises(DatabentoQuoteProviderError, match="resolve continuous symbols to instrument_id first"):
        resolver.resolve(
            request=DatabentoSymbolResolutionRequest(
                requested_symbol="MGC.v.0",
                dataset="GLBX.MDP3",
                stype_in="continuous",
                stype_out="raw_symbol",
            )
        )


def test_contract_key_must_match_explicit_mapping() -> None:
    provider = DatabentoQuoteProvider(config=config(), transport=FakeTransport(), now=aware_now())

    with pytest.raises(DatabentoQuoteProviderError, match="contract_key"):
        provider.get_quote("MGC-202608")


def test_stale_quote_is_unknown_and_blocks_live_money_readiness() -> None:
    provider = DatabentoQuoteProvider(
        config=config(),
        transport=FakeTransport(
            bbo_records=({"ts_event": "2026-05-02T11:59:00+00:00", "levels": [{"bid_px": "4626.0", "ask_px": "4626.1"}]},),
            trade_records=({"ts_event": "2026-05-02T11:59:00+00:00", "price": "4626.0"},),
        ),
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert snapshot.mode == MarketDataMode.UNKNOWN
    with pytest.raises(Exception, match="live-money readiness|unknown market data"):
        validate_quote_for_pricing(snapshot, now=aware_now(), max_age_seconds=15, allow_delayed_for_paper=False, live_money=True)


def test_missing_bid_ask_last_blocks_quote_derived_pricing() -> None:
    provider = DatabentoQuoteProvider(
        config=config(),
        transport=FakeTransport(
            bbo_records=({"ts_event": "2026-05-02T12:00:00+00:00", "bid_px": "4626.0", "ask_px": "4626.1"},),
            trade_records=(),
        ),
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert snapshot.last is None
    with pytest.raises(Exception, match="last is required"):
        validate_quote_for_pricing(snapshot, now=aware_now(), max_age_seconds=15, allow_delayed_for_paper=False, live_money=True)


def test_provider_source_has_no_track_a_live_feed_or_schwab_imports() -> None:
    source = inspect.getsource(provider_module)

    assert "live_feed" not in source
    assert "schwab" not in source.lower()
    assert "mgc_v05l.execution" not in source
    assert "mgc_v05l.app" not in source
    assert "dashboard" not in source
    assert "persistence" not in source
