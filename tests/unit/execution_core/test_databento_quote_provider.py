from __future__ import annotations

import builtins
import inspect
from io import BytesIO
from datetime import date, datetime, timezone
from typing import Any, Mapping, Sequence
from urllib.error import HTTPError

import pytest

from mgc_v05l.execution_core.databento_quote_provider import (
    DatabentoAvailableEndError,
    DatabentoQuoteParseError,
    DatabentoResolutionStatus,
    DatabentoQuoteProvider,
    DatabentoQuoteProviderConfig,
    DatabentoQuoteProviderError,
    NativeDatabentoQuoteTransport,
    DatabentoRecordDiagnosticRequest,
    DatabentoSymbolResolution,
    DatabentoSymbolResolutionRequest,
    UrllibDatabentoSymbolResolver,
    run_databento_record_diagnostic,
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
        self.errors: list[Exception] = []

    def request_records(self, **kwargs: Any) -> Sequence[Mapping[str, Any]]:
        self.requests.append(dict(kwargs))
        if self.errors:
            raise self.errors.pop(0)
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


class FakeDataFrame:
    def __init__(self, records: Sequence[Mapping[str, Any]]) -> None:
        self.records = tuple(records)
        self.empty = not self.records

    def reset_index(self) -> "FakeDataFrame":
        return self

    def to_dict(self, orient: str = "records") -> list[dict[str, Any]]:
        assert orient == "records"
        return [dict(record) for record in self.records]


class FakeDBNStore:
    def __init__(self, records: Sequence[Mapping[str, Any]]) -> None:
        self.records = records

    def to_df(self) -> FakeDataFrame:
        return FakeDataFrame(self.records)


class FakeNativeTimeseries:
    def __init__(self, records_by_schema: Mapping[str, Sequence[Mapping[str, Any]]], errors: Sequence[Exception] = ()) -> None:
        self.records_by_schema = records_by_schema
        self.errors = list(errors)
        self.requests: list[dict[str, Any]] = []

    def get_range(self, **kwargs: Any) -> FakeDBNStore:
        self.requests.append(dict(kwargs))
        if self.errors:
            raise self.errors.pop(0)
        return FakeDBNStore(self.records_by_schema.get(str(kwargs["schema"]), ()))


class FakeNativeClient:
    def __init__(self, records_by_schema: Mapping[str, Sequence[Mapping[str, Any]]], errors: Sequence[Exception] = ()) -> None:
        self.timeseries = FakeNativeTimeseries(records_by_schema, errors=errors)


def resolution(
    *,
    requested_symbol: str = "MGC.v.0",
    resolved_instrument_id: str | None = "123456",
    raw_symbol: str | None = "MGCM6",
    resolution_date: date = date(2026, 5, 2),
    resolution_start: date = date(2026, 5, 2),
    resolution_end: date = date(2026, 5, 3),
    resolution_status: str = DatabentoResolutionStatus.RESOLVED,
) -> DatabentoSymbolResolution:
    return DatabentoSymbolResolution(
        requested_symbol=requested_symbol,
        dataset="GLBX.MDP3",
        stype_in="continuous",
        stype_out="instrument_id",
        resolved_instrument_id=resolved_instrument_id,
        raw_symbol=raw_symbol,
        resolution_date=resolution_date,
        resolution_start=resolution_start,
        resolution_end=resolution_end,
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
    assert snapshot.raw["databento_schema"] == {"bid_ask": "mbp-1", "last": "trades"}
    assert snapshot.raw["records_returned"] == {"bid_ask": 1, "trades": 1}
    assert snapshot.raw["first_raw_record_keys_or_shape"]["bid_ask"]["levels[0]_keys"] == ["ask_px", "bid_px"]
    assert snapshot.raw["parser_bid_field_source"] == "levels[0].bid_px"
    assert snapshot.raw["parser_ask_field_source"] == "levels[0].ask_px"
    assert snapshot.raw["parser_last_field_source"] == "price"
    assert snapshot.raw["no_quote_records_reason"] is None
    assert snapshot.exchange == "COMEX"
    assert snapshot.currency == "USD"
    assert snapshot.delayed_data_warning_seen is False
    assert [request["schema"] for request in transport.requests] == ["mbp-1", "trades"]
    validate_quote_for_pricing(snapshot, now=aware_now(), max_age_seconds=15, allow_delayed_for_paper=False, live_money=True)


def test_native_client_mbp1_dataframe_columns_parse_bid_ask() -> None:
    fake_client = FakeNativeClient(
        {
            "mbp-1": (
                {
                    "ts_event": "2026-05-02T12:00:00+00:00",
                    "bid_px_00": "4626.0",
                    "ask_px_00": "4626.1",
                },
            ),
            "trades": ({"ts_event": "2026-05-02T12:00:00+00:00", "price": "4626.0"},),
        }
    )
    transport = NativeDatabentoQuoteTransport(client_factory=lambda api_key: fake_client)
    provider = DatabentoQuoteProvider(config=config(), transport=transport, now=aware_now())

    snapshot = provider.get_quote("MGC-202606")

    assert str(snapshot.bid) == "4626.0"
    assert str(snapshot.ask) == "4626.1"
    assert snapshot.raw["parser_bid_field_source"] == "bid_px_00"
    assert snapshot.raw["parser_ask_field_source"] == "ask_px_00"
    assert [request["schema"] for request in fake_client.timeseries.requests] == ["mbp-1", "trades"]
    assert fake_client.timeseries.requests[0]["symbols"] == ["MGCM6"]
    assert fake_client.timeseries.requests[0]["stype_in"] == "raw_symbol"


def test_native_client_trades_dataframe_parses_last() -> None:
    fake_client = FakeNativeClient(
        {
            "mbp-1": ({"ts_event": "2026-05-02T12:00:00+00:00", "bid_px_00": "4626.0", "ask_px_00": "4626.1"},),
            "trades": ({"ts_event": "2026-05-02T12:00:01+00:00", "price": "4626.2"},),
        }
    )
    provider = DatabentoQuoteProvider(
        config=config(),
        transport=NativeDatabentoQuoteTransport(client_factory=lambda api_key: fake_client),
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert str(snapshot.last) == "4626.2"
    assert snapshot.raw["parser_last_field_source"] == "price"


def test_native_client_empty_dataframe_fails_cleanly() -> None:
    fake_client = FakeNativeClient({"mbp-1": (), "trades": ()})
    provider = DatabentoQuoteProvider(
        config=config(),
        transport=NativeDatabentoQuoteTransport(client_factory=lambda api_key: fake_client),
        now=aware_now(),
    )

    with pytest.raises(DatabentoQuoteParseError, match="no bid/ask quote records") as exc_info:
        provider.get_quote("MGC-202606")

    assert exc_info.value.diagnostics["records_returned"] == {"bid_ask": 0, "trades": 0}


def native_available_end_error() -> RuntimeError:
    return RuntimeError(
        "422 data_start_after_available_end\n"
        "`start` in query ('2026-05-02 23:05:00.000000+00:00') was after the available end "
        "of dataset GLBX.MDP3 ('2026-05-02 23:00:00+00:00'). Try requesting with an earlier `start`."
    )


def test_native_client_available_end_error_is_caught() -> None:
    fake_client = FakeNativeClient({}, errors=(native_available_end_error(),))
    transport = NativeDatabentoQuoteTransport(client_factory=lambda api_key: fake_client)

    with pytest.raises(DatabentoAvailableEndError) as exc_info:
        transport.request_records(
            base_url="https://hist.databento.com/v0",
            api_key="test-key",
            dataset="GLBX.MDP3",
            symbol="MGCM6",
            schema="mbp-1",
            start=datetime(2026, 5, 2, 23, 5, tzinfo=timezone.utc),
            end=datetime(2026, 5, 2, 23, 10, tzinfo=timezone.utc),
            stype_in="raw_symbol",
            limit=1000,
        )

    assert exc_info.value.provider_available_end == datetime(2026, 5, 2, 23, 0, tzinfo=timezone.utc)
    assert "data_start_after_available_end" in exc_info.value.detail


def test_native_available_end_fallback_retries_once_for_mbp1_and_trades() -> None:
    fake_client = FakeNativeClient(
        {
            "mbp-1": ({"ts_event": "2026-05-02T22:59:59+00:00", "bid_px_00": "4626.0", "ask_px_00": "4626.1"},),
            "trades": ({"ts_event": "2026-05-02T22:59:58+00:00", "price": "4626.0"},),
        },
        errors=(native_available_end_error(),),
    )
    provider = DatabentoQuoteProvider(
        config=config(allow_available_end_fallback=True),
        transport=NativeDatabentoQuoteTransport(client_factory=lambda api_key: fake_client),
        now=datetime(2026, 5, 2, 23, 10, tzinfo=timezone.utc),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert [request["schema"] for request in fake_client.timeseries.requests] == ["mbp-1", "mbp-1", "trades"]
    assert fake_client.timeseries.requests[1]["end"] == "2026-05-02T23:00:00+00:00"
    assert fake_client.timeseries.requests[2]["end"] == "2026-05-02T23:00:00+00:00"
    assert snapshot.raw["provider_available_end"] == "2026-05-02T23:00:00+00:00"
    assert snapshot.raw["available_end_fallback_used"] is True
    assert snapshot.raw["allow_available_end_fallback_requested"] is True
    assert snapshot.raw["allow_available_end_fallback_effective"] is True
    assert snapshot.raw["native_databento_path_used"] is True
    assert snapshot.raw["schemas_attempted"] == ["mbp-1", "trades"]
    assert snapshot.raw["available_end_retry_attempted"] is True
    assert snapshot.raw["available_end_retry_reason"] == "initial_data_start_after_available_end"
    assert snapshot.raw["actual_quote_end"] == "2026-05-02T23:00:00+00:00"
    assert snapshot.raw["usable_for_live_money_readiness"] is False
    assert snapshot.mode == MarketDataMode.UNKNOWN


def test_native_available_end_without_fallback_fails_with_request_diagnostics() -> None:
    fake_client = FakeNativeClient({}, errors=(native_available_end_error(),))
    provider = DatabentoQuoteProvider(
        config=config(allow_available_end_fallback=False),
        transport=NativeDatabentoQuoteTransport(client_factory=lambda api_key: fake_client),
        now=datetime(2026, 5, 2, 23, 10, tzinfo=timezone.utc),
    )

    with pytest.raises(DatabentoAvailableEndError) as exc_info:
        provider.get_quote("MGC-202606")

    diagnostics = exc_info.value.diagnostics
    assert diagnostics["provider_available_end"] == "2026-05-02T23:00:00+00:00"
    assert diagnostics["available_end_fallback_used"] is False
    assert diagnostics["quote_request_symbol"] == "MGCM6"
    assert diagnostics["quote_request_stype_in"] == "raw_symbol"
    assert diagnostics["requested_quote_start"] == "2026-05-02T23:05:00+00:00"
    assert diagnostics["requested_quote_end"] == "2026-05-02T23:10:00+00:00"
    assert diagnostics["actual_quote_start"] == "2026-05-02T23:05:00+00:00"
    assert diagnostics["actual_quote_end"] == "2026-05-02T23:10:00+00:00"
    assert diagnostics["allow_available_end_fallback_requested"] is False
    assert diagnostics["allow_available_end_fallback_effective"] is False
    assert diagnostics["native_databento_path_used"] is True
    assert diagnostics["schemas_attempted"] == ["mbp-1", "trades"]
    assert diagnostics["available_end_retry_attempted"] is False
    assert diagnostics["available_end_retry_reason"] == "fallback_not_enabled"
    assert diagnostics["native_databento_error_code"] == "data_start_after_available_end"
    assert diagnostics["native_exception_class"] == "RuntimeError"
    assert "available end" in diagnostics["native_databento_error_message"]
    assert "available end" in diagnostics["native_exception_message_sanitized"]


def test_native_available_end_retry_failure_has_fallback_diagnostics() -> None:
    fake_client = FakeNativeClient({}, errors=(native_available_end_error(), native_available_end_error()))
    provider = DatabentoQuoteProvider(
        config=config(allow_available_end_fallback=True),
        transport=NativeDatabentoQuoteTransport(client_factory=lambda api_key: fake_client),
        now=datetime(2026, 5, 2, 23, 10, tzinfo=timezone.utc),
    )

    with pytest.raises(DatabentoAvailableEndError) as exc_info:
        provider.get_quote("MGC-202606")

    diagnostics = exc_info.value.diagnostics
    assert [request["schema"] for request in fake_client.timeseries.requests] == ["mbp-1", "mbp-1"]
    assert diagnostics["provider_available_end"] == "2026-05-02T23:00:00+00:00"
    assert diagnostics["allow_available_end_fallback_requested"] is True
    assert diagnostics["allow_available_end_fallback_effective"] is True
    assert diagnostics["native_databento_path_used"] is True
    assert diagnostics["schemas_attempted"] == ["mbp-1", "trades"]
    assert diagnostics["available_end_fallback_used"] is True
    assert diagnostics["available_end_retry_attempted"] is True
    assert diagnostics["available_end_retry_reason"] == "initial_data_start_after_available_end"
    assert diagnostics["requested_quote_start"] == "2026-05-02T23:05:00+00:00"
    assert diagnostics["requested_quote_end"] == "2026-05-02T23:10:00+00:00"
    assert diagnostics["actual_quote_start"] == "2026-05-02T22:55:00+00:00"
    assert diagnostics["actual_quote_end"] == "2026-05-02T23:00:00+00:00"
    assert diagnostics["native_exception_class"] == "RuntimeError"


def test_missing_native_databento_package_has_clear_message(monkeypatch: pytest.MonkeyPatch) -> None:
    real_import = builtins.__import__

    def fake_import(name: str, *args: object, **kwargs: object) -> object:
        if name == "databento":
            raise ModuleNotFoundError("No module named 'databento'")
        return real_import(name, *args, **kwargs)

    monkeypatch.setattr(builtins, "__import__", fake_import)

    with pytest.raises(DatabentoQuoteProviderError, match=r"pip install -e .*databento"):
        provider_module._native_historical_client("test-key")


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
    assert snapshot.raw["requested_resolution_date"] == "2026-05-02"
    assert snapshot.raw["actual_resolution_date_used"] == "2026-05-02"
    assert snapshot.raw["prior_session_fallback_used"] is False
    assert snapshot.raw["fallback_lookback_days"] == 3
    assert snapshot.raw["resolution_session_type"] == "CURRENT_SESSION"
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


def test_provider_fails_no_active_mapping_without_prior_session_fallback() -> None:
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", resolution_date="2026-05-02"),
        transport=FakeTransport(),
        resolver=FakeResolver(
            resolution(
                resolved_instrument_id=None,
                raw_symbol=None,
                resolution_status=DatabentoResolutionStatus.NOT_FOUND,
                resolution_date=date(2026, 5, 2),
            )
        ),
        now=aware_now(),
    )

    with pytest.raises(DatabentoQuoteProviderError, match="NOT_FOUND"):
        provider.get_quote("MGC-202606")


def test_provider_prior_session_fallback_selects_first_active_prior_mapping() -> None:
    resolver = FakeResolver(
        resolution(
            resolved_instrument_id=None,
            raw_symbol=None,
            resolution_status=DatabentoResolutionStatus.NOT_FOUND,
            resolution_date=date(2026, 5, 2),
        ),
        resolution(
            resolved_instrument_id="42008160",
            raw_symbol=None,
            resolution_date=date(2026, 5, 1),
            resolution_start=date(2026, 5, 1),
            resolution_end=date(2026, 5, 2),
        ),
        resolution(
            requested_symbol="42008160",
            resolved_instrument_id=None,
            raw_symbol=None,
            resolution_status=DatabentoResolutionStatus.NOT_FOUND,
            resolution_date=date(2026, 5, 1),
        ),
    )
    provider = DatabentoQuoteProvider(
        config=config(
            databento_symbol=None,
            databento_continuous_symbol="MGC.v.0",
            allowlisted_local_symbol="MGCM6",
            resolution_date="2026-05-02",
            allow_prior_session_resolution=True,
        ),
        transport=FakeTransport(),
        resolver=resolver,
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert [request.resolution_date for request in resolver.requests[:2]] == [date(2026, 5, 2), date(2026, 5, 1)]
    assert snapshot.raw["requested_resolution_date"] == "2026-05-02"
    assert snapshot.raw["actual_resolution_date_used"] == "2026-05-01"
    assert snapshot.raw["prior_session_fallback_used"] is True
    assert snapshot.raw["fallback_lookback_days"] == 3
    assert snapshot.raw["resolution_session_type"] == "PRIOR_SESSION_RESOLUTION_FALLBACK"
    assert snapshot.raw["resolved_instrument_id"] == "42008160"
    assert any("prior-session" in warning.lower() for warning in snapshot.provider_warnings)


def test_available_end_error_without_fallback_is_preserved() -> None:
    transport = FakeTransport()
    provider_available_end = datetime(2026, 5, 1, 23, 59, tzinfo=timezone.utc)
    transport.errors.append(
        DatabentoAvailableEndError(
            "requested quote window is after Databento available_end; rerun with --allow-available-end-fallback or earlier --quote-end-timestamp",
            provider_available_end=provider_available_end,
            detail="sanitized detail",
        )
    )
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0"),
        transport=transport,
        resolver=FakeResolver(resolution(raw_symbol=None)),
        now=aware_now(),
    )

    with pytest.raises(DatabentoAvailableEndError) as exc_info:
        provider.get_quote("MGC-202606")

    assert exc_info.value.provider_available_end == provider_available_end


def test_available_end_fallback_retries_once_with_provider_available_end() -> None:
    transport = FakeTransport()
    provider_available_end = datetime(2026, 5, 1, 23, 59, tzinfo=timezone.utc)
    transport.errors.append(
        DatabentoAvailableEndError(
            "requested quote window is after Databento available_end; rerun with --allow-available-end-fallback or earlier --quote-end-timestamp",
            provider_available_end=provider_available_end,
            detail="sanitized detail",
        )
    )
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allow_available_end_fallback=True),
        transport=transport,
        resolver=FakeResolver(resolution(raw_symbol=None), resolution(requested_symbol="123456", resolved_instrument_id=None, raw_symbol=None)),
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert len(transport.requests) == 3
    assert transport.requests[0]["end"] == aware_now()
    assert transport.requests[1]["end"] == provider_available_end
    assert transport.requests[2]["end"] == provider_available_end
    assert snapshot.raw["provider_available_end"] == provider_available_end.isoformat()
    assert snapshot.raw["available_end_fallback_used"] is True
    assert snapshot.raw["allow_available_end_fallback_requested"] is True
    assert snapshot.raw["allow_available_end_fallback_effective"] is True
    assert snapshot.raw["available_end_retry_attempted"] is True
    assert snapshot.raw["available_end_retry_reason"] == "initial_data_start_after_available_end"
    assert snapshot.raw["actual_quote_end"] == provider_available_end.isoformat()
    assert snapshot.raw["usable_for_live_money_readiness"] is False
    assert snapshot.mode == MarketDataMode.UNKNOWN


def test_available_end_fallback_retry_failure_does_not_loop() -> None:
    transport = FakeTransport()
    provider_available_end = datetime(2026, 5, 1, 23, 59, tzinfo=timezone.utc)
    for _ in range(2):
        transport.errors.append(
            DatabentoAvailableEndError(
                "requested quote window is after Databento available_end; rerun with --allow-available-end-fallback or earlier --quote-end-timestamp",
                provider_available_end=provider_available_end,
                detail="sanitized detail",
            )
        )
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allow_available_end_fallback=True),
        transport=transport,
        resolver=FakeResolver(resolution(raw_symbol=None)),
        now=aware_now(),
    )

    with pytest.raises(DatabentoAvailableEndError) as exc_info:
        provider.get_quote("MGC-202606")

    assert len(transport.requests) == 2
    diagnostics = exc_info.value.diagnostics
    assert diagnostics["provider_available_end"] == provider_available_end.isoformat()
    assert diagnostics["allow_available_end_fallback_requested"] is True
    assert diagnostics["allow_available_end_fallback_effective"] is True
    assert diagnostics["available_end_fallback_used"] is True
    assert diagnostics["available_end_retry_attempted"] is True
    assert diagnostics["available_end_retry_reason"] == "initial_data_start_after_available_end"
    assert diagnostics["requested_quote_end"] == aware_now().isoformat()
    assert diagnostics["actual_quote_end"] == provider_available_end.isoformat()


def test_http_422_available_end_error_is_caught_and_structured(monkeypatch: pytest.MonkeyPatch) -> None:
    detail = (
        b'{"detail":{"message":"start in query (2026-05-02T12:00:00Z) was after the available_end '
        b'(2026-05-01T23:59:00.000000000Z) of dataset GLBX.MDP3"}}'
    )

    def fake_urlopen(*args: object, **kwargs: object) -> object:
        raise HTTPError(
            url="https://hist.databento.com/v0/timeseries.get_range",
            code=422,
            msg="Unprocessable Entity",
            hdrs={},
            fp=BytesIO(detail),
        )

    monkeypatch.setattr(provider_module, "urlopen", fake_urlopen)
    transport = provider_module.UrllibDatabentoQuoteTransport()

    with pytest.raises(DatabentoAvailableEndError) as exc_info:
        transport.request_records(
            base_url="https://hist.databento.com/v0",
            api_key="test-key",
            dataset="GLBX.MDP3",
            symbol="42008160",
            schema="mbp-1",
            start=aware_now(),
            end=aware_now(),
            stype_in="instrument_id",
            limit=1,
        )

    assert exc_info.value.provider_available_end == datetime(2026, 5, 1, 23, 59, tzinfo=timezone.utc)
    assert "allow-available-end-fallback" in str(exc_info.value)


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


def test_mbp1_nested_levels_record_maps_to_bid_ask_with_field_sources() -> None:
    provider = DatabentoQuoteProvider(
        config=config(),
        transport=FakeTransport(
            bbo_records=({"ts_event": "2026-05-02T12:00:00+00:00", "levels": [{"bid_px": "4626.0", "ask_px": "4626.1"}]},),
            trade_records=({"ts_event": "2026-05-02T12:00:00+00:00", "price": "4626.0"},),
        ),
        now=aware_now(),
    )

    snapshot = provider.get_quote("MGC-202606")

    assert str(snapshot.bid) == "4626.0"
    assert str(snapshot.ask) == "4626.1"
    assert snapshot.raw["parser_bid_field_source"] == "levels[0].bid_px"
    assert snapshot.raw["parser_ask_field_source"] == "levels[0].ask_px"


def test_trade_record_maps_to_last_only_and_not_bid_ask() -> None:
    record = {"ts_event": "2026-05-02T12:00:00+00:00", "price": "4626.0"}

    last_parse = provider_module._first_decimal_with_source(record, ("price", "last", "last_px"))
    bid_parse = provider_module._first_decimal_with_source(record, ("bid_px", "bid_price", "bid"))
    ask_parse = provider_module._first_decimal_with_source(record, ("ask_px", "ask_price", "ask"))

    assert str(last_parse.value) == "4626.0"
    assert last_parse.source == "price"
    assert bid_parse.value is None
    assert bid_parse.source is None
    assert ask_parse.value is None
    assert ask_parse.source is None


def test_empty_quote_records_fail_cleanly_with_parser_diagnostics() -> None:
    provider = DatabentoQuoteProvider(
        config=config(),
        transport=FakeTransport(bbo_records=(), trade_records=()),
        now=aware_now(),
    )

    with pytest.raises(DatabentoQuoteParseError) as exc_info:
        provider.get_quote("MGC-202606")

    assert "no bid/ask quote records" in str(exc_info.value)
    assert exc_info.value.diagnostics["databento_schema"] == {"bid_ask": "mbp-1", "last": "trades"}
    assert exc_info.value.diagnostics["records_returned"] == {"bid_ask": 0, "trades": 0}
    assert exc_info.value.diagnostics["no_quote_records_reason"] == "no records returned for schema mbp-1"
    assert exc_info.value.diagnostics["quote_request_symbol"] == "MGCM6"
    assert exc_info.value.diagnostics["quote_request_stype_in"] == "raw_symbol"
    assert exc_info.value.diagnostics["resolved_symbol_stype"] == "raw_symbol"
    assert exc_info.value.diagnostics["dataset"] == "GLBX.MDP3"
    assert exc_info.value.diagnostics["schema"] == {"bid_ask": "mbp-1", "last": "trades"}
    assert exc_info.value.diagnostics["encoding"] == "json"
    assert exc_info.value.diagnostics["request_details"]["bid_ask"]["schema"] == "mbp-1"
    assert exc_info.value.diagnostics["request_details"]["bid_ask"]["symbol"] == "MGCM6"


def test_zero_record_failure_preserves_resolved_instrument_id_and_request_details() -> None:
    provider = DatabentoQuoteProvider(
        config=config(databento_symbol=None, databento_continuous_symbol="MGC.v.0", allowlisted_local_symbol="MGCM6"),
        transport=FakeTransport(bbo_records=(), trade_records=()),
        resolver=FakeResolver(resolution(raw_symbol=None), resolution(requested_symbol="123456", resolved_instrument_id=None, raw_symbol="MGCM6")),
        now=aware_now(),
    )

    with pytest.raises(DatabentoQuoteParseError) as exc_info:
        provider.get_quote("MGC-202606")

    diagnostics = exc_info.value.diagnostics
    assert diagnostics["resolved_instrument_id"] == "123456"
    assert diagnostics["resolved_symbol_stype"] == "instrument_id"
    assert diagnostics["quote_request_symbol"] == "123456"
    assert diagnostics["quote_request_stype_in"] == "instrument_id"
    assert diagnostics["request_details"]["bid_ask"]["endpoint"].endswith("/timeseries.get_range")
    assert diagnostics["request_details"]["bid_ask"]["start"] == "2026-05-02T11:55:00+00:00"
    assert diagnostics["request_details"]["bid_ask"]["end"] == "2026-05-02T12:00:00+00:00"
    assert diagnostics["raw_provider_error"] is None


def test_record_diagnostic_reports_zero_records_without_traceback() -> None:
    result = run_databento_record_diagnostic(
        request=DatabentoRecordDiagnosticRequest(
            api_key="test-key",
            dataset="GLBX.MDP3",
            symbol="MGCM6",
            stype_in="raw_symbol",
            schema="mbp-1",
            start=datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc),
            end=datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc),
        ),
        transport=FakeTransport(bbo_records=(), trade_records=()),
    )

    assert result.classification == "ZERO_RECORDS"
    assert result.records_returned == 0
    assert result.request["symbol"] == "MGCM6"
    assert result.request["stype_in"] == "raw_symbol"
    assert result.request["schema"] == "mbp-1"
    assert result.failure_assessment == "NO_DATA_BAD_SYMBOL_STYPE_BAD_SCHEMA_BAD_WINDOW_OR_ENTITLEMENT"


def test_record_diagnostic_reports_first_and_last_timestamp_when_records_exist() -> None:
    result = run_databento_record_diagnostic(
        request=DatabentoRecordDiagnosticRequest(
            api_key="test-key",
            dataset="GLBX.MDP3",
            symbol="42008160",
            stype_in="instrument_id",
            schema="trades",
            start=datetime(2026, 5, 1, 13, 30, tzinfo=timezone.utc),
            end=datetime(2026, 5, 1, 20, 0, tzinfo=timezone.utc),
        ),
        transport=FakeTransport(
            bbo_records=(),
            trade_records=(
                {"ts_event": "2026-05-01T13:31:00+00:00", "price": "4620.0"},
                {"ts_event": "2026-05-01T19:59:00+00:00", "price": "4626.0"},
            ),
        ),
    )

    assert result.classification == "RECORDS_FOUND"
    assert result.records_returned == 2
    assert result.first_record_timestamp == "2026-05-01T13:31:00+00:00"
    assert result.last_record_timestamp == "2026-05-01T19:59:00+00:00"
    assert result.first_raw_record_keys_or_shape == {"root_keys": ["price", "ts_event"]}


def test_wrong_quote_schema_fails_with_clear_parser_diagnostic() -> None:
    provider = DatabentoQuoteProvider(
        config=config(bbo_schema="trades"),
        transport=FakeTransport(
            bbo_records=(),
            trade_records=({"ts_event": "2026-05-02T12:00:00+00:00", "price": "4626.0"},),
        ),
        now=aware_now(),
    )

    with pytest.raises(DatabentoQuoteParseError) as exc_info:
        provider.get_quote("MGC-202606")

    assert exc_info.value.diagnostics["databento_schema"] == {"bid_ask": "trades", "last": "trades"}
    assert exc_info.value.diagnostics["records_returned"] == {"bid_ask": 1, "trades": 1}
    assert exc_info.value.diagnostics["parser_bid_field_source"] is None
    assert exc_info.value.diagnostics["parser_ask_field_source"] is None
    assert exc_info.value.diagnostics["parser_last_field_source"] == "price"
    assert "none contained parseable bid and ask" in exc_info.value.diagnostics["no_quote_records_reason"]


def test_provider_source_has_no_track_a_live_feed_or_schwab_imports() -> None:
    source = inspect.getsource(provider_module)

    assert "live_feed" not in source
    assert "schwab" not in source.lower()
    assert "mgc_v05l.execution" not in source
    assert "mgc_v05l.app" not in source
    assert "dashboard" not in source
    assert "persistence" not in source
