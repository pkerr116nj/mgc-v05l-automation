"""Tests for Schwab auth, history normalization, quotes, and symbol mapping."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest
from sqlalchemy import select

from mgc_v05l.config_models import load_settings_from_files
from mgc_v05l.market_data.historical_service import HistoricalBackfillService
from mgc_v05l.market_data.quote_service import QuoteService
from mgc_v05l.market_data.schwab_adapter import SchwabMarketDataAdapter
from mgc_v05l.market_data.schwab_auth import (
    SchwabOAuthClient,
    SchwabTokenStore,
    SchwabTokenWriteMismatchError,
    load_schwab_auth_config_from_env,
)
from mgc_v05l.market_data.schwab_config import load_schwab_market_data_config
from mgc_v05l.market_data.schwab_http import SchwabHistoricalHttpClient, SchwabHttpError, SchwabQuoteHttpClient
from mgc_v05l.market_data.schwab_models import (
    HttpRequest,
    JsonHttpTransport,
    SchwabAuthConfig,
    SchwabBarFieldMap,
    SchwabHistoricalRequest,
    SchwabMarketDataConfig,
    SchwabPriceHistoryFrequency,
    SchwabQuoteRequest,
    SchwabTokenSet,
    TimestampSemantics,
)
from mgc_v05l.persistence.db import build_engine
from mgc_v05l.persistence.repositories import RepositorySet
from mgc_v05l.persistence.tables import bars_table


class _FakeJsonTransport(JsonHttpTransport):
    def __init__(self, responses: list[dict]) -> None:
        self._responses = list(responses)
        self.requests: list[HttpRequest] = []

    def request_json(self, request: HttpRequest) -> dict:
        self.requests.append(request)
        if not self._responses:
            raise AssertionError("Unexpected extra HTTP request.")
        return self._responses.pop(0)


class _FailingJsonTransport(JsonHttpTransport):
    def __init__(self, error: Exception) -> None:
        self._error = error
        self.requests: list[HttpRequest] = []

    def request_json(self, request: HttpRequest) -> dict:
        self.requests.append(request)
        raise self._error


def _fixture_payload(name: str) -> dict:
    return json.loads((Path("tests/fixtures") / name).read_text(encoding="utf-8"))


def _build_settings(tmp_path: Path):
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text(
        'mode: "replay"\n'
        f'database_url: "sqlite:///{tmp_path / "schwab.sqlite3"}"\n',
        encoding="utf-8",
    )
    return load_settings_from_files([Path("config/base.yaml"), overlay_path])


def _build_config(token_path: Path) -> SchwabMarketDataConfig:
    return SchwabMarketDataConfig(
        auth=SchwabAuthConfig(
            app_key="app-key",
            app_secret="app-secret",
            callback_url="http://127.0.0.1:8182/callback",
            token_store_path=token_path,
        ),
        historical_symbol_map={"MGC": "MGC_EXTERNAL"},
        quote_symbol_map={"MGC": "MGC_QUOTE_EXTERNAL"},
        timeframe_map={"5m": SchwabPriceHistoryFrequency(frequency_type="minute", frequency=5)},
        field_map=SchwabBarFieldMap(
            timestamp_field="datetime",
            open_field="open",
            high_field="high",
            low_field="low",
            close_field="close",
            volume_field="volume",
            is_final_field=None,
            timestamp_semantics=TimestampSemantics.END,
        ),
        quotes_symbol_query_param="symbols",
    )


def test_auth_url_construction_and_token_store_round_trip(tmp_path: Path) -> None:
    token_path = tmp_path / "tokens.json"
    transport = _FakeJsonTransport([_fixture_payload("schwab_token_response.json")])
    auth_config = SchwabAuthConfig(
        app_key="app-key",
        app_secret="app-secret",
        callback_url="http://127.0.0.1:8182/callback",
        token_store_path=token_path,
    )
    client = SchwabOAuthClient(
        config=auth_config,
        transport=transport,
        token_store=SchwabTokenStore(token_path),
    )

    auth_url = client.build_authorize_url("state-123")
    token_set = client.exchange_code("code-abc")

    assert "client_id=app-key" in auth_url
    assert "response_type=code" in auth_url
    assert "state=state-123" in auth_url
    assert token_set.access_token == "access-token-123"
    assert token_path.exists()
    assert client.token_store.load() == token_set
    assert transport.requests[0].form == {
        "grant_type": "authorization_code",
        "code": "code-abc",
        "redirect_uri": "http://127.0.0.1:8182/callback",
    }


def test_exchange_code_decodes_percent_encoded_callback_code(tmp_path: Path) -> None:
    token_path = tmp_path / "tokens.json"
    transport = _FakeJsonTransport([_fixture_payload("schwab_token_response.json")])
    auth_config = SchwabAuthConfig(
        app_key="app-key",
        app_secret="app-secret",
        callback_url="http://127.0.0.1:8182/callback",
        token_store_path=token_path,
    )
    client = SchwabOAuthClient(
        config=auth_config,
        transport=transport,
        token_store=SchwabTokenStore(token_path),
    )

    client.exchange_code("abc%2B123%3D%3D")

    assert transport.requests[0].url == "https://api.schwabapi.com/v1/oauth/token"
    assert transport.requests[0].headers["Content-Type"] == "application/x-www-form-urlencoded"
    assert transport.requests[0].headers["Accept"] == "application/json"
    assert transport.requests[0].headers["Authorization"].startswith("Basic ")
    assert transport.requests[0].form == {
        "grant_type": "authorization_code",
        "code": "abc+123==",
        "redirect_uri": "http://127.0.0.1:8182/callback",
    }


def test_refresh_token_uses_basic_auth_without_client_id_and_writes_failure_diagnostic(tmp_path: Path) -> None:
    token_path = tmp_path / "tokens.json"
    auth_config = SchwabAuthConfig(
        app_key="app-key",
        app_secret="app-secret",
        callback_url="http://127.0.0.1:8182/callback",
        token_store_path=token_path,
    )
    error = SchwabHttpError(
        "provider error: unsupported_token_type",
        url="https://api.schwabapi.com/v1/oauth/token",
        status_code=400,
        headers={"Content-Type": "application/json"},
        response_body='{"error":"unsupported_token_type"}',
    )
    transport = _FailingJsonTransport(error)
    client = SchwabOAuthClient(
        config=auth_config,
        transport=transport,
        token_store=SchwabTokenStore(token_path),
    )
    client.token_store.save(
        SchwabTokenSet(
            access_token="stored-access-token",
            refresh_token="refresh-token-456",
            token_type="Bearer",
            expires_in=1800,
            scope="api",
            issued_at=datetime.now(timezone.utc),
        )
    )

    with pytest.raises(RuntimeError, match="unsupported_token_type"):
        client.refresh_token()

    assert transport.requests[0].headers["Authorization"].startswith("Basic ")
    assert transport.requests[0].form == {
        "grant_type": "refresh_token",
        "refresh_token": "refresh-token-456",
    }
    diagnostic = json.loads((tmp_path / "bootstrap_artifacts/latest_refresh_failure.json").read_text(encoding="utf-8"))
    assert diagnostic["token_endpoint_url"] == "https://api.schwabapi.com/v1/oauth/token"
    assert diagnostic["callback_url"] == "http://127.0.0.1:8182/callback"
    assert diagnostic["stored_token_fields"]["has_refresh_token"] is True
    assert diagnostic["refresh_token_length"] == len("refresh-token-456")
    assert diagnostic["refresh_token_head"] == "refr"
    assert diagnostic["refresh_token_tail"] == "-456"
    assert diagnostic["error_text"] == "provider error: unsupported_token_type"
    assert diagnostic["provider_status_code"] == 400
    assert diagnostic["provider_response_body"] == '{"error":"unsupported_token_type"}'


def test_exchange_writes_sanitized_exchange_artifact_and_persists_refresh_token_exactly(tmp_path: Path) -> None:
    token_path = tmp_path / "tokens.json"
    transport = _FakeJsonTransport(
        [
            {
                "access_token": "access-token-123",
                "refresh_token": "refresh-token-456",
                "id_token": "id-token-789",
                "token_type": "Bearer",
                "scope": "api",
                "expires_in": 1800,
            }
        ]
    )
    auth_config = SchwabAuthConfig(
        app_key="app-key",
        app_secret="app-secret",
        callback_url="http://127.0.0.1:8182/callback",
        token_store_path=token_path,
    )
    client = SchwabOAuthClient(
        config=auth_config,
        transport=transport,
        token_store=SchwabTokenStore(token_path),
    )

    client.exchange_code("code-abc")

    persisted = client.token_store.load_payload()
    exchange_artifact = json.loads((tmp_path / "bootstrap_artifacts/latest_exchange_result.json").read_text(encoding="utf-8"))

    assert persisted["refresh_token"] == "refresh-token-456"
    assert client.token_store.load().refresh_token == "refresh-token-456"
    assert exchange_artifact["exchange_response_summary"]["has_access_token"] is True
    assert exchange_artifact["exchange_response_summary"]["has_refresh_token"] is True
    assert exchange_artifact["exchange_response_summary"]["has_id_token"] is True
    assert exchange_artifact["exchange_response_summary"]["refresh_token"]["length"] == len("refresh-token-456")
    assert exchange_artifact["persisted_refresh_token_matches_exchange"] is True
    assert exchange_artifact["persisted_refresh_matches_exchange_access_token"] is False
    assert exchange_artifact["persisted_refresh_matches_exchange_id_token"] is False


def test_token_store_round_trip_preserves_refresh_token_exact_bytes(tmp_path: Path) -> None:
    token_path = tmp_path / "tokens.json"
    original_refresh = "AbC1+/=zZ9 tail"
    store = SchwabTokenStore(token_path)
    store.save(
        SchwabTokenSet(
            access_token="access",
            refresh_token=original_refresh,
            token_type="Bearer",
            expires_in=1800,
            scope="api",
            issued_at=datetime.now(timezone.utc),
        )
    )

    reloaded = store.load()

    assert reloaded is not None
    assert reloaded.refresh_token == original_refresh


def test_exchange_raises_when_persisted_refresh_token_differs_from_exchange_payload(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_path = tmp_path / "tokens.json"
    transport = _FakeJsonTransport(
        [
            {
                "access_token": "access-token-123",
                "refresh_token": "refresh-token-456",
                "token_type": "Bearer",
                "scope": "api",
                "expires_in": 1800,
            }
        ]
    )
    auth_config = SchwabAuthConfig(
        app_key="app-key",
        app_secret="app-secret",
        callback_url="http://127.0.0.1:8182/callback",
        token_store_path=token_path,
    )
    client = SchwabOAuthClient(
        config=auth_config,
        transport=transport,
        token_store=SchwabTokenStore(token_path),
    )
    original_save = client.token_store.save

    def _mutating_save(token_set, auth_metadata=None):
        original_save(token_set, auth_metadata=auth_metadata)
        payload = client.token_store.load_payload()
        payload["refresh_token"] = "wrong-refresh-token"
        token_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")

    monkeypatch.setattr(client.token_store, "save", _mutating_save)

    with pytest.raises(SchwabTokenWriteMismatchError, match="Persisted refresh token does not match"):
        client.exchange_code("code-abc")

    exchange_artifact = json.loads((tmp_path / "bootstrap_artifacts/latest_exchange_result.json").read_text(encoding="utf-8"))
    assert exchange_artifact["persisted_refresh_token_matches_exchange"] is False


def test_load_schwab_auth_config_from_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_path = tmp_path / "env-token.json"
    monkeypatch.setenv("SCHWAB_APP_KEY", "env-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "env-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))

    config = load_schwab_auth_config_from_env()

    assert config.app_key == "env-key"
    assert config.app_secret == "env-secret"
    assert config.callback_url == "http://localhost/callback"
    assert config.token_store_path == token_path


def test_load_schwab_market_data_config_honors_market_data_base_url(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_path = tmp_path / "tokens.json"
    config_path = tmp_path / "schwab.local.json"
    config_path.write_text(
        json.dumps(
            {
                "historical_symbol_map": {"MGC": "/MGC"},
                "quote_symbol_map": {"MGC": "/MGC"},
                "timeframe_map": {"5m": {"frequency_type": "minute", "frequency": 5}},
                "market_data_base_url": "https://example.invalid/marketdata/v99",
                "quotes_symbol_query_param": "symbols",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SCHWAB_APP_KEY", "env-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "env-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://localhost/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))

    config = load_schwab_market_data_config(config_path)

    assert config.market_data_base_url == "https://example.invalid/marketdata/v99"


def test_token_response_parsing_refresh_and_expiry() -> None:
    issued_at = datetime(2026, 3, 14, 14, 0, tzinfo=timezone.utc)
    token_set = SchwabTokenSet.from_token_response(
        _fixture_payload("schwab_token_response.json"),
        issued_at=issued_at,
    )

    assert token_set.refresh_token == "refresh-token-456"
    assert token_set.token_type == "Bearer"
    assert token_set.is_expired(datetime(2026, 3, 14, 14, 29, 45, tzinfo=timezone.utc)) is True
    assert token_set.is_expired(datetime(2026, 3, 14, 14, 15, tzinfo=timezone.utc)) is False


def test_pricehistory_response_normalization_and_persistence(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    repositories = RepositorySet(build_engine(settings.database_url))
    config = _build_config(tmp_path / "tokens.json")
    adapter = SchwabMarketDataAdapter(settings, config)
    transport = _FakeJsonTransport([_fixture_payload("schwab_pricehistory_response.json")])
    oauth_client = SchwabOAuthClient(
        config=config.auth,
        transport=transport,
        token_store=SchwabTokenStore(config.auth.token_store_path),
    )
    oauth_client.token_store.save(
        SchwabTokenSet(
            access_token="stored-access-token",
            refresh_token="stored-refresh-token",
            token_type="Bearer",
            expires_in=1800,
            scope="marketdata",
            issued_at=datetime.now(timezone.utc),
        )
    )
    service = HistoricalBackfillService(
        adapter=adapter,
        client=SchwabHistoricalHttpClient(
            oauth_client=oauth_client,
            market_data_config=config,
            transport=transport,
        ),
        repositories=repositories,
    )

    bars = service.fetch_bars(
        SchwabHistoricalRequest(
            internal_symbol="MGC",
            period_type="day",
            period=1,
            frequency_type="minute",
            frequency=5,
            start_date_ms=1741903200000,
            end_date_ms=1741903800000,
            need_extended_hours_data=False,
            need_previous_close=True,
        ),
        internal_timeframe="5m",
    )

    with repositories.engine.begin() as connection:
        persisted = connection.execute(select(bars_table.c.bar_id)).all()

    assert len(bars) == 2
    assert bars[0].bar_id.endswith("2025-03-13T22:05:00Z")
    assert bars[0].session_asia is True
    assert bars[0].session_us is False
    assert len(persisted) == 2
    assert transport.requests[0].query == {
        "symbol": "MGC_EXTERNAL",
        "periodType": "day",
        "needExtendedHoursData": False,
        "needPreviousClose": True,
        "period": 1,
        "frequencyType": "minute",
        "frequency": 5,
        "startDate": 1741903200000,
        "endDate": 1741903800000,
    }


def test_quote_response_normalization(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    config = _build_config(tmp_path / "tokens.json")
    adapter = SchwabMarketDataAdapter(settings, config)
    transport = _FakeJsonTransport([_fixture_payload("schwab_quotes_response.json")])
    oauth_client = SchwabOAuthClient(
        config=config.auth,
        transport=transport,
        token_store=SchwabTokenStore(config.auth.token_store_path),
    )
    oauth_client.token_store.save(
        SchwabTokenSet(
            access_token="stored-access-token",
            refresh_token="stored-refresh-token",
            token_type="Bearer",
            expires_in=1800,
            scope="marketdata",
            issued_at=datetime.now(timezone.utc),
        )
    )
    service = QuoteService(
        adapter=adapter,
        client=SchwabQuoteHttpClient(
            oauth_client=oauth_client,
            market_data_config=config,
            transport=transport,
        ),
    )

    quotes = service.fetch_quotes(SchwabQuoteRequest(internal_symbols=("MGC",)))

    assert len(quotes) == 1
    assert quotes[0].internal_symbol == "MGC"
    assert quotes[0].external_symbol == "MGC_QUOTE_EXTERNAL"
    assert quotes[0].quote_future == {"lastPrice": 3125.4, "netChange": 4.2}
    assert quotes[0].reference_future == {
        "symbol": "MGC_QUOTE_EXTERNAL",
        "description": "Micro Gold Futures",
    }
    assert transport.requests[0].query == {"symbols": "MGC_QUOTE_EXTERNAL"}


def test_symbol_mapping_behavior(tmp_path: Path) -> None:
    settings = _build_settings(tmp_path)
    adapter = SchwabMarketDataAdapter(settings, _build_config(tmp_path / "tokens.json"))

    assert adapter.map_historical_symbol("MGC") == "MGC_EXTERNAL"
    assert adapter.map_quote_symbol("MGC") == "MGC_QUOTE_EXTERNAL"
    assert adapter.map_timeframe("5m") == SchwabPriceHistoryFrequency(
        frequency_type="minute",
        frequency=5,
    )

    with pytest.raises(ValueError):
        adapter.map_quote_symbol("GC")
