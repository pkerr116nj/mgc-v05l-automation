from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

import mgc_v05l.app.schwab_token_bootstrap_web as bootstrap_module
from mgc_v05l.market_data import SchwabTokenSet, build_auth_metadata
from mgc_v05l.market_data.schwab_auth import SchwabTokenStore, SchwabTokenWriteMismatchError


def test_probe_resolution_rejects_placeholder_quote_symbol(monkeypatch, tmp_path: Path) -> None:
    monkeypatch.setenv("SCHWAB_APP_KEY", "key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://127.0.0.1:8182/callback")

    config_path = tmp_path / "schwab.local.json"
    config_path.write_text(
        """
{
  "historical_symbol_map": {"MGC": "/MGC"},
  "quote_symbol_map": {"MGC": "REPLACE_WITH_CONFIRMED_SCHWAB_QUOTE_SYMBOL"},
  "timeframe_map": {"5m": {"frequency_type": "minute", "frequency": 5}}
}
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(RuntimeError, match="Schwab config placeholder error"):
        bootstrap_module._probe_resolution(token_file=None, schwab_config_path=config_path, probe_symbol="MGC")


def _build_service(monkeypatch, tmp_path: Path) -> bootstrap_module.SchwabTokenBootstrapService:
    monkeypatch.setenv("SCHWAB_APP_KEY", "key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8182/callback")
    token_path = tmp_path / "tokens.json"
    monkeypatch.setattr(
        bootstrap_module,
        "_probe_resolution",
        lambda **_kwargs: {
            "internal_symbol": "MGC",
            "quote_symbol": "/MGC",
            "historical_symbol": "/MGC",
            "schwab_config_path": str(tmp_path / "schwab.local.json"),
            "token_file": str(token_path),
            "market_data_base_url": "https://api.schwabapi.com/marketdata/v1",
            "quotes_symbol_query_param": "symbols",
        },
    )
    return bootstrap_module.SchwabTokenBootstrapService(
        token_file=token_path,
        schwab_config_path=tmp_path / "schwab.local.json",
        probe_symbol="MGC",
    )


def _write_token(service: bootstrap_module.SchwabTokenBootstrapService, *, match_identity: bool = True) -> SchwabTokenSet:
    token_set = SchwabTokenSet(
        access_token="access",
        refresh_token="refresh",
        token_type="Bearer",
        expires_in=3600,
        scope="api",
        issued_at=datetime.now(timezone.utc),
    )
    metadata = build_auth_metadata(service._auth_config)
    if not match_identity:
        metadata = {**metadata, "callback_url": "https://127.0.0.1:9999/callback"}
    SchwabTokenStore(service.token_file_path).save(token_set, auth_metadata=metadata)
    return token_set


def _write_token_without_refresh(
    service: bootstrap_module.SchwabTokenBootstrapService,
    *,
    match_identity: bool = True,
) -> SchwabTokenSet:
    token_set = SchwabTokenSet(
        access_token="access",
        refresh_token=None,
        token_type="Bearer",
        expires_in=3600,
        scope="api",
        issued_at=datetime.now(timezone.utc),
    )
    metadata = build_auth_metadata(service._auth_config)
    if not match_identity:
        metadata = {**metadata, "callback_url": "https://127.0.0.1:9999/callback"}
    SchwabTokenStore(service.token_file_path).save(token_set, auth_metadata=metadata)
    return token_set


def _fake_loopback_success(service: bootstrap_module.SchwabTokenBootstrapService, *, match_identity: bool = True):
    def _runner(*, event_callback, **_kwargs):
        event_callback({"stage": "callback_received", "callback_received": True})
        event_callback({"stage": "auth_code_parsed", "callback_received": True, "auth_code_parsed": True})
        event_callback({"stage": "exchange_started", "exchange_attempted": True, "token_write_attempted": True})
        _write_token(service, match_identity=match_identity)
        event_callback(
            {
                "stage": "exchange_succeeded",
                "exchange_attempted": True,
                "exchange_success": True,
                "token_write_attempted": True,
                "token_write_success": True,
            }
        )
        return SimpleNamespace(
            authorize_url="https://example.test/authorize",
            callback_url=service._auth_config.callback_url,
            browser_opened=True,
            token_file=str(service.token_file_path),
        )

    return _runner


def test_local_authorize_fails_fast_on_callback_bind_conflict(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    monkeypatch.setattr(
        service,
        "_preflight_callback_listener",
        lambda: (_ for _ in ()).throw(
            RuntimeError(
                "Callback listener could not bind to 127.0.0.1:8182. Another process is using the port. Stop the existing bootstrap/dashboard listener or auto-select a new port and regenerate the callback URL."
            )
        ),
    )

    payload = service.run_local_authorize(state="mgc-v05l-local", scope=None, timeout_seconds=180)

    assert payload["final_state"] == "EXCHANGE_FAILED"
    assert payload["failed_stage"] == "callback_listener_bind"
    assert "Another process is using the port" in payload["error"]
    assert payload["auth_url_generated"] is False


def test_local_authorize_reports_exchange_failure_after_callback(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    monkeypatch.setattr(service, "_preflight_callback_listener", lambda: None)

    def _runner(*, event_callback, **_kwargs):
        event_callback({"stage": "callback_received", "callback_received": True})
        event_callback({"stage": "auth_code_parsed", "callback_received": True, "auth_code_parsed": True})
        event_callback({"stage": "exchange_started", "exchange_attempted": True, "token_write_attempted": True})
        event_callback(
            {
                "stage": "exchange_failed",
                "exchange_attempted": True,
                "exchange_success": False,
                "token_write_attempted": True,
                "token_write_success": False,
                "error": "bad exchange",
            }
        )
        raise bootstrap_module.SchwabAuthError("bad exchange")

    monkeypatch.setattr(bootstrap_module, "run_loopback_authorization", _runner)

    payload = service.run_local_authorize(state="mgc-v05l-local", scope=None, timeout_seconds=180)

    assert payload["final_state"] == "EXCHANGE_FAILED"
    assert payload["failed_stage"] == "exchange"
    assert payload["callback_received"] is True
    assert payload["code_parsed"] is True
    assert payload["exchange_attempted"] is True
    assert payload["exchange_succeeded"] is False


def test_local_authorize_reports_token_write_mismatch(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    monkeypatch.setattr(service, "_preflight_callback_listener", lambda: None)
    exchange_path = service.token_file_path.parent / "bootstrap_artifacts/latest_exchange_result.json"
    exchange_path.parent.mkdir(parents=True, exist_ok=True)
    exchange_path.write_text(
        '{"persisted_refresh_token_matches_exchange": false}',
        encoding="utf-8",
    )
    monkeypatch.setattr(
        bootstrap_module,
        "run_loopback_authorization",
        lambda **_kwargs: (_ for _ in ()).throw(
            SchwabTokenWriteMismatchError("Persisted refresh token does not match the exchange response refresh token.")
        ),
    )

    payload = service.run_local_authorize(state="mgc-v05l-local", scope=None, timeout_seconds=180)

    assert payload["final_state"] == "TOKEN_WRITE_MISMATCH"
    assert payload["failed_stage"] == "token_write_mismatch"
    assert payload["exchange_diagnostic"]["persisted_refresh_token_matches_exchange"] is False


def test_local_authorize_reports_refresh_failure_after_exchange(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    monkeypatch.setattr(service, "_preflight_callback_listener", lambda: None)
    monkeypatch.setattr(bootstrap_module, "run_loopback_authorization", _fake_loopback_success(service))

    token_set = _write_token(service)
    diagnostic_path = service.token_file_path.parent / "bootstrap_artifacts/latest_refresh_failure.json"
    diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostic_path.write_text(
        '{"provider_status_code": 400, "provider_response_body": "{\\"error\\":\\"unsupported_token_type\\"}"}',
        encoding="utf-8",
    )
    monkeypatch.setattr(service, "_evaluate_refresh", lambda **_kwargs: (False, "refresh exploded", token_set))

    payload = service.run_local_authorize(state="mgc-v05l-local", scope=None, timeout_seconds=180)

    assert payload["final_state"] == "REFRESH_FAILED_IMMEDIATELY_AFTER_EXCHANGE"
    assert payload["failed_stage"] == "refresh"
    assert payload["exchange_succeeded"] is True
    assert payload["token_written"] is True
    assert payload["refresh_attempted"] is True
    assert payload["refresh_succeeds"] is False
    assert payload["refresh_error"] == "refresh exploded"
    assert payload["refresh_failure_diagnostic"]["provider_status_code"] == 400
    assert payload["exchange_diagnostic_path"].endswith("latest_exchange_result.json")
    assert payload["persisted_token_artifact_path"].endswith("latest_persisted_token_payload.json")
    assert payload["refresh_result_artifact_path"].endswith("latest_refresh_result.json")


def test_runtime_ready_check_reports_probe_failure_after_refresh(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    token_set = _write_token(service)
    monkeypatch.setattr(service, "_evaluate_refresh", lambda **_kwargs: (True, None, token_set))
    monkeypatch.setattr(service, "_evaluate_market_data_probe", lambda: (False, "probe exploded", None))

    payload = service.check_runtime_ready()

    assert payload["final_state"] == "PROBE_FAILED"
    assert payload["failed_stage"] == "market_data_probe"
    assert payload["refresh_succeeds"] is True
    assert payload["market_data_probe_attempted"] is True
    assert payload["market_data_probe_succeeds"] is False
    assert payload["market_data_probe_error"] == "probe exploded"


def test_local_authorize_fails_hard_on_token_identity_mismatch(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    monkeypatch.setattr(service, "_preflight_callback_listener", lambda: None)
    monkeypatch.setattr(bootstrap_module, "run_loopback_authorization", _fake_loopback_success(service, match_identity=False))

    payload = service.run_local_authorize(state="mgc-v05l-local", scope=None, timeout_seconds=180)

    assert payload["final_state"] == "TOKEN_IDENTITY_MISMATCH"
    assert payload["failed_stage"] == "token_identity_mismatch"
    assert payload["token_written"] is True
    assert payload["token_client_match"] is False
    assert payload["error"] == "New token was written but does not match current Schwab app identity."


def test_local_authorize_reports_callback_timeout(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    monkeypatch.setattr(service, "_preflight_callback_listener", lambda: None)
    monkeypatch.setattr(
        bootstrap_module,
        "run_loopback_authorization",
        lambda **_kwargs: (_ for _ in ()).throw(
            bootstrap_module.SchwabAuthError(
                "Timed out waiting for Schwab callback at https://127.0.0.1:8182/callback after 180 seconds."
            )
        ),
    )

    payload = service.run_local_authorize(state="mgc-v05l-local", scope=None, timeout_seconds=180)

    assert payload["final_state"] == "EXCHANGE_FAILED"
    assert payload["failed_stage"] == "callback_timeout"
    assert "Timed out waiting for Schwab callback" in payload["error"]


def test_local_authorize_recovers_to_runtime_ready(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    monkeypatch.setattr(service, "_preflight_callback_listener", lambda: None)
    monkeypatch.setattr(bootstrap_module, "run_loopback_authorization", _fake_loopback_success(service))

    token_set = _write_token(service)
    monkeypatch.setattr(service, "_evaluate_refresh", lambda **_kwargs: (True, None, token_set))
    payload = service.run_local_authorize(state="mgc-v05l-local", scope=None, timeout_seconds=180)

    assert payload["final_state"] == "RUNTIME_READY"
    assert payload["runtime_ready"] is True
    assert payload["refresh_succeeds"] is True
    assert payload["market_data_probe_attempted"] is False
    assert payload["step_state"]["runtime_ready"] is True
    assert payload["persisted_token_artifact_path"].endswith("latest_persisted_token_payload.json")


def test_manual_exchange_refresh_failure_reports_immediate_refresh_verdict(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    diagnostic_path = service.token_file_path.parent / "bootstrap_artifacts/latest_refresh_failure.json"
    diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
    diagnostic_path.write_text(
        '{"provider_status_code": 400, "provider_response_body": "{\\"error\\":\\"unsupported_token_type\\"}"}',
        encoding="utf-8",
    )
    exchange_path = service.token_file_path.parent / "bootstrap_artifacts/latest_exchange_result.json"
    exchange_path.write_text(
        '{"persisted_refresh_token_matches_exchange": true, "exchange_response_summary": {"has_refresh_token": true}}',
        encoding="utf-8",
    )
    fake_oauth_client = SimpleNamespace(exchange_code=lambda _code: _write_token(service))
    monkeypatch.setattr(service, "_oauth_client", lambda: fake_oauth_client)
    token_set = _write_token(service)
    monkeypatch.setattr(service, "_evaluate_refresh", lambda **_kwargs: (False, "refresh exploded", token_set))

    payload = service.exchange_code("abc123")

    assert payload["final_state"] == "REFRESH_FAILED_IMMEDIATELY_AFTER_EXCHANGE"
    assert payload["failed_stage"] == "refresh"
    assert payload["exchange_diagnostic"]["persisted_refresh_token_matches_exchange"] is True


def test_manual_exchange_reports_token_write_mismatch(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    exchange_path = service.token_file_path.parent / "bootstrap_artifacts/latest_exchange_result.json"
    exchange_path.parent.mkdir(parents=True, exist_ok=True)
    exchange_path.write_text(
        '{"persisted_refresh_token_matches_exchange": false}',
        encoding="utf-8",
    )
    fake_oauth_client = SimpleNamespace(
        exchange_code=lambda _code: (_ for _ in ()).throw(
            SchwabTokenWriteMismatchError("Persisted refresh token does not match the exchange response refresh token.")
        )
    )
    monkeypatch.setattr(service, "_oauth_client", lambda: fake_oauth_client)

    payload = service.exchange_code("abc123")

    assert payload["final_state"] == "TOKEN_WRITE_MISMATCH"
    assert payload["failed_stage"] == "token_write_mismatch"


def test_local_authorize_fails_when_exchange_writes_no_refresh_token(monkeypatch, tmp_path: Path) -> None:
    service = _build_service(monkeypatch, tmp_path)
    monkeypatch.setattr(service, "_preflight_callback_listener", lambda: None)

    def _runner(*, event_callback, **_kwargs):
        event_callback({"stage": "callback_received", "callback_received": True})
        event_callback({"stage": "auth_code_parsed", "callback_received": True, "auth_code_parsed": True})
        event_callback({"stage": "exchange_started", "exchange_attempted": True, "token_write_attempted": True})
        _write_token_without_refresh(service)
        event_callback(
            {
                "stage": "exchange_succeeded",
                "exchange_attempted": True,
                "exchange_success": True,
                "token_write_attempted": True,
                "token_write_success": True,
            }
        )
        return SimpleNamespace(
            authorize_url="https://example.test/authorize",
            callback_url=service._auth_config.callback_url,
            browser_opened=True,
            token_file=str(service.token_file_path),
        )

    monkeypatch.setattr(bootstrap_module, "run_loopback_authorization", _runner)

    payload = service.run_local_authorize(state="mgc-v05l-local", scope=None, timeout_seconds=180)

    assert payload["final_state"] == "TOKEN_WRITTEN_BUT_INVALID"
    assert payload["failed_stage"] == "token_written_but_invalid"
    assert payload["token_written"] is True
    assert payload["has_refresh_token"] is False
    assert payload["error"] == "Authorization succeeded but Schwab did not issue a usable refresh token for this app/session."
