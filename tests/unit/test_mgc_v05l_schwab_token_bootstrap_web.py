"""Tests for the local Schwab token bootstrap web tool service."""

from __future__ import annotations

import errno
import json
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mgc_v05l.app import schwab_token_bootstrap_web
from mgc_v05l.app.schwab_token_bootstrap_web import SchwabTokenBootstrapService
from mgc_v05l.market_data.schwab_local_auth import SchwabLoopbackAuthResult
from mgc_v05l.market_data.schwab_models import HttpRequest


class _FakeJsonTransport:
    def __init__(self, responses: list[dict]) -> None:
        self._responses = list(responses)
        self.requests: list[HttpRequest] = []

    def request_json(self, request: HttpRequest) -> dict:
        self.requests.append(request)
        if not self._responses:
            raise AssertionError("Unexpected extra request.")
        return self._responses.pop(0)


def _fixture_payload(name: str) -> dict:
    return json.loads((Path("tests/fixtures") / name).read_text(encoding="utf-8"))


def test_status_reports_missing_token(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_path = tmp_path / "tokens.json"
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://127.0.0.1:8182/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))

    service = SchwabTokenBootstrapService(
        transport_factory=lambda: _FakeJsonTransport([]),
        market_data_probe=lambda: {"probe_symbol": "MGC", "quote_count": 1, "external_symbol": "/MGC"},
    )

    status = service.status()

    assert status["callback_url"] == "http://127.0.0.1:8182/callback"
    assert status["token_file"] == str(token_path.resolve())
    assert status["token_exists"] is False
    assert status["has_refresh_token"] is False
    assert status["refresh_succeeds"] is False
    assert status["runtime_ready"] is False
    assert status["debug"]["resolved_token_path"] == str(token_path.resolve())
    assert status["current_client_identity"]["callback_url"] == "http://127.0.0.1:8182/callback"
    assert status["stored_token_identity"] is None
    assert status["token_client_match"] is None
    assert status["commands"]["cli_refresh"].endswith(f'--token-file "{token_path.resolve()}"')
    assert status["commands"]["launch_web"].endswith(f'--token-file "{token_path.resolve()}"')


def test_exchange_refresh_and_runtime_ready_flow(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_path = tmp_path / "tokens.json"
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://127.0.0.1:8182/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))

    exchange_transport = _FakeJsonTransport([_fixture_payload("schwab_token_response.json")])
    exchange_refresh_check_transport = _FakeJsonTransport([_fixture_payload("schwab_token_response.json")])
    ready_transport = _FakeJsonTransport([_fixture_payload("schwab_token_response.json")])
    refresh_transport = _FakeJsonTransport([_fixture_payload("schwab_token_response.json")])
    transports = [exchange_transport, exchange_refresh_check_transport, ready_transport, refresh_transport]
    service = SchwabTokenBootstrapService(
        transport_factory=lambda: transports.pop(0),
        market_data_probe=lambda: {"probe_symbol": "MGC", "quote_count": 1, "external_symbol": "/MGC"},
    )

    exchanged = service.exchange_code("manual-code")
    assert exchanged["token_exists"] is True
    assert exchanged["has_refresh_token"] is True
    assert exchanged["refresh_succeeds"] is True
    assert exchanged["market_data_probe_succeeds"] is True
    assert exchanged["runtime_ready"] is True
    assert exchanged["token_client_match"] is True
    assert exchanged["last_operation"]["exchange_attempted"] is True
    assert exchanged["last_operation"]["exchange_success"] is True
    assert exchanged["last_operation"]["token_write_attempted"] is True
    assert exchanged["last_operation"]["token_write_success"] is True
    assert exchange_transport.requests[0].form["code"] == "manual-code"

    ready = service.check_runtime_ready()
    assert ready["runtime_ready"] is True
    assert ready["refresh_succeeds"] is True
    assert ready["market_data_probe_succeeds"] is True

    refreshed = service.refresh_token()
    assert refreshed["token_exists"] is True
    assert refreshed["refresh_succeeds"] is True
    assert refresh_transport.requests[0].form["grant_type"] == "refresh_token"


def test_status_and_runtime_ready_report_refresh_failure(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_path = tmp_path / "tokens.json"
    token_path.write_text(
        json.dumps(
            {
                "access_token": "stale-token",
                "refresh_token": "bad-refresh-token",
                "token_type": "Bearer",
                "expires_in": 1800,
                "scope": "marketdata",
                "issued_at": datetime.now(timezone.utc).isoformat(),
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://127.0.0.1:8182/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))

    def _raising_transport_factory():
        class _RaisingTransport:
            def request_json(self, request: HttpRequest) -> dict:
                raise RuntimeError("401 Unauthorized\nerror: invalid_client")

        return _RaisingTransport()

    service = SchwabTokenBootstrapService(
        transport_factory=_raising_transport_factory,
        market_data_probe=lambda: {"probe_symbol": "MGC", "quote_count": 1, "external_symbol": "/MGC"},
    )

    status = service.status()
    ready = service.check_runtime_ready()

    assert status["token_exists"] is True
    assert status["refresh_succeeds"] is False
    assert status["runtime_ready"] is False
    assert status["market_data_probe_succeeds"] is False
    assert "invalid_client" in status["refresh_error"]
    assert status["stored_token_identity"] is None
    assert ready["runtime_ready"] is False
    assert "invalid_client" in ready["message"]


def test_runtime_ready_stays_false_when_refresh_succeeds_but_market_probe_fails(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_path = tmp_path / "tokens.json"
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://127.0.0.1:8182/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))

    transports = [
        _FakeJsonTransport([_fixture_payload("schwab_token_response.json")]),
        _FakeJsonTransport([_fixture_payload("schwab_token_response.json")]),
        _FakeJsonTransport([_fixture_payload("schwab_token_response.json")]),
    ]
    service = SchwabTokenBootstrapService(
        transport_factory=lambda: transports.pop(0),
        market_data_probe=lambda: (_ for _ in ()).throw(RuntimeError("quotes probe failed")),
    )
    service.exchange_code("manual-code")

    status = service.status()

    assert status["refresh_succeeds"] is True
    assert status["market_data_probe_succeeds"] is False
    assert status["runtime_ready"] is False
    assert "quotes probe failed" in str(status["market_data_probe_error"])


def test_local_authorize_bind_failure_is_tracked_in_last_operation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_path = tmp_path / "tokens.json"
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://127.0.0.1:8182/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))

    def _fake_run_loopback_authorization(*args, **kwargs):
        event_callback = kwargs["event_callback"]
        event_callback(
            {
                "stage": "callback_listener_bind_failed",
                "callback_received": False,
                "auth_code_parsed": False,
                "exchange_attempted": False,
                "exchange_success": False,
                "token_write_attempted": False,
                "token_write_path": str(token_path.resolve()),
                "token_write_success": False,
                "error": "Could not bind callback listener on http://127.0.0.1:8182/callback: [Errno 48] Address already in use",
            }
        )
        raise RuntimeError("Could not bind callback listener on http://127.0.0.1:8182/callback: [Errno 48] Address already in use")

    monkeypatch.setattr(schwab_token_bootstrap_web, "run_loopback_authorization", _fake_run_loopback_authorization)

    service = SchwabTokenBootstrapService(
        transport_factory=lambda: _FakeJsonTransport([]),
        market_data_probe=lambda: {"probe_symbol": "MGC", "quote_count": 1, "external_symbol": "/MGC"},
    )

    with pytest.raises(RuntimeError, match="Could not bind callback listener"):
        service.run_local_authorize(state="state-123", scope=None, timeout_seconds=120)

    assert service.last_operation_status()["callback_received"] is False
    assert service.last_operation_status()["exchange_attempted"] is False
    assert service.last_operation_status()["token_write_success"] is False
    assert "Address already in use" in str(service.last_operation_status()["error"])


def test_html_page_shows_copyable_cli_commands() -> None:
    assert 'id="resolvedTokenPathBox"' in schwab_token_bootstrap_web._HTML_PAGE
    assert 'id="cliRefreshBox"' in schwab_token_bootstrap_web._HTML_PAGE
    assert 'id="authGateBox"' in schwab_token_bootstrap_web._HTML_PAGE
    assert 'id="launchWebBox"' in schwab_token_bootstrap_web._HTML_PAGE
    assert "function copyFrom(id)" in schwab_token_bootstrap_web._HTML_PAGE


def test_run_server_falls_forward_to_next_free_port_and_writes_info_file(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    token_path = tmp_path / "tokens.json"
    info_path = tmp_path / "launch.json"
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "http://127.0.0.1:8182/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))

    attempts: list[tuple[str, int]] = []

    class _FakeServer:
        def __init__(self, server_address, service) -> None:
            del service
            attempts.append(server_address)
            if server_address[1] == 8182:
                raise OSError(errno.EADDRINUSE, "Address already in use")
            self.server_address = server_address

        def serve_forever(self) -> None:
            return

        def server_close(self) -> None:
            return

    monkeypatch.setattr(schwab_token_bootstrap_web, "_SchwabTokenBootstrapHttpServer", _FakeServer)

    result = schwab_token_bootstrap_web.run_schwab_token_bootstrap_server(
        host="127.0.0.1",
        port=8182,
        token_file=token_path,
        open_browser=False,
        info_file=info_path,
    )

    assert attempts == [("127.0.0.1", 8182), ("127.0.0.1", 8183)]
    assert result["port"] == 8183
    assert result["url"] == "http://127.0.0.1:8183/"
    launch_info = json.loads(info_path.read_text(encoding="utf-8"))
    assert launch_info["port"] == 8183
    assert launch_info["url"] == "http://127.0.0.1:8183/"
    assert launch_info["token_file"] == str(token_path.resolve())


def test_local_authorize_uses_existing_loopback_helper(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    token_path = tmp_path / "tokens.json"
    monkeypatch.setenv("SCHWAB_APP_KEY", "app-key")
    monkeypatch.setenv("SCHWAB_APP_SECRET", "app-secret")
    monkeypatch.setenv("SCHWAB_CALLBACK_URL", "https://127.0.0.1:8818/callback")
    monkeypatch.setenv("SCHWAB_TOKEN_FILE", str(token_path))

    def _fake_run_loopback_authorization(
        oauth_client,
        *,
        state: str,
        scope: str | None,
        timeout_seconds: int,
        open_browser: bool,
        browser_opener,
        event_callback,
    ) -> SchwabLoopbackAuthResult:
        del oauth_client, browser_opener
        assert state == "state-123"
        assert scope == "marketdata"
        assert timeout_seconds == 120
        assert open_browser is True
        event_callback(
            {
                "stage": "exchange_succeeded",
                "callback_received": True,
                "auth_code_parsed": True,
                "exchange_attempted": True,
                "exchange_success": True,
                "token_write_attempted": True,
                "token_write_path": str(token_path.resolve()),
                "token_write_success": True,
                "error": None,
            }
        )
        return SchwabLoopbackAuthResult(
            authorize_url="https://example.test/auth",
            callback_url="https://127.0.0.1:8818/callback",
            browser_opened=True,
            token_file=str(token_path),
            tls_cert_file=str(tmp_path / "loopback-cert.pem"),
            tls_key_file=str(tmp_path / "loopback-key.pem"),
            access_token_expires_at=datetime.now(timezone.utc).isoformat(),
            token_scope="marketdata",
        )

    monkeypatch.setattr(schwab_token_bootstrap_web, "run_loopback_authorization", _fake_run_loopback_authorization)

    service = SchwabTokenBootstrapService(
        transport_factory=lambda: _FakeJsonTransport([]),
        market_data_probe=lambda: {"probe_symbol": "MGC", "quote_count": 1, "external_symbol": "/MGC"},
    )
    result = service.run_local_authorize(state="state-123", scope="marketdata", timeout_seconds=120)

    assert result["callback_url"] == "https://127.0.0.1:8818/callback"
    assert result["browser_opened"] is True
    assert result["message"] == "Local authorize completed successfully."
    assert result["last_operation"]["exchange_success"] is True
    assert result["last_operation"]["token_write_success"] is True
