"""Tests for the local loopback Schwab OAuth helper."""

from __future__ import annotations

from pathlib import Path
import subprocess

import pytest

from mgc_v05l.market_data.schwab_auth import SchwabAuthError, SchwabOAuthClient, SchwabTokenStore
from mgc_v05l.market_data import schwab_local_auth
from mgc_v05l.market_data.schwab_local_auth import run_loopback_authorization
from mgc_v05l.market_data.schwab_models import HttpRequest, JsonHttpTransport, SchwabAuthConfig


class _FakeJsonTransport(JsonHttpTransport):
    def __init__(self, responses: list[dict]) -> None:
        self._responses = list(responses)
        self.requests: list[HttpRequest] = []

    def request_json(self, request: HttpRequest) -> dict:
        self.requests.append(request)
        if not self._responses:
            raise AssertionError("Unexpected extra HTTP request.")
        return self._responses.pop(0)


def _build_client(token_path: Path, callback_url: str, transport: JsonHttpTransport) -> SchwabOAuthClient:
    return SchwabOAuthClient(
        config=SchwabAuthConfig(
            app_key="app-key",
            app_secret="app-secret",
            callback_url=callback_url,
            token_store_path=token_path,
        ),
        transport=transport,
        token_store=SchwabTokenStore(token_path),
    )


def test_loopback_auth_rejects_https_callback(tmp_path: Path) -> None:
    client = _build_client(
        tmp_path / "tokens.json",
        "ftp://127.0.0.1:8182/callback",
        _FakeJsonTransport([]),
    )

    with pytest.raises(SchwabAuthError, match="HTTP\\(S\\)"):
        run_loopback_authorization(
            client,
            state="state-123",
            scope=None,
            timeout_seconds=1,
            open_browser=False,
        )


def test_loopback_auth_rejects_non_localhost_callback(tmp_path: Path) -> None:
    client = _build_client(
        tmp_path / "tokens.json",
        "http://example.com:8182/callback",
        _FakeJsonTransport([]),
    )

    with pytest.raises(SchwabAuthError, match="localhost/127.0.0.1"):
        run_loopback_authorization(
            client,
            state="state-123",
            scope=None,
            timeout_seconds=1,
            open_browser=False,
        )


def test_loopback_auth_exchanges_callback_code_and_persists_tokens(tmp_path: Path) -> None:
    token_path = tmp_path / "tokens.json"
    transport = _FakeJsonTransport(
        [
            {
                "access_token": "access-token-123",
                "refresh_token": "refresh-token-456",
                "token_type": "Bearer",
                "expires_in": 1800,
                "scope": "readonly",
            }
        ]
    )
    client = _build_client(token_path, "https://127.0.0.1:8818/callback", transport)

    class _FakeServer:
        def serve_forever(self) -> None:
            callback_state.code = "auth-code-abc"
            callback_state.event.set()

        def shutdown(self) -> None:
            return

        def server_close(self) -> None:
            return

    callback_state = None

    def _fake_build_callback_server(host: str, port: int, path: str, state, event_callback=None):
        del host, port, path, event_callback
        nonlocal callback_state
        callback_state = state
        return _FakeServer()

    def _fake_ensure_loopback_tls_material(*, token_file: Path, hostname: str):
        assert token_file == token_path
        assert hostname == "127.0.0.1"
        return tmp_path / "loopback-cert.pem", tmp_path / "loopback-key.pem"

    def _fake_wrap_server_with_tls(server, cert_path: Path, key_path: Path):
        assert cert_path == tmp_path / "loopback-cert.pem"
        assert key_path == tmp_path / "loopback-key.pem"
        return server

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(schwab_local_auth, "_build_callback_server", _fake_build_callback_server)
    monkeypatch.setattr(schwab_local_auth, "_ensure_loopback_tls_material", _fake_ensure_loopback_tls_material)
    monkeypatch.setattr(schwab_local_auth, "_wrap_server_with_tls", _fake_wrap_server_with_tls)

    result = run_loopback_authorization(
        client,
        state="state-123",
        scope=None,
        timeout_seconds=2,
        open_browser=False,
    )
    monkeypatch.undo()

    stored = client.token_store.load()

    assert result.callback_url == "https://127.0.0.1:8818/callback"
    assert result.browser_opened is False
    assert result.tls_cert_file == str(tmp_path / "loopback-cert.pem")
    assert result.tls_key_file == str(tmp_path / "loopback-key.pem")
    assert stored is not None
    assert stored.access_token == "access-token-123"
    assert stored.refresh_token == "refresh-token-456"
    assert transport.requests[0].form == {
        "grant_type": "authorization_code",
        "code": "auth-code-abc",
        "redirect_uri": "https://127.0.0.1:8818/callback",
        "client_id": "app-key",
    }


def test_loopback_auth_reports_bind_failure_cleanly(tmp_path: Path) -> None:
    client = _build_client(
        tmp_path / "tokens.json",
        "http://127.0.0.1:8182/callback",
        _FakeJsonTransport([]),
    )

    monkeypatch = pytest.MonkeyPatch()

    def _fake_build_callback_server(host: str, port: int, path: str, state, event_callback=None):
        del host, port, path, state, event_callback
        raise OSError(48, "Address already in use")

    monkeypatch.setattr(schwab_local_auth, "_build_callback_server", _fake_build_callback_server)

    with pytest.raises(SchwabAuthError, match="Could not bind callback listener"):
        run_loopback_authorization(
            client,
            state="state-123",
            scope=None,
            timeout_seconds=1,
            open_browser=False,
        )

    monkeypatch.undo()


def test_ensure_loopback_tls_material_generates_cert_and_key(tmp_path: Path) -> None:
    token_path = tmp_path / "schwab" / "tokens.json"

    def _fake_run(command, check, capture_output, text):
        del check, capture_output, text
        key_index = command.index("-keyout") + 1
        cert_index = command.index("-out") + 1
        Path(command[key_index]).write_text("key", encoding="utf-8")
        Path(command[cert_index]).write_text("cert", encoding="utf-8")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(schwab_local_auth.subprocess, "run", _fake_run)

    cert_path, key_path = schwab_local_auth._ensure_loopback_tls_material(
        token_file=token_path,
        hostname="127.0.0.1",
    )
    monkeypatch.undo()

    assert cert_path == token_path.parent / "loopback-cert.pem"
    assert key_path == token_path.parent / "loopback-key.pem"
    assert cert_path.exists()
    assert key_path.exists()
