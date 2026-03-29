"""Loopback OAuth helper for practical local Schwab authorization."""

from __future__ import annotations

import json
import ssl
import subprocess
import tempfile
import threading
import webbrowser
from dataclasses import dataclass, field
from datetime import timezone
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import parse_qs, urlparse

from .schwab_auth import SchwabAuthError, SchwabOAuthClient
from .schwab_models import SchwabTokenSet


@dataclass(frozen=True)
class SchwabLoopbackAuthResult:
    authorize_url: str
    callback_url: str
    browser_opened: bool
    token_file: str
    tls_cert_file: str | None
    tls_key_file: str | None
    access_token_expires_at: str | None
    token_scope: str | None


def run_loopback_authorization(
    oauth_client: SchwabOAuthClient,
    *,
    state: str,
    scope: str | None,
    timeout_seconds: int,
    open_browser: bool = True,
    browser_opener: Callable[[str], bool] = webbrowser.open,
    event_callback: Callable[[dict[str, Any]], None] | None = None,
) -> SchwabLoopbackAuthResult:
    scheme, host, port, path = _validate_loopback_callback_url(oauth_client.config.callback_url)
    authorize_url = oauth_client.build_authorize_url(state, scope=scope)
    callback_state = _CallbackState(expected_state=state)
    _emit_event(
        event_callback,
        stage="callback_listener",
        callback_received=False,
        auth_code_parsed=False,
        exchange_attempted=False,
        exchange_success=False,
        token_write_attempted=False,
        token_write_path=str(oauth_client.token_store.path.expanduser().resolve(strict=False)),
        token_write_success=False,
    )
    try:
        server = _build_callback_server(host, port, path, callback_state, event_callback)
    except OSError as exc:
        _emit_event(
            event_callback,
            stage="callback_listener_bind_failed",
            callback_received=False,
            auth_code_parsed=False,
            exchange_attempted=False,
            exchange_success=False,
            token_write_attempted=False,
            token_write_path=str(oauth_client.token_store.path.expanduser().resolve(strict=False)),
            token_write_success=False,
            error=f"Could not bind callback listener on {oauth_client.config.callback_url}: {exc}",
        )
        raise SchwabAuthError(
            f"Could not bind callback listener on {oauth_client.config.callback_url}: {exc}"
        ) from exc
    tls_cert_file: Path | None = None
    tls_key_file: Path | None = None
    if scheme == "https":
        tls_cert_file, tls_key_file = _ensure_loopback_tls_material(
            token_file=oauth_client.token_store.path,
            hostname=host,
        )
        server = _wrap_server_with_tls(server, tls_cert_file, tls_key_file)

    server_thread = threading.Thread(target=server.serve_forever, daemon=True)
    server_thread.start()
    browser_opened = False
    try:
        if open_browser:
            browser_opened = bool(browser_opener(authorize_url))
        if not callback_state.event.wait(timeout_seconds):
            raise SchwabAuthError(
                f"Timed out waiting for Schwab callback at {oauth_client.config.callback_url} after {timeout_seconds} seconds."
            )
        if callback_state.error_message is not None:
            raise SchwabAuthError(callback_state.error_message)
        if callback_state.code is None:
            raise SchwabAuthError("Callback completed without an authorization code.")
        _emit_event(
            event_callback,
            stage="exchange_started",
            callback_received=True,
            auth_code_parsed=True,
            exchange_attempted=True,
            exchange_success=False,
            token_write_attempted=True,
            token_write_path=str(oauth_client.token_store.path.expanduser().resolve(strict=False)),
            token_write_success=False,
        )
        try:
            token_set = oauth_client.exchange_code(callback_state.code)
        except Exception as exc:
            _emit_event(
                event_callback,
                stage="exchange_failed",
                callback_received=True,
                auth_code_parsed=True,
                exchange_attempted=True,
                exchange_success=False,
                token_write_attempted=True,
                token_write_path=str(oauth_client.token_store.path.expanduser().resolve(strict=False)),
                token_write_success=False,
                error=str(exc),
            )
            raise
        _emit_event(
            event_callback,
            stage="exchange_succeeded",
            callback_received=True,
            auth_code_parsed=True,
            exchange_attempted=True,
            exchange_success=True,
            token_write_attempted=True,
            token_write_path=str(oauth_client.token_store.path.expanduser().resolve(strict=False)),
            token_write_success=oauth_client.token_store.path.exists(),
        )
    finally:
        server.shutdown()
        server.server_close()
        server_thread.join(timeout=1)

    return SchwabLoopbackAuthResult(
        authorize_url=authorize_url,
        callback_url=oauth_client.config.callback_url,
        browser_opened=browser_opened,
        token_file=str(oauth_client.token_store.path),
        tls_cert_file=str(tls_cert_file) if tls_cert_file is not None else None,
        tls_key_file=str(tls_key_file) if tls_key_file is not None else None,
        access_token_expires_at=(
            token_set.expires_at.astimezone(timezone.utc).isoformat()
            if token_set.expires_at is not None
            else None
        ),
        token_scope=token_set.scope,
    )


def _validate_loopback_callback_url(callback_url: str) -> tuple[str, str, int, str]:
    parsed = urlparse(callback_url)
    if parsed.scheme not in {"http", "https"}:
        raise SchwabAuthError(
            f"Loopback listener only supports HTTP(S) callback URLs, received {callback_url!r}."
        )
    if parsed.hostname not in {"127.0.0.1", "localhost"}:
        raise SchwabAuthError(
            f"Loopback listener only supports localhost/127.0.0.1 callback URLs, received {callback_url!r}."
        )
    if parsed.port is None:
        raise SchwabAuthError(f"Loopback callback URL must include an explicit port, received {callback_url!r}.")
    path = parsed.path or "/"
    return parsed.scheme, parsed.hostname, parsed.port, path


@dataclass
class _CallbackState:
    expected_state: str
    code: str | None = None
    error_message: str | None = None
    event: threading.Event = field(default_factory=threading.Event)


def _build_callback_server(
    host: str,
    port: int,
    path: str,
    callback_state: _CallbackState,
    event_callback: Callable[[dict[str, Any]], None] | None = None,
) -> HTTPServer:
    class _Handler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:  # noqa: N802
            parsed = urlparse(self.path)
            if parsed.path != path:
                self.send_response(404)
                self.end_headers()
                self.wfile.write(b"Not found.")
                return

            _emit_event(
                event_callback,
                stage="callback_received",
                callback_received=True,
            )
            params = parse_qs(parsed.query)
            returned_state = params.get("state", [None])[0]
            if returned_state != callback_state.expected_state:
                callback_state.error_message = "State mismatch on Schwab callback."
                _emit_event(
                    event_callback,
                    stage="state_mismatch",
                    callback_received=True,
                    auth_code_parsed=False,
                    error=callback_state.error_message,
                )
                self._write_html(400, "<h1>Schwab auth failed</h1><p>State mismatch.</p>")
                callback_state.event.set()
                return

            if "error" in params:
                callback_state.error_message = params.get("error_description", params["error"])[0]
                _emit_event(
                    event_callback,
                    stage="callback_error",
                    callback_received=True,
                    auth_code_parsed=False,
                    error=callback_state.error_message,
                )
                self._write_html(400, "<h1>Schwab auth failed</h1><p>See terminal for details.</p>")
                callback_state.event.set()
                return

            callback_state.code = params.get("code", [None])[0]
            _emit_event(
                event_callback,
                stage="auth_code_parsed",
                callback_received=True,
                auth_code_parsed=bool(callback_state.code),
                error=None if callback_state.code else "Callback completed without an authorization code.",
            )
            self._write_html(200, "<h1>Schwab auth received</h1><p>You can close this window.</p>")
            callback_state.event.set()

        def log_message(self, format: str, *args) -> None:  # noqa: A003
            return

        def _write_html(self, status: int, body: str) -> None:
            payload = (
                "<html><head><title>Schwab OAuth</title></head>"
                f"<body style='font-family: -apple-system, sans-serif;'>{body}</body></html>"
            ).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "text/html; charset=utf-8")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

    return HTTPServer((host, port), _Handler)


def _emit_event(event_callback: Callable[[dict[str, Any]], None] | None, **payload: Any) -> None:
    if event_callback is not None:
        event_callback(payload)


def _wrap_server_with_tls(server: HTTPServer, cert_path: Path, key_path: Path) -> HTTPServer:
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    context.load_cert_chain(certfile=str(cert_path), keyfile=str(key_path))
    server.socket = context.wrap_socket(server.socket, server_side=True)
    return server


def _ensure_loopback_tls_material(*, token_file: Path, hostname: str) -> tuple[Path, Path]:
    cert_path = token_file.parent / "loopback-cert.pem"
    key_path = token_file.parent / "loopback-key.pem"
    if cert_path.exists() and key_path.exists():
        return cert_path, key_path

    cert_path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", suffix=".cnf", delete=False) as config_file:
        config_file.write(_openssl_loopback_config(hostname))
        config_path = Path(config_file.name)
    try:
        completed = subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-nodes",
                "-newkey",
                "rsa:2048",
                "-sha256",
                "-days",
                "30",
                "-keyout",
                str(key_path),
                "-out",
                str(cert_path),
                "-config",
                str(config_path),
                "-extensions",
                "v3_req",
            ],
            check=False,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise SchwabAuthError("openssl is required to create the local HTTPS loopback certificate.") from exc
    finally:
        config_path.unlink(missing_ok=True)

    if completed.returncode != 0:
        raise SchwabAuthError(
            f"Failed to create local HTTPS loopback certificate with openssl: {completed.stderr.strip() or completed.stdout.strip()}"
        )

    return cert_path, key_path


def _openssl_loopback_config(hostname: str) -> str:
    del hostname
    return "\n".join(
        [
            "[req]",
            "default_bits = 2048",
            "prompt = no",
            "default_md = sha256",
            "distinguished_name = dn",
            "x509_extensions = v3_req",
            "",
            "[dn]",
            "CN = 127.0.0.1",
            "",
            "[v3_req]",
            "subjectAltName = @alt_names",
            "",
            "[alt_names]",
            "IP.1 = 127.0.0.1",
            "DNS.1 = localhost",
        ]
    )


def json_ready_loopback_result(result: SchwabLoopbackAuthResult) -> dict[str, str | bool | None]:
    return json.loads(json.dumps(result.__dict__))
