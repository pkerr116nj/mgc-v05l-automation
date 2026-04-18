"""Shared local auth/bootstrap helpers for Schwab token readiness."""

from __future__ import annotations

import errno
import json
import os
import webbrowser
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable

from ..config_models import load_settings_from_files
from ..market_data import (
    QuoteService,
    SchwabAuthError,
    SchwabMarketDataAdapter,
    SchwabOAuthClient,
    SchwabQuoteHttpClient,
    SchwabQuoteRequest,
    SchwabTokenStore,
    UrllibJsonTransport,
    build_auth_metadata,
    json_ready_loopback_result,
    load_schwab_auth_config_from_env,
    load_schwab_market_data_config,
    run_loopback_authorization,
)

_QUOTE_PLACEHOLDER = "REPLACE_WITH_CONFIRMED_SCHWAB_QUOTE_SYMBOL"


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[3]


def _resolve_path(path: str | Path | None, default: str | Path) -> Path:
    candidate = Path(path or default).expanduser()
    if not candidate.is_absolute():
        candidate = _repo_root() / candidate
    return candidate.resolve(strict=False)


def _with_auth_override(schwab_config: Any, auth_config: Any) -> Any:
    return type(schwab_config)(
        auth=auth_config,
        historical_symbol_map=schwab_config.historical_symbol_map,
        quote_symbol_map=schwab_config.quote_symbol_map,
        timeframe_map=schwab_config.timeframe_map,
        field_map=schwab_config.field_map,
        market_context_quote_symbols=getattr(schwab_config, "market_context_quote_symbols", {}),
        treasury_context_quote_symbols=getattr(schwab_config, "treasury_context_quote_symbols", {}),
        market_data_base_url=schwab_config.market_data_base_url,
        quotes_symbol_query_param=schwab_config.quotes_symbol_query_param,
    )


def _probe_resolution(
    *,
    token_file: str | Path | None,
    schwab_config_path: str | Path | None,
    probe_symbol: str,
) -> dict[str, Any]:
    config_path = _resolve_path(schwab_config_path, "config/schwab.local.json")
    settings = load_settings_from_files([_repo_root() / "config/base.yaml", _repo_root() / "config/replay.yaml"])
    auth_config = load_schwab_auth_config_from_env(token_file)
    schwab_config = _with_auth_override(load_schwab_market_data_config(config_path), auth_config)
    adapter = SchwabMarketDataAdapter(settings, schwab_config)
    quote_symbol = adapter.map_quote_symbol(probe_symbol)
    historical_symbol = adapter.map_historical_symbol(probe_symbol)
    placeholder_fields = []
    for field_name, field_value in (
        ("quote_symbol_map", quote_symbol),
        ("historical_symbol_map", historical_symbol),
        ("market_data_base_url", str(schwab_config.market_data_base_url or "")),
        ("quotes_symbol_query_param", str(schwab_config.quotes_symbol_query_param or "")),
    ):
        if _QUOTE_PLACEHOLDER in str(field_value):
            placeholder_fields.append({"field": field_name, "value": str(field_value)})
    if placeholder_fields:
        details = ", ".join(f"{item['field']}={item['value']!r}" for item in placeholder_fields)
        raise RuntimeError(
            "Schwab config placeholder error: "
            f"{config_path} still contains {_QUOTE_PLACEHOLDER!r} in probe-relevant fields for {probe_symbol}: {details}"
        )
    return {
        "internal_symbol": probe_symbol,
        "quote_symbol": quote_symbol,
        "historical_symbol": historical_symbol,
        "schwab_config_path": str(config_path),
        "token_file": str(auth_config.token_store_path),
        "market_data_base_url": str(schwab_config.market_data_base_url or ""),
        "quotes_symbol_query_param": str(schwab_config.quotes_symbol_query_param or ""),
    }


class SchwabTokenBootstrapService:
    """Shared auth/bootstrap state machine used by the dashboard and terminal scripts."""

    def __init__(
        self,
        token_file: str | Path | None = None,
        *,
        transport_factory: Callable[[], UrllibJsonTransport] = UrllibJsonTransport,
        browser_opener: Callable[[str], bool] = webbrowser.open,
        schwab_config_path: str | Path | None = None,
        probe_symbol: str = "MGC",
    ) -> None:
        self._auth_config = load_schwab_auth_config_from_env(token_file)
        self._transport_factory = transport_factory
        self._browser_opener = browser_opener
        self._probe_symbol = str(probe_symbol).strip() or "MGC"
        self._probe = _probe_resolution(
            token_file=token_file,
            schwab_config_path=schwab_config_path,
            probe_symbol=self._probe_symbol,
        )
        self._schwab_config_path = Path(self._probe["schwab_config_path"])
        self._last_operation: dict[str, Any] = {
            "callback_received": False,
            "auth_code_parsed": False,
            "exchange_attempted": False,
            "exchange_success": False,
            "token_write_attempted": False,
            "token_write_success": False,
            "error": None,
        }

    @property
    def token_file_path(self) -> Path:
        return self._auth_config.token_store_path

    def _artifact_dir(self) -> Path:
        return self.token_file_path.expanduser().resolve(strict=False).parent / "bootstrap_artifacts"

    def _write_status_snapshot(self, payload: dict[str, Any]) -> None:
        artifact_dir = self._artifact_dir()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / "latest_status.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )

    def _append_event(self, event_type: str, payload: dict[str, Any]) -> None:
        artifact_dir = self._artifact_dir()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        event_payload = {
            "event_type": event_type,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "token_file": str(self.token_file_path.expanduser().resolve(strict=False)),
            "payload": payload,
        }
        with (artifact_dir / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(event_payload, sort_keys=True))
            handle.write("\n")

    def _oauth_client(self) -> SchwabOAuthClient:
        return SchwabOAuthClient(
            config=self._auth_config,
            transport=self._transport_factory(),
            token_store=SchwabTokenStore(self.token_file_path),
        )

    def status(self) -> dict[str, Any]:
        return self._build_status_payload(run_refresh_check=False, run_probe_check=False, force_refresh=False)

    def generate_auth_url(self, state: str, scope: str | None = None) -> dict[str, Any]:
        authorize_url = self._oauth_client().build_authorize_url(state, scope=scope or None)
        payload = {"authorize_url": authorize_url, "state": state, "scope": scope or None}
        self._append_event("auth_url_generated", payload)
        return payload

    def exchange_code(self, code: str) -> dict[str, Any]:
        token_path = self.token_file_path.expanduser().resolve(strict=False)
        self._last_operation.update(
            {
                "auth_code_parsed": bool(str(code).strip()),
                "exchange_attempted": True,
                "token_write_attempted": True,
                "error": None,
            }
        )
        self._append_event("manual_exchange_started", dict(self._last_operation))
        try:
            self._oauth_client().exchange_code(code)
        except Exception as exc:
            self._last_operation["error"] = str(exc)
            self._last_operation["token_write_success"] = token_path.exists()
            self._append_event("manual_exchange_failed", dict(self._last_operation))
            raise
        self._last_operation["exchange_success"] = True
        self._last_operation["token_write_success"] = token_path.exists()
        self._append_event("manual_exchange_succeeded", dict(self._last_operation))
        payload = self._build_status_payload(run_refresh_check=True, run_probe_check=True, force_refresh=False)
        payload["message"] = "Authorization code exchanged successfully."
        return payload

    def refresh_token(self) -> dict[str, Any]:
        payload = self._build_status_payload(run_refresh_check=True, run_probe_check=False, force_refresh=True)
        payload["message"] = "Refresh token exchange succeeded." if payload["refresh_succeeds"] else "Refresh token exchange failed."
        return payload

    def check_runtime_ready(self) -> dict[str, Any]:
        payload = self._build_status_payload(run_refresh_check=True, run_probe_check=True, force_refresh=True)
        payload["message"] = "Token is runtime-ready." if payload["runtime_ready"] else (
            payload["market_data_probe_error"] or payload["refresh_error"] or "Token is not runtime-ready."
        )
        return payload

    def run_local_authorize(self, *, state: str, scope: str | None, timeout_seconds: int) -> dict[str, Any]:
        result = run_loopback_authorization(
            self._oauth_client(),
            state=state,
            scope=scope or None,
            timeout_seconds=timeout_seconds,
            open_browser=True,
            browser_opener=self._browser_opener,
            event_callback=lambda payload: self._last_operation.update(payload),
        )
        payload = json_ready_loopback_result(result)
        payload["message"] = "Local authorize completed successfully."
        return payload

    def _build_status_payload(
        self,
        *,
        run_refresh_check: bool,
        run_probe_check: bool,
        force_refresh: bool,
    ) -> dict[str, Any]:
        token_store = SchwabTokenStore(self.token_file_path)
        token_set = token_store.load()
        token_metadata = token_store.load_metadata() if self.token_file_path.exists() else None
        refresh_succeeds = False
        refresh_error = None
        market_data_probe_succeeds = False
        market_data_probe_error = None
        market_data_probe_result = None

        if run_refresh_check:
            refresh_succeeds, refresh_error, token_set = self._evaluate_refresh(token_set=token_set, force_refresh=force_refresh)
        if run_probe_check and token_set is not None and refresh_succeeds:
            market_data_probe_succeeds, market_data_probe_error, market_data_probe_result = self._evaluate_market_data_probe()

        current_client_identity = build_auth_metadata(self._auth_config)
        token_client_match = (
            token_metadata is not None
            and token_metadata.get("client_key_fingerprint") == current_client_identity["client_key_fingerprint"]
            and token_metadata.get("client_secret_fingerprint") == current_client_identity["client_secret_fingerprint"]
            and token_metadata.get("callback_url") == current_client_identity["callback_url"]
        )
        runtime_ready = bool(token_set is not None and refresh_succeeds and market_data_probe_succeeds)
        payload = {
            "runtime_ready": runtime_ready,
            "callback_url": self._auth_config.callback_url,
            "token_file": str(self.token_file_path.expanduser().resolve(strict=False)),
            "schwab_config_path": str(self._schwab_config_path),
            "probe_symbol": self._probe_symbol,
            "probe_resolution": dict(self._probe),
            "token_exists": token_set is not None if token_set is not None else self.token_file_path.exists(),
            "has_refresh_token": bool(token_set and token_set.refresh_token),
            "token_scope": token_set.scope if token_set is not None else None,
            "access_token_expires_at": (
                token_set.expires_at.astimezone(timezone.utc).isoformat()
                if token_set is not None and token_set.expires_at is not None
                else None
            ),
            "token_expired": token_set.is_expired(datetime.now(timezone.utc)) if token_set is not None else None,
            "refresh_succeeds": refresh_succeeds,
            "refresh_error": refresh_error,
            "market_data_probe_succeeds": market_data_probe_succeeds,
            "market_data_probe_error": market_data_probe_error,
            "market_data_probe_result": market_data_probe_result,
            "current_client_identity": current_client_identity,
            "stored_token_identity": token_metadata,
            "token_client_match": token_client_match if token_metadata is not None else None,
            "artifact_dir": str(self._artifact_dir()),
            "last_operation": dict(self._last_operation),
        }
        self._write_status_snapshot(payload)
        self._append_event("readiness_evaluated", {"runtime_ready": runtime_ready, "probe_resolution": dict(self._probe)})
        return payload

    def _evaluate_refresh(self, *, token_set: Any, force_refresh: bool) -> tuple[bool, str | None, Any]:
        if token_set is None:
            return False, "No token file found.", None
        if not token_set.refresh_token:
            return False, "No refresh token is available in the local token store.", token_set
        try:
            refreshed = self._oauth_client().refresh_token(token_set.refresh_token)
        except Exception as exc:
            return False, str(exc), SchwabTokenStore(self.token_file_path).load()
        return True, None, refreshed if force_refresh else (SchwabTokenStore(self.token_file_path).load() or refreshed)

    def _evaluate_market_data_probe(self) -> tuple[bool, str | None, dict[str, Any] | None]:
        try:
            result = self._run_market_data_probe()
        except Exception as exc:
            return False, str(exc), None
        return True, None, result

    def _run_market_data_probe(self) -> dict[str, Any]:
        settings = load_settings_from_files([_repo_root() / "config/base.yaml", _repo_root() / "config/replay.yaml"])
        schwab_config = _with_auth_override(load_schwab_market_data_config(self._schwab_config_path), self._auth_config)
        service = QuoteService(
            adapter=SchwabMarketDataAdapter(settings, schwab_config),
            client=SchwabQuoteHttpClient(
                oauth_client=self._oauth_client(),
                market_data_config=schwab_config,
                transport=self._transport_factory(),
            ),
        )
        quotes = service.fetch_quotes(SchwabQuoteRequest(internal_symbols=(self._probe_symbol,)))
        if not quotes:
            raise RuntimeError(f"No quote returned for probe symbol {self._probe_symbol}.")
        first_quote = quotes[0]
        return {
            "probe_symbol": self._probe_symbol,
            "quote_count": len(quotes),
            "configured_quote_symbol": self._probe["quote_symbol"],
            "configured_historical_symbol": self._probe["historical_symbol"],
            "returned_symbol": first_quote.raw_payload.get("symbol"),
            "reference_product": (first_quote.reference_future or {}).get("product"),
        }

    def last_operation_status(self) -> dict[str, Any]:
        return dict(self._last_operation)


class _BootstrapServer(ThreadingHTTPServer):
    def __init__(self, server_address: tuple[str, int], service: SchwabTokenBootstrapService):
        super().__init__(server_address, _BootstrapHandler)
        self.service = service


class _BootstrapHandler(BaseHTTPRequestHandler):
    server: _BootstrapServer

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/":
            self._write_html(_HTML_PAGE)
            return
        if self.path == "/api/status":
            self._write_json(200, self.server.service.status())
            return
        self._write_json(404, {"error": "Not found"})

    def do_POST(self) -> None:  # noqa: N802
        try:
            payload = self._read_json_body()
            if self.path == "/api/auth-url":
                self._write_json(200, self.server.service.generate_auth_url(str(payload.get("state") or "mgc-v05l-local"), payload.get("scope")))
                return
            if self.path == "/api/local-authorize":
                self._write_json(
                    200,
                    self.server.service.run_local_authorize(
                        state=str(payload.get("state") or "mgc-v05l-local"),
                        scope=str(payload.get("scope")) if payload.get("scope") not in (None, "") else None,
                        timeout_seconds=int(payload.get("timeout_seconds") or 180),
                    ),
                )
                return
            if self.path == "/api/exchange-code":
                self._write_json(200, self.server.service.exchange_code(str(payload.get("code") or "")))
                return
            if self.path == "/api/refresh":
                self._write_json(200, self.server.service.refresh_token())
                return
            if self.path == "/api/runtime-ready":
                self._write_json(200, self.server.service.check_runtime_ready())
                return
            self._write_json(404, {"error": "Not found"})
        except (RuntimeError, SchwabAuthError, ValueError) as exc:
            self._write_json(400, {"error": str(exc), "last_operation": self.server.service.last_operation_status()})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw = self.rfile.read(length).decode("utf-8")
        return json.loads(raw) if raw.strip() else {}

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _write_html(self, html: str) -> None:
        body = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)


def run_schwab_token_bootstrap_server(
    *,
    host: str,
    port: int,
    token_file: str | Path | None = None,
    open_browser: bool = True,
    info_file: str | Path | None = None,
    port_search_limit: int = 25,
    schwab_config_path: str | Path | None = None,
    probe_symbol: str = "MGC",
) -> dict[str, Any]:
    service = SchwabTokenBootstrapService(
        token_file=token_file,
        schwab_config_path=schwab_config_path,
        probe_symbol=probe_symbol,
    )
    server, bound_port = _bind_server_with_fallback(host, port, service, port_search_limit)
    url = f"http://{host}:{bound_port}/"
    resolved_info_file = _write_launch_info(
        info_file=info_file,
        host=host,
        port=bound_port,
        url=url,
        token_file=str(service.token_file_path.expanduser().resolve(strict=False)),
        schwab_config_path=str(service._schwab_config_path),
    )
    print(f"Schwab token web listening on {url}", flush=True)
    print(f"Resolved token path: {service.token_file_path.expanduser().resolve(strict=False)}", flush=True)
    print(f"Resolved Schwab config path: {service._schwab_config_path}", flush=True)
    if resolved_info_file is not None:
        print(f"Launch info written to {resolved_info_file}", flush=True)
    if open_browser:
        webbrowser.open(url)
    try:
        server.serve_forever()
    finally:
        server.server_close()
    return {"host": host, "port": bound_port, "url": url, "info_file": str(resolved_info_file) if resolved_info_file else None}


def _bind_server_with_fallback(host: str, start_port: int, service: SchwabTokenBootstrapService, port_search_limit: int) -> tuple[_BootstrapServer, int]:
    last_error: OSError | None = None
    for offset in range(max(port_search_limit, 0) + 1):
        port = start_port + offset
        try:
            return _BootstrapServer((host, port), service), port
        except OSError as exc:
            if exc.errno != errno.EADDRINUSE:
                raise
            last_error = exc
    raise OSError(errno.EADDRINUSE, f"Could not bind {host} on port {start_port} or the next {port_search_limit} ports.") from last_error


def _write_launch_info(
    *,
    info_file: str | Path | None,
    host: str,
    port: int,
    url: str,
    token_file: str,
    schwab_config_path: str,
) -> Path | None:
    if info_file is None:
        return None
    resolved = Path(info_file).expanduser().resolve(strict=False)
    resolved.parent.mkdir(parents=True, exist_ok=True)
    resolved.write_text(
        json.dumps(
            {
                "host": host,
                "port": port,
                "url": url,
                "token_file": token_file,
                "schwab_config_path": schwab_config_path,
                "written_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return resolved


_HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Schwab Token Bootstrap</title>
  <style>
    body { font-family: -apple-system, sans-serif; margin: 32px; background: #f6f7fb; color: #18202f; }
    main { max-width: 900px; margin: 0 auto; }
    section { background: white; padding: 20px; border-radius: 12px; box-shadow: 0 10px 30px rgba(20,30,60,0.08); margin-bottom: 16px; }
    button { margin-right: 8px; margin-bottom: 8px; padding: 10px 14px; border: 0; border-radius: 8px; background: #1743b3; color: white; cursor: pointer; }
    input { width: 100%; padding: 10px 12px; margin-bottom: 8px; border: 1px solid #ccd3e0; border-radius: 8px; box-sizing: border-box; }
    pre { background: #eef2fb; padding: 12px; border-radius: 8px; white-space: pre-wrap; }
  </style>
</head>
<body>
  <main>
    <section>
      <h1>Schwab Token Bootstrap</h1>
      <p>This shared bootstrap helper uses the same token file, Schwab config, and probe symbol as the auth gate.</p>
      <button onclick="status()">Refresh Status</button>
      <button onclick="runtimeReady()">Check Runtime Ready</button>
      <button onclick="refreshToken()">Refresh Token</button>
      <button onclick="authUrl()">Generate Auth URL</button>
      <button onclick="localAuthorize()">Local Authorize</button>
      <input id="code" placeholder="Paste Schwab authorization code for manual exchange">
      <button onclick="exchangeCode()">Exchange Code</button>
    </section>
    <section>
      <h2>Response</h2>
      <pre id="out">Loading...</pre>
    </section>
  </main>
  <script>
    async function call(path, payload) {
      const response = await fetch(path, {
        method: payload ? "POST" : "GET",
        headers: payload ? {"Content-Type": "application/json"} : {},
        body: payload ? JSON.stringify(payload) : undefined
      });
      const data = await response.json();
      document.getElementById("out").textContent = JSON.stringify(data, null, 2);
      return data;
    }
    function status() { return call("/api/status"); }
    function runtimeReady() { return call("/api/runtime-ready", {}); }
    function refreshToken() { return call("/api/refresh", {}); }
    function authUrl() { return call("/api/auth-url", {state: "mgc-v05l-local"}); }
    function localAuthorize() { return call("/api/local-authorize", {state: "mgc-v05l-local", timeout_seconds: 180}); }
    function exchangeCode() { return call("/api/exchange-code", {code: document.getElementById("code").value}); }
    status();
  </script>
</body>
</html>
"""
