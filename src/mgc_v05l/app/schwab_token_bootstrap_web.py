"""Local browser UI for Schwab token bootstrap and refresh."""

from __future__ import annotations

import errno
import os
import json
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


class SchwabTokenBootstrapService:
    """Thin service wrapper around the existing Schwab auth/token helpers."""

    def __init__(
        self,
        token_file: str | Path | None = None,
        transport_factory: Callable[[], UrllibJsonTransport] = UrllibJsonTransport,
        browser_opener: Callable[[str], bool] = webbrowser.open,
        schwab_config_path: str | Path | None = None,
        probe_symbol: str = "MGC",
        market_data_probe: Callable[[], dict[str, Any]] | None = None,
    ) -> None:
        self._auth_config = load_schwab_auth_config_from_env(token_file)
        self._transport_factory = transport_factory
        self._browser_opener = browser_opener
        schwab_config = Path(schwab_config_path or os.environ.get("SCHWAB_CONFIG", "config/schwab.local.json")).expanduser()
        if not schwab_config.is_absolute():
            schwab_config = Path(os.environ.get("REPO_ROOT", os.getcwd())) / schwab_config
        self._schwab_config_path = schwab_config.resolve(strict=False)
        self._probe_symbol = probe_symbol
        self._market_data_probe = market_data_probe
        self._last_operation: dict[str, Any] = self._empty_operation_status()
        self._auth_url_generated = False

    @property
    def callback_url(self) -> str:
        return self._auth_config.callback_url

    @property
    def token_file_path(self) -> Path:
        return self._auth_config.token_store_path

    def status(self) -> dict[str, Any]:
        return self._build_status_payload(run_refresh_check=True, run_probe_check=True)

    def generate_auth_url(self, state: str, scope: str | None = None) -> dict[str, Any]:
        client = self._oauth_client()
        self._auth_url_generated = True
        payload = {
            "authorize_url": client.build_authorize_url(state, scope=scope or None),
            "state": state,
            "scope": scope or None,
        }
        self._append_event("auth_url_generated", payload)
        return payload

    def exchange_code(self, code: str) -> dict[str, Any]:
        token_path = self.token_file_path.expanduser().resolve(strict=False)
        self._last_operation = self._empty_operation_status(operation="manual_exchange", token_write_path=str(token_path))
        self._last_operation["auth_code_parsed"] = bool(code.strip())
        self._last_operation["exchange_attempted"] = True
        self._last_operation["token_write_attempted"] = True
        self._append_event("manual_exchange_started", self._last_operation)
        try:
            self._oauth_client().exchange_code(code)
        except Exception as exc:
            self._last_operation["exchange_success"] = False
            self._last_operation["token_write_success"] = token_path.exists()
            self._last_operation["error"] = str(exc)
            self._append_event("manual_exchange_failed", self._last_operation)
            raise
        self._last_operation["exchange_success"] = True
        self._last_operation["token_write_success"] = token_path.exists()
        self._append_event("manual_exchange_succeeded", self._last_operation)
        payload = self._build_status_payload(run_refresh_check=True, run_probe_check=True)
        payload["message"] = "Authorization code exchanged successfully."
        return payload

    def refresh_token(self) -> dict[str, Any]:
        payload = self._build_status_payload(run_refresh_check=True, run_probe_check=True, force_refresh=True)
        payload["message"] = (
            "Refresh token exchange succeeded."
            if payload["refresh_succeeds"]
            else "Refresh token exchange failed."
        )
        return payload

    def check_runtime_ready(self) -> dict[str, Any]:
        payload = self._build_status_payload(run_refresh_check=True, run_probe_check=True, force_refresh=True)
        payload["message"] = (
            "Token is runtime-ready."
            if payload["runtime_ready"]
            else (
                payload["market_data_probe_error"]
                or payload["refresh_error"]
                or "Token is not runtime-ready."
            )
        )
        return payload

    def run_local_authorize(
        self,
        *,
        state: str,
        scope: str | None,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        token_path = self.token_file_path.expanduser().resolve(strict=False)
        self._last_operation = self._empty_operation_status(operation="local_authorize", token_write_path=str(token_path))
        self._auth_url_generated = True
        self._append_event("local_authorize_started", self._last_operation)
        result = run_loopback_authorization(
            self._oauth_client(),
            state=state,
            scope=scope or None,
            timeout_seconds=timeout_seconds,
            open_browser=True,
            browser_opener=self._browser_opener,
            event_callback=self._record_auth_event,
        )
        payload = json_ready_loopback_result(result)
        payload["message"] = "Local authorize completed successfully."
        payload["last_operation"] = self._last_operation
        self._append_event("local_authorize_completed", payload)
        return payload

    def _oauth_client(self) -> SchwabOAuthClient:
        return SchwabOAuthClient(
            config=self._auth_config,
            transport=self._transport_factory(),
            token_store=self._token_store(),
        )

    def _token_store(self) -> SchwabTokenStore:
        return SchwabTokenStore(self.token_file_path)

    def _build_status_payload(
        self,
        *,
        run_refresh_check: bool,
        run_probe_check: bool = False,
        force_refresh: bool = False,
    ) -> dict[str, Any]:
        token_path = self.token_file_path
        resolved_token_path = token_path.expanduser().resolve(strict=False)
        token_set = self._token_store().load()
        token_metadata = self._token_store().load_metadata() if resolved_token_path.exists() else None
        refresh_succeeds: bool = False
        refresh_error: str | None = None
        market_data_probe_succeeds: bool = False
        market_data_probe_error: str | None = None
        market_data_probe_result: dict[str, Any] | None = None

        if run_refresh_check:
            refresh_succeeds, refresh_error, token_set = self._evaluate_refresh(
                token_set=token_set,
                force_refresh=force_refresh,
            )
        if run_probe_check and token_set is not None and refresh_succeeds:
            market_data_probe_succeeds, market_data_probe_error, market_data_probe_result = (
                self._evaluate_market_data_probe()
            )

        token_exists = token_set is not None if token_set is not None else token_path.exists()
        runtime_ready = bool(token_exists and refresh_succeeds and market_data_probe_succeeds)
        current_client_identity = build_auth_metadata(self._auth_config)
        token_client_match = (
            token_metadata is not None
            and token_metadata.get("client_key_fingerprint") == current_client_identity["client_key_fingerprint"]
            and token_metadata.get("client_secret_fingerprint") == current_client_identity["client_secret_fingerprint"]
            and token_metadata.get("callback_url") == current_client_identity["callback_url"]
        )
        step_state = {
            "token_missing": not token_exists,
            "auth_url_generated": self._auth_url_generated,
            "callback_received": bool(self._last_operation.get("callback_received")),
            "code_parsed": bool(self._last_operation.get("auth_code_parsed")),
            "exchange_attempted": bool(self._last_operation.get("exchange_attempted")),
            "exchange_succeeded": bool(self._last_operation.get("exchange_success")),
            "token_written": bool(token_exists),
            "refresh_attempted": run_refresh_check,
            "refresh_succeeded": refresh_succeeds,
            "market_data_probe_attempted": run_probe_check and token_set is not None and refresh_succeeds,
            "market_data_probe_succeeded": market_data_probe_succeeds,
            "runtime_ready": runtime_ready,
        }

        payload = {
            "callback_url": self.callback_url,
            "token_file": str(resolved_token_path),
            "token_exists": token_exists,
            "has_refresh_token": bool(token_set and token_set.refresh_token),
            "token_scope": token_set.scope if token_set is not None else None,
            "access_token_expires_at": (
                token_set.expires_at.astimezone(timezone.utc).isoformat()
                if token_set is not None and token_set.expires_at is not None
                else None
            ),
            "token_expired": (
                token_set.is_expired(datetime.now(timezone.utc))
                if token_set is not None
                else None
            ),
            "refresh_succeeds": refresh_succeeds,
            "refresh_error": refresh_error,
            "market_data_probe_succeeds": market_data_probe_succeeds,
            "market_data_probe_error": market_data_probe_error,
            "market_data_probe_result": market_data_probe_result,
            "runtime_ready": runtime_ready,
            "refresh_checked_at": datetime.now(timezone.utc).isoformat() if run_refresh_check else None,
            "market_data_probe_checked_at": (
                datetime.now(timezone.utc).isoformat()
                if run_probe_check and token_set is not None and refresh_succeeds
                else None
            ),
            "current_client_identity": current_client_identity,
            "stored_token_identity": token_metadata,
            "token_client_match": token_client_match if token_metadata is not None else None,
            "schwab_config_path": str(self._schwab_config_path),
            "probe_symbol": self._probe_symbol,
            "step_state": step_state,
            "commands": {
                "cli_refresh": (
                    "PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.main "
                    f'schwab-refresh-token --token-file "{resolved_token_path}"'
                ),
                "launch_web": (
                    f'bash scripts/run_schwab_token_web.sh --token-file "{resolved_token_path}"'
                ),
                "auth_gate": (
                    "PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.main "
                    f'schwab-auth-gate --token-file "{resolved_token_path}" '
                    f'--schwab-config "{self._schwab_config_path}" --internal-symbol "{self._probe_symbol}"'
                ),
                "market_data_probe": (
                    "PYTHONPATH=src .venv/bin/python -m mgc_v05l.app.main "
                    f'schwab-fetch-quote --token-file "{resolved_token_path}" '
                    f'--schwab-config "{self._schwab_config_path}" --internal-symbol "{self._probe_symbol}"'
                ),
            },
            "debug": {
                "cwd": os.getcwd(),
                "resolved_token_path": str(resolved_token_path),
                "resolved_schwab_config_path": str(self._schwab_config_path),
                "token_exists": token_path.exists(),
                "current_client_identity": current_client_identity,
                "stored_token_identity": token_metadata,
                "token_client_match": token_client_match if token_metadata is not None else None,
                "refresh_succeeds": refresh_succeeds,
                "refresh_error": refresh_error,
                "market_data_probe_succeeds": market_data_probe_succeeds,
                "market_data_probe_error": market_data_probe_error,
                "last_operation": self._last_operation,
                "artifact_dir": str(self._artifact_dir()),
            },
            "last_operation": self._last_operation,
        }
        self._write_status_snapshot(payload)
        self._append_event(
            "readiness_evaluated",
            {
                "runtime_ready": runtime_ready,
                "token_file": str(resolved_token_path),
                "refresh_succeeds": refresh_succeeds,
                "market_data_probe_succeeds": market_data_probe_succeeds,
                "step_state": step_state,
            },
        )
        return payload

    def _evaluate_refresh(
        self,
        *,
        token_set,
        force_refresh: bool,
    ) -> tuple[bool, str | None, Any]:
        if token_set is None:
            return False, "No token file found.", None
        if not token_set.refresh_token:
            return False, "No refresh token is available in the local token store.", token_set

        self._append_event("refresh_attempted", {"token_file": str(self.token_file_path.expanduser().resolve(strict=False))})
        try:
            refreshed = self._oauth_client().refresh_token(token_set.refresh_token)
        except Exception as exc:
            self._append_event("refresh_failed", {"error": str(exc)})
            return False, str(exc), self._token_store().load()

        if force_refresh:
            token_set = refreshed
        else:
            token_set = self._token_store().load() or refreshed
        self._append_event("refresh_succeeded", {"token_file": str(self.token_file_path.expanduser().resolve(strict=False))})
        return True, None, token_set

    def _evaluate_market_data_probe(self) -> tuple[bool, str | None, dict[str, Any] | None]:
        self._append_event("market_data_probe_attempted", {"probe_symbol": self._probe_symbol})
        try:
            result = (
                self._market_data_probe()
                if self._market_data_probe is not None
                else self._run_market_data_probe()
            )
        except Exception as exc:
            self._append_event("market_data_probe_failed", {"probe_symbol": self._probe_symbol, "error": str(exc)})
            return False, str(exc), None
        self._append_event("market_data_probe_succeeded", result)
        return True, None, result

    def _run_market_data_probe(self) -> dict[str, Any]:
        repo_root = Path(__file__).resolve().parents[3]
        settings = load_settings_from_files([repo_root / "config/base.yaml", repo_root / "config/replay.yaml"])
        schwab_config = load_schwab_market_data_config(self._schwab_config_path)
        schwab_config = type(schwab_config)(
            auth=self._auth_config,
            historical_symbol_map=schwab_config.historical_symbol_map,
            quote_symbol_map=schwab_config.quote_symbol_map,
            timeframe_map=schwab_config.timeframe_map,
            field_map=schwab_config.field_map,
            market_data_base_url=schwab_config.market_data_base_url,
            quotes_symbol_query_param=schwab_config.quotes_symbol_query_param,
        )
        adapter = SchwabMarketDataAdapter(settings, schwab_config)
        service = QuoteService(
            adapter=adapter,
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
            "external_symbol": first_quote.external_symbol,
            "returned_symbol": first_quote.raw_payload.get("symbol"),
            "reference_product": (first_quote.reference_future or {}).get("product"),
        }

    def _record_auth_event(self, payload: dict[str, Any]) -> None:
        self._last_operation.update(payload)
        self._append_event("auth_event", payload)

    def _empty_operation_status(
        self,
        *,
        operation: str | None = None,
        token_write_path: str | None = None,
    ) -> dict[str, Any]:
        return {
            "operation": operation,
            "callback_received": False,
            "auth_code_parsed": False,
            "exchange_attempted": False,
            "exchange_success": False,
            "token_write_attempted": False,
            "token_write_path": token_write_path,
            "token_write_success": False,
            "error": None,
        }

    def last_operation_status(self) -> dict[str, Any]:
        return dict(self._last_operation)

    def _artifact_dir(self) -> Path:
        return self.token_file_path.expanduser().resolve(strict=False).parent / "bootstrap_artifacts"

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

    def _write_status_snapshot(self, payload: dict[str, Any]) -> None:
        artifact_dir = self._artifact_dir()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / "latest_status.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )


class _SchwabTokenBootstrapHttpServer(ThreadingHTTPServer):
    def __init__(self, server_address, service: SchwabTokenBootstrapService):
        super().__init__(server_address, _SchwabTokenBootstrapHandler)
        self.service = service


class _SchwabTokenBootstrapHandler(BaseHTTPRequestHandler):
    server: _SchwabTokenBootstrapHttpServer

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
                result = self.server.service.generate_auth_url(
                    state=str(payload.get("state") or "mgc-v05l-local"),
                    scope=str(payload.get("scope")) if payload.get("scope") not in (None, "") else None,
                )
                self._write_json(200, result)
                return
            if self.path == "/api/local-authorize":
                result = self.server.service.run_local_authorize(
                    state=str(payload.get("state") or "mgc-v05l-local"),
                    scope=str(payload.get("scope")) if payload.get("scope") not in (None, "") else None,
                    timeout_seconds=int(payload.get("timeout_seconds") or 180),
                )
                self._write_json(200, result)
                return
            if self.path == "/api/exchange-code":
                result = self.server.service.exchange_code(str(payload.get("code") or ""))
                self._write_json(200, result)
                return
            if self.path == "/api/refresh":
                result = self.server.service.refresh_token()
                self._write_json(200, result)
                return
            if self.path == "/api/runtime-ready":
                result = self.server.service.check_runtime_ready()
                self._write_json(200, result)
                return
            self._write_json(404, {"error": "Not found"})
        except SchwabAuthError as exc:
            self._write_json(400, {"error": str(exc), "last_operation": self.server.service.last_operation_status()})
        except RuntimeError as exc:
            self._write_json(400, {"error": str(exc), "last_operation": self.server.service.last_operation_status()})
        except Exception as exc:  # pragma: no cover - defensive HTTP boundary
            self._write_json(500, {"error": str(exc), "last_operation": self.server.service.last_operation_status()})

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return

    def _read_json_body(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length", "0") or "0")
        if length <= 0:
            return {}
        raw_body = self.rfile.read(length).decode("utf-8")
        if not raw_body.strip():
            return {}
        return json.loads(raw_body)

    def _write_html(self, html: str) -> None:
        payload = html.encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def _write_json(self, status: int, payload: dict[str, Any]) -> None:
        body = json.dumps(payload, sort_keys=True).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
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
    server, bound_port = _bind_server_with_fallback(
        host=host,
        start_port=port,
        service=service,
        port_search_limit=port_search_limit,
    )
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
    return {
        "host": host,
        "port": bound_port,
        "url": url,
        "info_file": str(resolved_info_file) if resolved_info_file is not None else None,
    }


def _bind_server_with_fallback(
    *,
    host: str,
    start_port: int,
    service: SchwabTokenBootstrapService,
    port_search_limit: int,
) -> tuple[_SchwabTokenBootstrapHttpServer, int]:
    last_error: OSError | None = None
    for offset in range(max(port_search_limit, 0) + 1):
        candidate_port = start_port + offset
        try:
            return _SchwabTokenBootstrapHttpServer((host, candidate_port), service), candidate_port
        except OSError as exc:
            if exc.errno != errno.EADDRINUSE:
                raise
            last_error = exc
    raise OSError(
        errno.EADDRINUSE,
        f"Could not bind {host} on port {start_port} or the next {port_search_limit} ports.",
    ) from last_error


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
    resolved_info_file = Path(info_file).expanduser().resolve(strict=False)
    resolved_info_file.parent.mkdir(parents=True, exist_ok=True)
    resolved_info_file.write_text(
        json.dumps(
            {
                "host": host,
                "port": port,
                "schwab_config_path": schwab_config_path,
                "token_file": token_file,
                "url": url,
                "written_at": datetime.now(timezone.utc).isoformat(),
            },
            indent=2,
            sort_keys=True,
        ),
        encoding="utf-8",
    )
    return resolved_info_file


_HTML_PAGE = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <title>Schwab Token Bootstrap</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 32px; color: #1d2433; background: #f6f7fb; }
    .wrap { max-width: 980px; margin: 0 auto; }
    .card { background: white; border-radius: 14px; padding: 20px 22px; margin-bottom: 18px; box-shadow: 0 10px 30px rgba(20,30,60,0.08); }
    h1, h2 { margin-top: 0; }
    .grid { display: grid; gap: 12px; grid-template-columns: repeat(auto-fit, minmax(260px, 1fr)); }
    label { display: block; font-size: 13px; font-weight: 600; margin-bottom: 6px; }
    input { width: 100%; padding: 10px 12px; border: 1px solid #ccd3e0; border-radius: 8px; box-sizing: border-box; }
    button { padding: 10px 14px; border: 0; border-radius: 8px; background: #1743b3; color: white; cursor: pointer; margin-right: 8px; margin-bottom: 8px; }
    button.secondary { background: #5f6d8d; }
    button:disabled { background: #99a3bd; cursor: not-allowed; }
    code, pre { background: #eef2fb; border-radius: 8px; padding: 2px 6px; }
    pre { padding: 12px; overflow-x: auto; white-space: pre-wrap; }
    .status { display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
    .status div { background: #f7f9fe; border-radius: 10px; padding: 12px; }
    .label { font-size: 12px; color: #60708f; margin-bottom: 6px; text-transform: uppercase; letter-spacing: 0.04em; }
    .value { font-size: 15px; word-break: break-word; }
    .ok { color: #0d7a3a; }
    .bad { color: #a12f2f; }
    .copyrow { display: flex; gap: 10px; align-items: flex-start; margin-bottom: 12px; }
    .copyrow pre { flex: 1; margin: 0; }
    .steps { display: grid; gap: 10px; grid-template-columns: repeat(auto-fit, minmax(220px, 1fr)); }
    .steps div { background: #f7f9fe; border-radius: 10px; padding: 12px; }
  </style>
</head>
<body>
  <div class="wrap">
    <h1>Schwab Token Bootstrap</h1>
    <div class="card">
      <h2>Status</h2>
        <div class="status">
          <div><div class="label">Callback URL</div><div class="value" id="callbackUrl">-</div></div>
          <div><div class="label">Token File</div><div class="value" id="tokenFile">-</div></div>
          <div><div class="label">Token Exists</div><div class="value" id="tokenExists">-</div></div>
          <div><div class="label">Has Refresh Token</div><div class="value" id="hasRefreshToken">-</div></div>
          <div><div class="label">Refresh Succeeds</div><div class="value" id="refreshSucceeds">-</div></div>
          <div><div class="label">Market-Data Probe Succeeds</div><div class="value" id="probeSucceeds">-</div></div>
          <div><div class="label">Runtime Ready</div><div class="value" id="runtimeReady">-</div></div>
          <div><div class="label">Expires At (UTC)</div><div class="value" id="expiresAt">-</div></div>
          <div><div class="label">Token Scope</div><div class="value" id="tokenScope">-</div></div>
          <div><div class="label">Schwab Config</div><div class="value" id="schwabConfigPath">-</div></div>
          <div><div class="label">Probe Symbol</div><div class="value" id="probeSymbol">-</div></div>
        </div>
        <p><button onclick="refreshStatus()">Refresh Status</button><button class="secondary" onclick="checkRuntimeReady()">Check Runtime Ready</button></p>
      </div>

    <div class="card">
      <h2>Step State</h2>
      <div class="steps">
        <div><div class="label">Token Missing</div><div class="value" id="stepTokenMissing">-</div></div>
        <div><div class="label">Auth URL Generated</div><div class="value" id="stepAuthUrlGenerated">-</div></div>
        <div><div class="label">Callback Received</div><div class="value" id="stepCallbackReceived">-</div></div>
        <div><div class="label">Code Parsed</div><div class="value" id="stepCodeParsed">-</div></div>
        <div><div class="label">Exchange Attempted</div><div class="value" id="stepExchangeAttempted">-</div></div>
        <div><div class="label">Exchange Succeeded</div><div class="value" id="stepExchangeSucceeded">-</div></div>
        <div><div class="label">Token Written</div><div class="value" id="stepTokenWritten">-</div></div>
        <div><div class="label">Refresh Attempted</div><div class="value" id="stepRefreshAttempted">-</div></div>
        <div><div class="label">Refresh Succeeded</div><div class="value" id="stepRefreshSucceeded">-</div></div>
        <div><div class="label">Probe Attempted</div><div class="value" id="stepProbeAttempted">-</div></div>
        <div><div class="label">Probe Succeeded</div><div class="value" id="stepProbeSucceeded">-</div></div>
        <div><div class="label">Runtime Ready</div><div class="value" id="stepRuntimeReady">-</div></div>
      </div>
    </div>

    <div class="card">
      <h2>Debug</h2>
      <pre id="debugBox">-</pre>
    </div>

    <div class="card">
      <h2>CLI Commands</h2>
      <div class="label">Resolved Token Path</div>
      <pre id="resolvedTokenPathBox">-</pre>
      <div class="label">CLI Refresh Command</div>
      <div class="copyrow">
        <pre id="cliRefreshBox">-</pre>
        <button class="secondary" onclick="copyFrom('cliRefreshBox')">Copy</button>
      </div>
      <div class="label">Production Auth Gate</div>
      <div class="copyrow">
        <pre id="authGateBox">-</pre>
        <button class="secondary" onclick="copyFrom('authGateBox')">Copy</button>
      </div>
      <div class="label">Launch Web Tool</div>
      <div class="copyrow">
        <pre id="launchWebBox">-</pre>
        <button class="secondary" onclick="copyFrom('launchWebBox')">Copy</button>
      </div>
    </div>

    <div class="card">
      <h2>Authorize</h2>
      <div class="grid">
        <div><label for="state">OAuth State</label><input id="state" value="mgc-v05l-local"></div>
        <div><label for="scope">Scope (optional)</label><input id="scope" placeholder="leave blank unless needed"></div>
        <div><label for="timeoutSeconds">Local Authorize Timeout</label><input id="timeoutSeconds" value="180"></div>
      </div>
      <p>
        <button onclick="generateAuthUrl()">Generate Auth URL</button>
        <button class="secondary" id="openAuthUrlBtn" onclick="openAuthUrl()" disabled>Open Auth URL</button>
        <button onclick="runLocalAuthorize()">Local Authorize</button>
      </p>
      <div class="label">Auth URL</div>
      <pre id="authUrlBox">-</pre>
    </div>

    <div class="card">
      <h2>Manual Code Exchange</h2>
      <label for="authCode">Authorization Code</label>
      <input id="authCode" placeholder="paste the Schwab authorization code here">
      <p><button onclick="exchangeCode()">Exchange Code</button><button class="secondary" onclick="refreshToken()">Refresh Token</button></p>
    </div>

    <div class="card">
      <h2>Response</h2>
      <pre id="responseBox">-</pre>
    </div>
  </div>

  <script>
    let currentAuthUrl = null;

    async function api(path, payload) {
      const response = await fetch(path, {
        method: payload ? "POST" : "GET",
        headers: payload ? { "Content-Type": "application/json" } : {},
        body: payload ? JSON.stringify(payload) : undefined,
      });
      const data = await response.json();
      document.getElementById("responseBox").textContent = JSON.stringify(data, null, 2);
      if (!response.ok) {
        throw new Error(data.error || "Request failed");
      }
      return data;
    }

    function statusText(value) {
      if (value === true) return '<span class="ok">yes</span>';
      if (value === false) return '<span class="bad">no</span>';
      return '-';
    }

    async function refreshStatus() {
      const data = await api('/api/status');
      document.getElementById('callbackUrl').textContent = data.callback_url || '-';
      document.getElementById('tokenFile').textContent = data.token_file || '-';
      document.getElementById('tokenExists').innerHTML = statusText(data.token_exists);
      document.getElementById('hasRefreshToken').innerHTML = statusText(data.has_refresh_token);
      document.getElementById('refreshSucceeds').innerHTML = statusText(data.refresh_succeeds);
      document.getElementById('probeSucceeds').innerHTML = statusText(data.market_data_probe_succeeds);
      document.getElementById('runtimeReady').innerHTML = statusText(data.runtime_ready);
      document.getElementById('expiresAt').textContent = data.access_token_expires_at || '-';
      document.getElementById('tokenScope').textContent = data.token_scope || '-';
      document.getElementById('schwabConfigPath').textContent = data.schwab_config_path || '-';
      document.getElementById('probeSymbol').textContent = data.probe_symbol || '-';
      document.getElementById('resolvedTokenPathBox').textContent = (data.debug || {}).resolved_token_path || data.token_file || '-';
      document.getElementById('cliRefreshBox').textContent = (data.commands || {}).cli_refresh || '-';
      document.getElementById('authGateBox').textContent = (data.commands || {}).auth_gate || '-';
      document.getElementById('launchWebBox').textContent = (data.commands || {}).launch_web || '-';
      const steps = data.step_state || {};
      document.getElementById('stepTokenMissing').innerHTML = statusText(steps.token_missing);
      document.getElementById('stepAuthUrlGenerated').innerHTML = statusText(steps.auth_url_generated);
      document.getElementById('stepCallbackReceived').innerHTML = statusText(steps.callback_received);
      document.getElementById('stepCodeParsed').innerHTML = statusText(steps.code_parsed);
      document.getElementById('stepExchangeAttempted').innerHTML = statusText(steps.exchange_attempted);
      document.getElementById('stepExchangeSucceeded').innerHTML = statusText(steps.exchange_succeeded);
      document.getElementById('stepTokenWritten').innerHTML = statusText(steps.token_written);
      document.getElementById('stepRefreshAttempted').innerHTML = statusText(steps.refresh_attempted);
      document.getElementById('stepRefreshSucceeded').innerHTML = statusText(steps.refresh_succeeded);
      document.getElementById('stepProbeAttempted').innerHTML = statusText(steps.market_data_probe_attempted);
      document.getElementById('stepProbeSucceeded').innerHTML = statusText(steps.market_data_probe_succeeded);
      document.getElementById('stepRuntimeReady').innerHTML = statusText(steps.runtime_ready);
      document.getElementById('debugBox').textContent = JSON.stringify(data.debug || {}, null, 2);
    }

    async function generateAuthUrl() {
      const data = await api('/api/auth-url', {
        state: document.getElementById('state').value,
        scope: document.getElementById('scope').value
      });
      currentAuthUrl = data.authorize_url;
      document.getElementById('authUrlBox').textContent = currentAuthUrl || '-';
      document.getElementById('openAuthUrlBtn').disabled = !currentAuthUrl;
    }

    function openAuthUrl() {
      if (currentAuthUrl) window.open(currentAuthUrl, '_blank', 'noopener');
    }

    async function copyFrom(id) {
      const text = document.getElementById(id).textContent || '';
      await navigator.clipboard.writeText(text);
    }

    async function runLocalAuthorize() {
      document.getElementById('responseBox').textContent = 'Waiting for Schwab callback...';
      const data = await api('/api/local-authorize', {
        state: document.getElementById('state').value,
        scope: document.getElementById('scope').value,
        timeout_seconds: Number(document.getElementById('timeoutSeconds').value || 180)
      });
      await refreshStatus();
      return data;
    }

    async function exchangeCode() {
      await api('/api/exchange-code', { code: document.getElementById('authCode').value });
      await refreshStatus();
    }

    async function refreshToken() {
      await api('/api/refresh', {});
      await refreshStatus();
    }

    async function checkRuntimeReady() {
      await api('/api/runtime-ready', {});
      await refreshStatus();
    }

    refreshStatus().catch((error) => {
      document.getElementById('responseBox').textContent = error.message;
    });
  </script>
</body>
</html>
"""
