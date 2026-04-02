"""Deterministic local bootstrap flow for Schwab auth and runtime readiness."""

from __future__ import annotations

import errno
import json
import socket
import webbrowser
from datetime import datetime, timezone
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Callable
from urllib.parse import urlparse

from ..config_models import load_settings_from_files
from ..market_data import (
    QuoteService,
    SchwabAuthError,
    SchwabMarketDataAdapter,
    SchwabOAuthClient,
    SchwabQuoteHttpClient,
    SchwabQuoteRequest,
    SchwabTokenSet,
    SchwabTokenStore,
    UrllibJsonTransport,
    build_auth_metadata,
    load_schwab_auth_config_from_env,
    load_schwab_market_data_config,
    run_loopback_authorization,
)
from ..market_data.schwab_auth import SchwabTokenWriteMismatchError

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


def _callback_listener_spec(callback_url: str) -> dict[str, Any]:
    parsed = urlparse(callback_url)
    scheme = str(parsed.scheme or "").strip().lower()
    hostname = str(parsed.hostname or "").strip()
    port = parsed.port
    path = parsed.path or "/"
    if scheme not in {"http", "https"}:
        raise RuntimeError(f"Callback URL must be HTTP or HTTPS, received {callback_url!r}.")
    if hostname not in {"127.0.0.1", "localhost"}:
        raise RuntimeError(
            f"Callback URL must use localhost or 127.0.0.1, received {callback_url!r}."
        )
    if port is None:
        raise RuntimeError(f"Callback URL must include an explicit port, received {callback_url!r}.")
    return {
        "resolved_callback_url": callback_url,
        "listener_bind_address": hostname,
        "listener_bind_port": port,
        "listener_bind_path": path,
        "listener_bind_url": f"{scheme}://{hostname}:{port}{path}",
    }


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
    placeholder_fields: list[dict[str, str]] = []
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


def _step_state(payload: dict[str, Any]) -> dict[str, bool]:
    return {
        "callback_received": bool(payload.get("callback_received")),
        "code_parsed": bool(payload.get("code_parsed")),
        "exchange_attempted": bool(payload.get("exchange_attempted")),
        "exchange_succeeded": bool(payload.get("exchange_succeeded")),
        "token_written": bool(payload.get("token_written")),
        "refresh_succeeded": bool(payload.get("refresh_succeeds")),
        "market_data_probe_succeeded": bool(payload.get("market_data_probe_succeeds")),
        "runtime_ready": bool(payload.get("runtime_ready")),
    }


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _refresh_failure_artifact_path(token_file_path: Path) -> Path:
    return token_file_path.expanduser().resolve(strict=False).parent / "bootstrap_artifacts" / "latest_refresh_failure.json"


def _exchange_artifact_path(token_file_path: Path) -> Path:
    return token_file_path.expanduser().resolve(strict=False).parent / "bootstrap_artifacts" / "latest_exchange_result.json"


def _persisted_token_artifact_path(token_file_path: Path) -> Path:
    return token_file_path.expanduser().resolve(strict=False).parent / "bootstrap_artifacts" / "latest_persisted_token_payload.json"


def _refresh_result_artifact_path(token_file_path: Path) -> Path:
    return token_file_path.expanduser().resolve(strict=False).parent / "bootstrap_artifacts" / "latest_refresh_result.json"


def _safe_json_load(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


class SchwabTokenBootstrapService:
    """Shared bootstrap flow used by the token web UI and auth gate CLI."""

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
        self._callback = _callback_listener_spec(self._auth_config.callback_url)
        self._probe = _probe_resolution(
            token_file=token_file,
            schwab_config_path=schwab_config_path,
            probe_symbol=self._probe_symbol,
        )
        self._schwab_config_path = Path(self._probe["schwab_config_path"])
        self._latest_attempt: dict[str, Any] = {}

    @property
    def token_file_path(self) -> Path:
        return self._auth_config.token_store_path

    def _artifact_dir(self) -> Path:
        return self.token_file_path.expanduser().resolve(strict=False).parent / "bootstrap_artifacts"

    def last_operation_status(self) -> dict[str, Any]:
        return dict(self._latest_attempt)

    def status(self) -> dict[str, Any]:
        status_path = self._artifact_dir() / "latest_status.json"
        if status_path.exists():
            return json.loads(status_path.read_text(encoding="utf-8"))
        payload = self._base_payload(operation="status")
        return self._finalize_failure(
            payload,
            failed_stage="not_authorized",
            error="No completed Schwab bootstrap attempt is recorded yet.",
            next_fix="Click Authorize with Schwab to run the full local bootstrap flow.",
        )

    def generate_auth_url(self, state: str, scope: str | None = None) -> dict[str, Any]:
        payload = self._base_payload(operation="generate_auth_url")
        payload["auth_url_generated"] = True
        payload["authorize_url"] = self._oauth_client().build_authorize_url(state, scope=scope or None)
        payload["state"] = state
        payload["scope"] = scope or None
        payload["message"] = "Authorization URL generated."
        payload["step_state"] = _step_state(payload)
        self._write_canonical_artifact(payload)
        return payload

    def exchange_code(self, code: str) -> dict[str, Any]:
        payload = self._base_payload(operation="manual_exchange")
        payload["token_write_attempted"] = True
        payload["exchange_attempted"] = True
        payload["code_parsed"] = bool(str(code).strip())
        payload["auth_url_generated"] = True
        payload["post_exchange_validation"] = True
        try:
            self._oauth_client().exchange_code(code)
        except SchwabTokenWriteMismatchError as exc:
            payload["exchange_diagnostic"] = _safe_json_load(_exchange_artifact_path(self.token_file_path))
            return self._finalize_failure(
                payload,
                failed_stage="token_write_mismatch",
                error=str(exc),
                next_fix=self._next_fix("token_write_mismatch"),
            )
        except Exception as exc:
            return self._finalize_failure(
                payload,
                failed_stage="exchange",
                error=str(exc),
                next_fix="Re-run Authorize with Schwab and confirm the callback returns a valid authorization code for the current callback URL.",
            )
        payload["exchange_succeeded"] = True
        payload["token_written"] = self.token_file_path.exists()
        return self._evaluate_runtime_status(
            operation="manual_exchange",
            run_refresh=True,
            run_probe=False,
            force_refresh=True,
            payload=payload,
        )

    def debug_exchange_refresh(self, code: str) -> dict[str, Any]:
        payload = self._base_payload(operation="debug_exchange_refresh")
        payload["token_write_attempted"] = True
        payload["exchange_attempted"] = True
        payload["code_parsed"] = bool(str(code).strip())
        payload["auth_url_generated"] = True
        payload["post_exchange_validation"] = True
        try:
            self._oauth_client().exchange_code(code)
        except SchwabTokenWriteMismatchError as exc:
            payload["exchange_diagnostic"] = _safe_json_load(_exchange_artifact_path(self.token_file_path))
            return self._finalize_failure(
                payload,
                failed_stage="token_write_mismatch",
                error=str(exc),
                next_fix=self._next_fix("token_write_mismatch"),
            )
        except Exception as exc:
            return self._finalize_failure(
                payload,
                failed_stage="exchange",
                error=str(exc),
                next_fix="Fix the authorization-code exchange path before retrying the backend auth debug harness.",
            )
        payload["exchange_succeeded"] = True
        payload["token_written"] = self.token_file_path.exists()
        return self._evaluate_runtime_status(
            operation="debug_exchange_refresh",
            run_refresh=True,
            run_probe=False,
            force_refresh=True,
            payload=payload,
        )

    def local_authorize_proof(
        self,
        *,
        state: str = "mgc-v05l-local",
        scope: str | None = None,
        timeout_seconds: int = 180,
    ) -> dict[str, Any]:
        return self.run_local_authorize(state=state, scope=scope, timeout_seconds=timeout_seconds)

    def refresh_token(self) -> dict[str, Any]:
        return self._evaluate_runtime_status(
            operation="refresh_token",
            run_refresh=True,
            run_probe=False,
            force_refresh=True,
        )

    def check_runtime_ready(self) -> dict[str, Any]:
        return self._evaluate_runtime_status(
            operation="runtime_ready_check",
            run_refresh=True,
            run_probe=True,
            force_refresh=True,
        )

    def run_local_authorize(
        self,
        *,
        state: str,
        scope: str | None,
        timeout_seconds: int,
    ) -> dict[str, Any]:
        payload = self._base_payload(operation="local_authorize")
        try:
            self._preflight_callback_listener()
        except Exception as exc:
            return self._finalize_failure(
                payload,
                failed_stage="callback_listener_bind",
                error=str(exc),
                next_fix=self._next_fix("callback_listener_bind"),
            )

        payload["authorize_url"] = self._oauth_client().build_authorize_url(state, scope=scope or None)
        payload["auth_url_generated"] = True
        payload["browser_launch_attempted"] = True
        payload["state"] = state
        payload["scope"] = scope or None

        def _event_callback(event: dict[str, Any]) -> None:
            stage = str(event.get("stage") or "")
            payload["callback_stage"] = stage
            payload["callback_received"] = bool(payload.get("callback_received")) or bool(event.get("callback_received"))
            payload["code_parsed"] = bool(payload.get("code_parsed")) or bool(event.get("auth_code_parsed"))
            payload["exchange_attempted"] = bool(payload.get("exchange_attempted")) or bool(event.get("exchange_attempted"))
            payload["exchange_succeeded"] = bool(payload.get("exchange_succeeded")) or bool(event.get("exchange_success"))
            payload["token_write_attempted"] = bool(payload.get("token_write_attempted")) or bool(event.get("token_write_attempted"))
            payload["token_written"] = bool(payload.get("token_written")) or bool(event.get("token_write_success"))
            if event.get("error"):
                payload["callback_error"] = str(event["error"])

        try:
            result = run_loopback_authorization(
                oauth_client=self._oauth_client(),
                state=state,
                scope=scope or None,
                timeout_seconds=timeout_seconds,
                open_browser=True,
                browser_opener=self._browser_opener,
                event_callback=_event_callback,
            )
        except SchwabTokenWriteMismatchError as exc:
            payload["exchange_diagnostic"] = _safe_json_load(_exchange_artifact_path(self.token_file_path))
            return self._finalize_failure(
                payload,
                failed_stage="token_write_mismatch",
                error=str(exc),
                next_fix=self._next_fix("token_write_mismatch"),
            )
        except Exception as exc:
            failed_stage = self._local_authorize_failure_stage(payload, str(exc))
            return self._finalize_failure(
                payload,
                failed_stage=failed_stage,
                error=str(exc),
                next_fix=self._next_fix(failed_stage),
            )

        payload["browser_opened"] = bool(getattr(result, "browser_opened", True))
        payload["authorize_url"] = str(getattr(result, "authorize_url", payload.get("authorize_url") or ""))
        payload["resolved_callback_url"] = str(
            getattr(result, "callback_url", payload.get("resolved_callback_url") or self._callback["resolved_callback_url"])
        )
        payload["token_write_path"] = str(
            getattr(result, "token_file", payload.get("token_write_path") or self.token_file_path)
        )
        payload["post_exchange_validation"] = True
        payload["callback_received"] = True
        payload["code_parsed"] = True
        payload["exchange_attempted"] = True
        payload["exchange_succeeded"] = True
        payload["token_write_attempted"] = True
        payload["token_written"] = self.token_file_path.exists()

        return self._evaluate_runtime_status(
            operation="local_authorize",
            run_refresh=True,
            run_probe=False,
            force_refresh=True,
            payload=payload,
        )

    def _oauth_client(self) -> SchwabOAuthClient:
        return SchwabOAuthClient(
            config=self._auth_config,
            transport=self._transport_factory(),
            token_store=SchwabTokenStore(self.token_file_path),
        )

    def _preflight_callback_listener(self) -> None:
        host = str(self._callback["listener_bind_address"])
        port = int(self._callback["listener_bind_port"])
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((host, port))
        except OSError as exc:
            if exc.errno == errno.EADDRINUSE:
                raise RuntimeError(
                    f"Callback listener could not bind to {host}:{port}. "
                    "Another process is using the port. Stop the existing bootstrap/dashboard listener or auto-select a new port and regenerate the callback URL."
                ) from exc
            raise RuntimeError(f"Callback listener could not bind to {host}:{port}: {exc}") from exc
        finally:
            sock.close()

    def _evaluate_runtime_status(
        self,
        *,
        operation: str,
        run_refresh: bool,
        run_probe: bool,
        force_refresh: bool,
        payload: dict[str, Any] | None = None,
    ) -> dict[str, Any]:
        payload = dict(payload or self._base_payload(operation=operation))
        token_store = SchwabTokenStore(self.token_file_path)
        token_set = token_store.load()
        token_metadata = token_store.load_metadata() if self.token_file_path.exists() else None

        payload["stored_token_identity"] = token_metadata
        payload["exchange_diagnostic_path"] = str(_exchange_artifact_path(self.token_file_path))
        payload["exchange_diagnostic"] = _safe_json_load(_exchange_artifact_path(self.token_file_path))
        payload["persisted_token_artifact_path"] = str(_persisted_token_artifact_path(self.token_file_path))
        payload["persisted_token_artifact"] = _safe_json_load(_persisted_token_artifact_path(self.token_file_path))
        payload["refresh_result_artifact_path"] = str(_refresh_result_artifact_path(self.token_file_path))
        payload["refresh_result_artifact"] = _safe_json_load(_refresh_result_artifact_path(self.token_file_path))
        payload["token_exists"] = token_set is not None if token_set is not None else self.token_file_path.exists()
        payload["has_refresh_token"] = bool(token_set and token_set.refresh_token)
        payload["token_type"] = token_set.token_type if token_set is not None else None
        payload["token_scope"] = token_set.scope if token_set is not None else None
        payload["access_token_expires_at"] = (
            token_set.expires_at.astimezone(timezone.utc).isoformat()
            if token_set is not None and token_set.expires_at is not None
            else None
        )
        payload["stored_token_fields"] = self._stored_token_fields()
        payload["refresh_token_length"] = len(token_set.refresh_token or "") if token_set is not None and token_set.refresh_token else 0
        payload["refresh_token_head"] = (token_set.refresh_token or "")[:4] if token_set is not None else ""
        payload["refresh_token_tail"] = (token_set.refresh_token or "")[-4:] if token_set is not None and token_set.refresh_token else ""
        payload["token_expired"] = token_set.is_expired(datetime.now(timezone.utc)) if token_set is not None else None
        payload["token_written"] = bool(payload.get("token_written")) or payload["token_exists"]
        payload["token_write_attempted"] = bool(payload.get("token_write_attempted")) or payload["token_written"]

        if token_metadata is not None:
            payload["token_client_match"] = self._token_client_match(token_metadata)
            if payload["token_client_match"] is False:
                return self._finalize_failure(
                    payload,
                    failed_stage="token_identity_mismatch",
                    error="New token was written but does not match current Schwab app identity.",
                    next_fix=self._next_fix("token_identity_mismatch"),
                )

        if payload["token_written"] and not payload["has_refresh_token"]:
            return self._finalize_failure(
                payload,
                failed_stage="token_written_but_invalid",
                error="Authorization succeeded but Schwab did not issue a usable refresh token for this app/session.",
                next_fix=self._next_fix("token_written_but_invalid"),
            )

        exchange_diagnostic = payload.get("exchange_diagnostic")
        if isinstance(exchange_diagnostic, dict) and exchange_diagnostic.get("persisted_refresh_token_matches_exchange") is False:
            return self._finalize_failure(
                payload,
                failed_stage="token_write_mismatch",
                error="Persisted refresh token does not match the exchange response refresh token.",
                next_fix=self._next_fix("token_write_mismatch"),
            )

        if run_refresh:
            payload["refresh_attempted"] = True
            refresh_succeeds, refresh_error, token_set = self._evaluate_refresh(token_set=token_set, force_refresh=force_refresh)
            payload["refresh_succeeds"] = refresh_succeeds
            payload["refresh_error"] = refresh_error
            payload["token_exists"] = token_set is not None if token_set is not None else payload["token_exists"]
            payload["persisted_token_artifact"] = _safe_json_load(_persisted_token_artifact_path(self.token_file_path))
            payload["refresh_result_artifact"] = _safe_json_load(_refresh_result_artifact_path(self.token_file_path))
            if not refresh_succeeds:
                payload["refresh_failure_diagnostic_path"] = str(_refresh_failure_artifact_path(self.token_file_path))
                payload["refresh_failure_diagnostic"] = _safe_json_load(_refresh_failure_artifact_path(self.token_file_path))
                payload["refresh_result_artifact"] = _safe_json_load(_refresh_result_artifact_path(self.token_file_path))
                return self._finalize_failure(
                    payload,
                    failed_stage="refresh",
                    error=str(refresh_error or "Refresh failed."),
                    next_fix=self._next_fix("refresh"),
                    final_state_override=(
                        "REFRESH_FAILED_IMMEDIATELY_AFTER_EXCHANGE"
                        if payload.get("post_exchange_validation")
                        else None
                    ),
                )

        if run_probe:
            payload["market_data_probe_attempted"] = True
            probe_succeeds, probe_error, probe_result = self._evaluate_market_data_probe()
            payload["market_data_probe_succeeds"] = probe_succeeds
            payload["market_data_probe_error"] = probe_error
            payload["market_data_probe_result"] = probe_result
            if not probe_succeeds:
                return self._finalize_failure(
                    payload,
                    failed_stage="market_data_probe",
                    error=str(probe_error or "Market-data probe failed."),
                    next_fix=self._next_fix("market_data_probe"),
                )

        if not payload["token_exists"]:
            return self._finalize_failure(
                payload,
                failed_stage="token_missing",
                error="No token file found.",
                next_fix=self._next_fix("token_missing"),
            )

        payload["runtime_ready"] = True
        if run_probe:
            payload["message"] = "RUNTIME_READY: callback received, code parsed, token exchanged, token written, refresh succeeded, probe succeeded, runtime ready."
        else:
            payload["message"] = "RUNTIME_READY: callback received, code parsed, token exchanged, token written, immediate refresh succeeded."
        return self._finalize_success(payload)

    def _evaluate_refresh(
        self,
        *,
        token_set: SchwabTokenSet | None,
        force_refresh: bool,
    ) -> tuple[bool, str | None, SchwabTokenSet | None]:
        if token_set is None:
            return False, "No token file found.", None
        if not token_set.refresh_token:
            return False, "No refresh token is available in the local token store.", token_set
        try:
            refreshed = self._oauth_client().refresh_token(token_set.refresh_token)
        except Exception as exc:
            return False, str(exc), SchwabTokenStore(self.token_file_path).load()
        if force_refresh:
            token_set = refreshed
        else:
            token_set = SchwabTokenStore(self.token_file_path).load() or refreshed
        return True, None, token_set

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
            "configured_quote_symbol": self._probe["quote_symbol"],
            "configured_historical_symbol": self._probe["historical_symbol"],
            "quote_count": len(quotes),
            "returned_symbol": first_quote.raw_payload.get("symbol"),
            "reference_product": (first_quote.reference_future or {}).get("product"),
        }

    def _base_payload(self, *, operation: str) -> dict[str, Any]:
        token_path = self.token_file_path.expanduser().resolve(strict=False)
        current_client_identity = build_auth_metadata(self._auth_config)
        payload = {
            "generated_at": _now_iso(),
            "operation": operation,
            "resolved_callback_url": self._callback["resolved_callback_url"],
            "listener_bind_address": self._callback["listener_bind_address"],
            "listener_bind_port": self._callback["listener_bind_port"],
            "listener_bind_path": self._callback["listener_bind_path"],
            "listener_bind_url": self._callback["listener_bind_url"],
            "schwab_config_path": str(self._schwab_config_path),
            "probe_symbol": self._probe_symbol,
            "probe_resolution": dict(self._probe),
            "token_file": str(token_path),
            "token_write_path": str(token_path),
            "post_exchange_validation": False,
            "exchange_diagnostic_path": str(_exchange_artifact_path(token_path)),
            "exchange_diagnostic": None,
            "persisted_token_artifact_path": str(_persisted_token_artifact_path(token_path)),
            "persisted_token_artifact": None,
            "refresh_result_artifact_path": str(_refresh_result_artifact_path(token_path)),
            "refresh_result_artifact": None,
            "current_client_identity": current_client_identity,
            "stored_token_identity": None,
            "token_client_match": None,
            "token_exists": token_path.exists(),
            "token_written": token_path.exists(),
            "has_refresh_token": False,
            "token_scope": None,
            "token_type": None,
            "stored_token_fields": None,
            "refresh_token_length": 0,
            "refresh_token_head": "",
            "refresh_token_tail": "",
            "access_token_expires_at": None,
            "token_expired": None,
            "authorize_url": None,
            "auth_url_generated": False,
            "browser_launch_attempted": False,
            "browser_opened": False,
            "callback_received": False,
            "code_parsed": False,
            "exchange_attempted": False,
            "exchange_succeeded": False,
            "token_write_attempted": False,
            "refresh_attempted": False,
            "refresh_succeeds": False,
            "refresh_error": None,
            "market_data_probe_attempted": False,
            "market_data_probe_succeeds": False,
            "market_data_probe_error": None,
            "market_data_probe_result": None,
            "refresh_failure_diagnostic_path": None,
            "refresh_failure_diagnostic": None,
            "runtime_ready": False,
            "final_state": None,
            "failed_stage": None,
            "error": None,
            "next_fix": None,
            "message": None,
        }
        payload["step_state"] = _step_state(payload)
        return payload

    def _finalize_success(self, payload: dict[str, Any]) -> dict[str, Any]:
        payload["final_state"] = "RUNTIME_READY"
        payload["failed_stage"] = None
        payload["error"] = None
        payload["next_fix"] = None
        payload["runtime_ready"] = True
        payload["step_state"] = _step_state(payload)
        self._write_canonical_artifact(payload)
        return payload

    def _finalize_failure(
        self,
        payload: dict[str, Any],
        *,
        failed_stage: str,
        error: str,
        next_fix: str,
        final_state_override: str | None = None,
    ) -> dict[str, Any]:
        payload["final_state"] = final_state_override or self._final_verdict(failed_stage)
        payload["failed_stage"] = failed_stage
        payload["error"] = error
        payload["next_fix"] = next_fix
        payload["runtime_ready"] = False
        payload["message"] = f"{payload['final_state']}: stage={failed_stage} error={error}"
        payload["step_state"] = _step_state(payload)
        self._write_canonical_artifact(payload)
        return payload

    def _write_canonical_artifact(self, payload: dict[str, Any]) -> None:
        payload["generated_at"] = _now_iso()
        payload["step_state"] = _step_state(payload)
        artifact_dir = self._artifact_dir()
        artifact_dir.mkdir(parents=True, exist_ok=True)
        (artifact_dir / "latest_status.json").write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        self._latest_attempt = dict(payload)
        with (artifact_dir / "events.jsonl").open("a", encoding="utf-8") as handle:
            handle.write(json.dumps({"timestamp": _now_iso(), "operation": payload.get("operation"), "final_state": payload.get("final_state"), "failed_stage": payload.get("failed_stage"), "runtime_ready": payload.get("runtime_ready")}, sort_keys=True))
            handle.write("\n")

    def _token_client_match(self, stored_token_identity: dict[str, Any]) -> bool:
        current = build_auth_metadata(self._auth_config)
        return (
            stored_token_identity.get("client_key_fingerprint") == current["client_key_fingerprint"]
            and stored_token_identity.get("client_secret_fingerprint") == current["client_secret_fingerprint"]
            and stored_token_identity.get("callback_url") == current["callback_url"]
        )

    def _local_authorize_failure_stage(self, payload: dict[str, Any], error: str) -> str:
        lowered = error.lower()
        if "another process is using the port" in lowered or "could not bind callback listener" in lowered:
            return "callback_listener_bind"
        if "timed out waiting for schwab callback" in lowered:
            return "callback_timeout"
        if payload.get("exchange_attempted") and not payload.get("exchange_succeeded"):
            return "exchange"
        if payload.get("callback_received") and not payload.get("code_parsed"):
            return "code_parse"
        return "callback"

    def _final_verdict(self, failed_stage: str) -> str:
        if failed_stage in {"callback_listener_bind", "callback_timeout", "callback", "code_parse", "exchange", "not_authorized"}:
            return "EXCHANGE_FAILED"
        if failed_stage == "token_write_mismatch":
            return "TOKEN_WRITE_MISMATCH"
        if failed_stage in {"token_missing", "token_written_but_invalid"}:
            return "TOKEN_WRITTEN_BUT_INVALID"
        if failed_stage == "token_identity_mismatch":
            return "TOKEN_IDENTITY_MISMATCH"
        if failed_stage == "refresh":
            return "REFRESH_FAILED_WITH_PROVIDER_ERROR"
        if failed_stage == "refresh_immediate_after_exchange":
            return "REFRESH_FAILED_IMMEDIATELY_AFTER_EXCHANGE"
        if failed_stage == "market_data_probe":
            return "PROBE_FAILED"
        return "EXCHANGE_FAILED"

    def _stored_token_fields(self) -> dict[str, bool]:
        payload = _safe_json_load(self.token_file_path)
        if payload is None:
            return {
                "has_access_token": False,
                "has_refresh_token": False,
                "has_token_type": False,
                "has_scope": False,
                "has_issued_at": False,
                "has_expires_in": False,
            }
        return {
            "has_access_token": bool(payload.get("access_token")),
            "has_refresh_token": bool(payload.get("refresh_token")),
            "has_token_type": bool(payload.get("token_type")),
            "has_scope": bool(payload.get("scope")),
            "has_issued_at": bool(payload.get("issued_at")),
            "has_expires_in": payload.get("expires_in") is not None,
        }

    def _next_fix(self, failed_stage: str) -> str:
        host = self._callback["listener_bind_address"]
        port = self._callback["listener_bind_port"]
        if failed_stage == "callback_listener_bind":
            return (
                f"Stop the process currently using {host}:{port}, or choose a different callback port and regenerate the callback URL before authorizing again."
            )
        if failed_stage == "callback_timeout":
            return "Re-run Authorize with Schwab and complete the approval flow in the browser before the local callback listener times out."
        if failed_stage == "code_parse":
            return "Re-run Authorize with Schwab and make sure the callback returns a valid authorization code on the configured callback URL."
        if failed_stage == "exchange":
            return "Re-run Authorize with Schwab and complete a fresh approval flow so a new authorization code can be exchanged."
        if failed_stage == "token_written_but_invalid":
            return "Inspect the written token summary and confirm Schwab issued a refresh-capable token for the current app/session before retrying authorization."
        if failed_stage == "token_write_mismatch":
            return (
                f"Inspect the exchange diagnostic artifact at {_exchange_artifact_path(self.token_file_path)}. "
                "Fix the token persistence path before attempting any refresh validation."
            )
        if failed_stage == "token_identity_mismatch":
            return "Delete the mismatched token file and rerun Authorize with Schwab using the current app key, secret, and callback URL."
        if failed_stage == "refresh":
            return (
                f"Inspect the refresh diagnostic artifact at {_refresh_failure_artifact_path(self.token_file_path)}. "
                "If the token shape and app identity are correct, fix the refresh request contract or Schwab app registration before retrying authorization."
            )
        if failed_stage == "market_data_probe":
            return "Fix the Schwab market-data access or probe symbol mapping, then rerun Authorize with Schwab to verify the new token end-to-end."
        if failed_stage == "token_missing":
            return "Run Authorize with Schwab so the local token file is created before checking runtime readiness again."
        return "Inspect the exact error text in the bootstrap result and rerun Authorize with Schwab after correcting that root cause."


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
        payload = self._read_json_body()
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
        if self.path == "/api/status":
            self._write_json(200, self.server.service.status())
            return
        if self.path == "/api/runtime-ready":
            self._write_json(200, self.server.service.check_runtime_ready())
            return
        if self.path == "/api/refresh":
            self._write_json(200, self.server.service.refresh_token())
            return
        if self.path == "/api/auth-url":
            self._write_json(
                200,
                self.server.service.generate_auth_url(
                    state=str(payload.get("state") or "mgc-v05l-local"),
                    scope=str(payload.get("scope")) if payload.get("scope") not in (None, "") else None,
                ),
            )
            return
        if self.path == "/api/exchange-code":
            self._write_json(200, self.server.service.exchange_code(str(payload.get("code") or "")))
            return
        self._write_json(404, {"error": "Not found"})

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
) -> tuple[_BootstrapServer, int]:
    last_error: OSError | None = None
    for offset in range(max(port_search_limit, 0) + 1):
        candidate_port = start_port + offset
        try:
            return _BootstrapServer((host, candidate_port), service), candidate_port
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
                "written_at": _now_iso(),
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
  <title>Schwab Bootstrap</title>
  <style>
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; margin: 32px; background: #f6f7fb; color: #162033; }
    main { max-width: 920px; margin: 0 auto; }
    section { background: #fff; border-radius: 14px; padding: 20px 22px; margin-bottom: 18px; box-shadow: 0 10px 30px rgba(20,30,60,0.08); }
    button { padding: 11px 16px; border: 0; border-radius: 9px; cursor: pointer; }
    button.primary { background: #1743b3; color: #fff; font-weight: 700; }
    button.secondary { background: #5f6d8d; color: #fff; }
    button:disabled { opacity: 0.55; cursor: not-allowed; }
    pre { background: #eef2fb; border-radius: 10px; padding: 12px; white-space: pre-wrap; }
    .status-ok { color: #0d7a3a; font-weight: 700; }
    .status-bad { color: #a12f2f; font-weight: 700; }
    .row { display: flex; gap: 10px; align-items: center; flex-wrap: wrap; }
    details { margin-top: 16px; }
  </style>
</head>
<body>
  <main>
    <section>
      <h1>Authorize with Schwab</h1>
      <p>This flow now runs callback capture, code exchange, token write, refresh, market-data probe, and runtime verdict as one blocking workflow.</p>
      <div class="row">
        <button id="authorizeBtn" class="primary" onclick="authorizeWithSchwab()">Authorize with Schwab</button>
        <button id="refreshBtn" class="secondary" onclick="refreshStatus()">Refresh Latest Result</button>
      </div>
      <p id="headline">Loading latest result...</p>
      <pre id="resultBox">-</pre>
      <details>
        <summary>Advanced tools</summary>
        <div class="row" style="margin-top: 12px;">
          <button id="runtimeBtn" class="secondary" onclick="checkRuntimeReady()">Check Runtime Ready</button>
          <button id="refreshTokenBtn" class="secondary" onclick="refreshToken()">Refresh Token</button>
          <button id="authUrlBtn" class="secondary" onclick="generateAuthUrl()">Generate Auth URL</button>
        </div>
        <input id="authCode" placeholder="Optional manual authorization code">
        <button id="exchangeBtn" class="secondary" onclick="exchangeCode()">Exchange Code</button>
      </details>
    </section>
  </main>
  <script>
    const buttons = ["authorizeBtn", "refreshBtn", "runtimeBtn", "refreshTokenBtn", "authUrlBtn", "exchangeBtn"];
    function setBusy(busy) {
      for (const id of buttons) {
        const el = document.getElementById(id);
        if (el) el.disabled = busy;
      }
      document.getElementById("headline").textContent = busy ? "Authorizing with Schwab..." : document.getElementById("headline").textContent;
    }
    function render(payload) {
      const finalState = payload.final_state || (payload.runtime_ready ? "RUNTIME_READY" : "EXCHANGE_FAILED");
      const headline = finalState === "RUNTIME_READY"
        ? "RUNTIME_READY: token verified end-to-end."
        : `FAILURE: ${payload.failed_stage || "unknown"}${payload.error ? " — " + payload.error : ""}`;
      document.getElementById("headline").innerHTML =
        finalState === "RUNTIME_READY"
          ? `<span class="status-ok">${headline}</span>`
          : `<span class="status-bad">${headline}</span>`;
      document.getElementById("resultBox").textContent = JSON.stringify(payload, null, 2);
    }
    async function call(path, payload) {
      const response = await fetch(path, {
        method: payload ? "POST" : "GET",
        headers: payload ? {"Content-Type": "application/json"} : {},
        body: payload ? JSON.stringify(payload) : undefined,
      });
      const data = await response.json();
      render(data);
      return data;
    }
    async function authorizeWithSchwab() {
      setBusy(true);
      try {
        await call("/api/local-authorize", {state: "mgc-v05l-local", timeout_seconds: 180});
      } finally {
        setBusy(false);
      }
    }
    async function refreshStatus() { await call("/api/status"); }
    async function checkRuntimeReady() { await call("/api/runtime-ready", {}); }
    async function refreshToken() { await call("/api/refresh", {}); }
    async function generateAuthUrl() { await call("/api/auth-url", {state: "mgc-v05l-local"}); }
    async function exchangeCode() { await call("/api/exchange-code", {code: document.getElementById("authCode").value}); }
    refreshStatus();
  </script>
</body>
</html>
"""
