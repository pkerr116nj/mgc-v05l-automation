"""Schwab OAuth helpers and local token persistence."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlencode

from .schwab_models import HttpRequest, JsonHttpTransport, SchwabAuthConfig, SchwabTokenSet


class SchwabAuthError(RuntimeError):
    """Raised when Schwab auth config or token state is invalid."""


class SchwabTokenWriteMismatchError(SchwabAuthError):
    """Raised when the persisted refresh token does not match the exchange response."""


class SchwabTokenStore:
    """Small JSON token store for local development use."""

    def __init__(self, path: Path) -> None:
        self._path = path

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> Optional[SchwabTokenSet]:
        if not self._path.exists():
            return None
        payload = self.load_payload()
        return SchwabTokenSet.from_json_dict(payload)

    def load_payload(self) -> dict:
        if not self._path.exists():
            raise FileNotFoundError(self._path)
        return json.loads(self._path.read_text(encoding="utf-8"))

    def load_metadata(self) -> Optional[dict]:
        if not self._path.exists():
            return None
        payload = self.load_payload()
        metadata = payload.get("_meta")
        return metadata if isinstance(metadata, dict) else None

    def save(self, token_set: SchwabTokenSet, auth_metadata: dict | None = None) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        payload = token_set.to_json_dict()
        if auth_metadata:
            payload["_meta"] = auth_metadata
        self._path.write_text(
            json.dumps(payload, indent=2, sort_keys=True),
            encoding="utf-8",
        )
        artifact_path = self._path.parent / "bootstrap_artifacts" / "latest_persisted_token_payload.json"
        artifact_path.parent.mkdir(parents=True, exist_ok=True)
        artifact_path.write_text(
            json.dumps(
                {
                    "generated_at": _now_iso(),
                    "token_file_path": str(self._path.expanduser().resolve(strict=False)),
                    "persisted_token_summary": _summarize_token_mapping(payload),
                    "meta": payload.get("_meta"),
                },
                indent=2,
                sort_keys=True,
            ),
            encoding="utf-8",
        )

    def summary(self) -> dict[str, object]:
        payload = self.load_payload()
        return _summarize_token_mapping(payload)


@dataclass
class SchwabOAuthClient:
    """Authorization-code OAuth client backed by an injectable JSON transport."""

    config: SchwabAuthConfig
    transport: JsonHttpTransport
    token_store: SchwabTokenStore

    def build_authorize_url(self, state: str, scope: Optional[str] = None) -> str:
        query = {
            "client_id": self.config.app_key,
            "redirect_uri": self.config.callback_url,
            "response_type": "code",
            "state": state,
        }
        if scope:
            query["scope"] = scope
        return f"{self.config.authorize_url}?{urlencode(query)}"

    def exchange_code(self, code: str) -> SchwabTokenSet:
        normalized_code = _normalize_authorization_code(code)
        request = HttpRequest(
            method="POST",
            url=self.config.token_url,
            headers=self._token_headers(),
            form={
                "grant_type": "authorization_code",
                "code": normalized_code,
                "redirect_uri": self.config.callback_url,
            },
        )
        payload = self.transport.request_json(request)
        token_set = SchwabTokenSet.from_token_response(payload)
        self.token_store.save(token_set, auth_metadata=build_auth_metadata(self.config))
        persisted_payload = self.token_store.load_payload()
        diagnostic = self._write_exchange_diagnostic(
            request=request,
            exchange_payload=payload,
            persisted_payload=persisted_payload,
        )
        if diagnostic["persisted_refresh_token_matches_exchange"] is False:
            raise SchwabTokenWriteMismatchError(
                "Persisted refresh token does not match the exchange response refresh token."
            )
        return token_set

    def refresh_token(self, refresh_token: Optional[str] = None) -> SchwabTokenSet:
        token_to_refresh = refresh_token
        stored = self.token_store.load()
        stored_payload = self.token_store.load_payload() if self.token_store.path.exists() else None
        if token_to_refresh is None:
            if stored is None or not stored.refresh_token:
                self._write_refresh_failure_diagnostic(
                    refresh_token=None,
                    stored_token_set=stored,
                    stored_payload=stored_payload,
                    error=SchwabAuthError("No refresh token is available in the local token store."),
                )
                raise SchwabAuthError("No refresh token is available in the local token store.")
            token_to_refresh = stored.refresh_token

        request = HttpRequest(
            method="POST",
            url=self.config.token_url,
            headers=self._token_headers(),
            form={
                "grant_type": "refresh_token",
                "refresh_token": token_to_refresh,
            },
        )
        try:
            payload = self.transport.request_json(request)
        except Exception as exc:
            self._write_refresh_failure_diagnostic(
                refresh_token=token_to_refresh,
                stored_token_set=stored,
                stored_payload=stored_payload,
                error=exc,
                request=request,
            )
            self._write_refresh_result_artifact(
                refresh_token=token_to_refresh,
                stored_token_set=stored,
                stored_payload=stored_payload,
                request=request,
                provider_error=exc,
                refresh_payload=None,
                refresh_succeeds=False,
            )
            raise
        token_set = SchwabTokenSet.from_token_response(payload)
        if token_set.refresh_token is None:
            token_set = SchwabTokenSet(
                access_token=token_set.access_token,
                refresh_token=token_to_refresh,
                token_type=token_set.token_type,
                expires_in=token_set.expires_in,
                scope=token_set.scope,
                issued_at=token_set.issued_at,
            )
        self.token_store.save(token_set, auth_metadata=build_auth_metadata(self.config))
        self._clear_refresh_failure_diagnostic()
        self._write_refresh_result_artifact(
            refresh_token=token_to_refresh,
            stored_token_set=stored,
            stored_payload=stored_payload,
            request=request,
            provider_error=None,
            refresh_payload=payload,
            refresh_succeeds=True,
        )
        return token_set

    def get_access_token(self) -> str:
        token_set = self.token_store.load()
        if token_set is None:
            raise SchwabAuthError(
                f"No token file found at {self.token_store.path}. Run the auth-code exchange first."
            )
        if token_set.is_expired() and token_set.refresh_token:
            token_set = self.refresh_token(token_set.refresh_token)
        elif token_set.is_expired():
            raise SchwabAuthError("Stored access token is expired and no refresh token is available.")
        return token_set.access_token

    def _token_headers(self) -> dict[str, str]:
        credentials = f"{self.config.app_key}:{self.config.app_secret}".encode("utf-8")
        basic = base64.b64encode(credentials).decode("ascii")
        return {
            "Accept": "application/json",
            "Authorization": f"Basic {basic}",
            "Content-Type": "application/x-www-form-urlencoded",
        }

    def _refresh_failure_artifact_path(self) -> Path:
        return self.token_store.path.parent / "bootstrap_artifacts" / "latest_refresh_failure.json"

    def _exchange_artifact_path(self) -> Path:
        return self.token_store.path.parent / "bootstrap_artifacts" / "latest_exchange_result.json"

    def _refresh_result_artifact_path(self) -> Path:
        return self.token_store.path.parent / "bootstrap_artifacts" / "latest_refresh_result.json"

    def _clear_refresh_failure_diagnostic(self) -> None:
        path = self._refresh_failure_artifact_path()
        if path.exists():
            path.unlink()

    def _write_exchange_diagnostic(
        self,
        *,
        request: HttpRequest,
        exchange_payload: dict[str, object],
        persisted_payload: dict[str, object],
    ) -> dict[str, object]:
        path = self._exchange_artifact_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        exchange_refresh = _token_string(exchange_payload, "refresh_token")
        persisted_refresh = _token_string(persisted_payload, "refresh_token")
        diagnostic = {
            "generated_at": _now_iso(),
            "token_endpoint_url": self.config.token_url,
            "callback_url": self.config.callback_url,
            "app_key_fingerprint": _fingerprint_value(self.config.app_key),
            "token_file_path": str(self.token_store.path.expanduser().resolve(strict=False)),
            "request_url": request.url,
            "request_form_keys": sorted((request.form or {}).keys()),
            "exchange_response_summary": _summarize_token_mapping(exchange_payload),
            "persisted_token_summary": _summarize_token_mapping(persisted_payload),
            "persisted_refresh_token_matches_exchange": exchange_refresh == persisted_refresh,
            "persisted_refresh_matches_exchange_access_token": (
                persisted_refresh is not None and persisted_refresh == _token_string(exchange_payload, "access_token")
            ),
            "persisted_refresh_matches_exchange_id_token": (
                persisted_refresh is not None and persisted_refresh == _token_string(exchange_payload, "id_token")
            ),
        }
        path.write_text(json.dumps(diagnostic, indent=2, sort_keys=True), encoding="utf-8")
        return diagnostic

    def _write_refresh_failure_diagnostic(
        self,
        *,
        refresh_token: str | None,
        stored_token_set: SchwabTokenSet | None,
        stored_payload: dict | None,
        error: Exception,
        request: HttpRequest | None = None,
    ) -> None:
        path = self._refresh_failure_artifact_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        has_payload = isinstance(stored_payload, dict)
        response_status = getattr(error, "status_code", None)
        response_body = getattr(error, "response_body", None)
        exchange_artifact = _safe_json_dict(self._exchange_artifact_path())
        exchange_summary = (
            exchange_artifact.get("exchange_response_summary")
            if isinstance(exchange_artifact, dict) and isinstance(exchange_artifact.get("exchange_response_summary"), dict)
            else None
        )
        persisted_summary = (
            exchange_artifact.get("persisted_token_summary")
            if isinstance(exchange_artifact, dict) and isinstance(exchange_artifact.get("persisted_token_summary"), dict)
            else None
        )
        exchange_refresh_preview = (
            exchange_summary.get("refresh_token")
            if isinstance(exchange_summary, dict) and isinstance(exchange_summary.get("refresh_token"), dict)
            else None
        )
        persisted_refresh_preview = (
            persisted_summary.get("refresh_token")
            if isinstance(persisted_summary, dict) and isinstance(persisted_summary.get("refresh_token"), dict)
            else None
        )
        request_refresh_preview = _token_preview(refresh_token)
        diagnostic = {
            "generated_at": _now_iso(),
            "token_endpoint_url": self.config.token_url,
            "callback_url": self.config.callback_url,
            "app_key_fingerprint": _fingerprint_value(self.config.app_key),
            "token_file_path": str(self.token_store.path.expanduser().resolve(strict=False)),
            "request_url": request.url if request is not None else self.config.token_url,
            "request_form_keys": sorted((request.form or {}).keys()) if request is not None else [],
            "stored_token_fields": {
                "has_access_token": bool(has_payload and stored_payload.get("access_token")),
                "has_refresh_token": bool(has_payload and stored_payload.get("refresh_token")),
                "has_token_type": bool(has_payload and stored_payload.get("token_type")),
                "has_scope": bool(has_payload and stored_payload.get("scope")),
                "has_issued_at": bool(has_payload and stored_payload.get("issued_at")),
                "has_expires_in": stored_payload.get("expires_in") is not None if has_payload else False,
            },
            "stored_token_type": stored_token_set.token_type if stored_token_set is not None else None,
            "stored_scope": stored_token_set.scope if stored_token_set is not None else None,
            "stored_issued_at": (
                stored_token_set.issued_at.isoformat() if stored_token_set is not None else None
            ),
            "stored_expires_in": stored_token_set.expires_in if stored_token_set is not None else None,
            "refresh_token_length": len(refresh_token or ""),
            "refresh_token_head": (refresh_token or "")[:4],
            "refresh_token_tail": (refresh_token or "")[-4:] if refresh_token else "",
            "exchange_refresh_token_preview": exchange_refresh_preview,
            "persisted_refresh_token_preview": persisted_refresh_preview,
            "request_refresh_token_preview": request_refresh_preview,
            "request_refresh_matches_exchange": _token_preview_matches(request_refresh_preview, exchange_refresh_preview),
            "request_refresh_matches_persisted": _token_preview_matches(request_refresh_preview, persisted_refresh_preview),
            "provider_status_code": response_status,
            "provider_response_body": response_body,
            "error_text": str(error),
        }
        path.write_text(json.dumps(diagnostic, indent=2, sort_keys=True), encoding="utf-8")

    def _write_refresh_result_artifact(
        self,
        *,
        refresh_token: str | None,
        stored_token_set: SchwabTokenSet | None,
        stored_payload: dict | None,
        request: HttpRequest,
        provider_error: Exception | None,
        refresh_payload: dict[str, object] | None,
        refresh_succeeds: bool,
    ) -> None:
        path = self._refresh_result_artifact_path()
        path.parent.mkdir(parents=True, exist_ok=True)
        exchange_artifact = _safe_json_dict(self._exchange_artifact_path())
        exchange_summary = (
            exchange_artifact.get("exchange_response_summary")
            if isinstance(exchange_artifact, dict) and isinstance(exchange_artifact.get("exchange_response_summary"), dict)
            else None
        )
        persisted_summary = (
            exchange_artifact.get("persisted_token_summary")
            if isinstance(exchange_artifact, dict) and isinstance(exchange_artifact.get("persisted_token_summary"), dict)
            else _summarize_token_mapping(stored_payload or {})
        )
        exchange_refresh_preview = (
            exchange_summary.get("refresh_token")
            if isinstance(exchange_summary, dict) and isinstance(exchange_summary.get("refresh_token"), dict)
            else None
        )
        persisted_refresh_preview = (
            persisted_summary.get("refresh_token")
            if isinstance(persisted_summary, dict) and isinstance(persisted_summary.get("refresh_token"), dict)
            else None
        )
        request_refresh_preview = _token_preview(refresh_token)
        artifact = {
            "generated_at": _now_iso(),
            "token_endpoint_url": self.config.token_url,
            "callback_url": self.config.callback_url,
            "app_key_fingerprint": _fingerprint_value(self.config.app_key),
            "token_file_path": str(self.token_store.path.expanduser().resolve(strict=False)),
            "refresh_succeeds": refresh_succeeds,
            "request_url": request.url,
            "request_form_keys": sorted((request.form or {}).keys()),
            "exchange_refresh_token_preview": exchange_refresh_preview,
            "persisted_refresh_token_preview": persisted_refresh_preview,
            "request_refresh_token_preview": request_refresh_preview,
            "request_refresh_matches_exchange": _token_preview_matches(request_refresh_preview, exchange_refresh_preview),
            "request_refresh_matches_persisted": _token_preview_matches(request_refresh_preview, persisted_refresh_preview),
            "refresh_response_summary": _summarize_token_mapping(refresh_payload or {}) if refresh_payload is not None else None,
            "provider_status_code": getattr(provider_error, "status_code", None),
            "provider_response_body": getattr(provider_error, "response_body", None),
            "error_text": str(provider_error) if provider_error is not None else None,
        }
        path.write_text(json.dumps(artifact, indent=2, sort_keys=True), encoding="utf-8")


def _normalize_authorization_code(code: str) -> str:
    normalized = code.strip()
    if not normalized:
        raise SchwabAuthError("Authorization code must not be empty.")
    return unquote(normalized)


def build_auth_metadata(config: SchwabAuthConfig) -> dict[str, str]:
    return {
        "client_key_fingerprint": _fingerprint_value(config.app_key),
        "client_secret_fingerprint": _fingerprint_value(config.app_secret),
        "callback_url": config.callback_url,
        "token_store_path": str(config.token_store_path),
    }


def _fingerprint_value(value: str) -> str:
    digest = hashlib.sha256(value.encode("utf-8")).hexdigest()
    return digest[:12]


def _token_string(payload: dict[str, object], key: str) -> str | None:
    value = payload.get(key)
    if value in (None, ""):
        return None
    return str(value)


def _token_preview(value: str | None) -> dict[str, object]:
    token = value or ""
    return {
        "present": bool(token),
        "length": len(token),
        "head": token[:4],
        "tail": token[-4:] if token else "",
    }


def _token_preview_matches(left: dict[str, object] | None, right: dict[str, object] | None) -> bool | None:
    if left is None or right is None:
        return None
    return left == right


def _summarize_token_mapping(payload: dict[str, object]) -> dict[str, object]:
    return {
        "has_access_token": bool(payload.get("access_token")),
        "has_refresh_token": bool(payload.get("refresh_token")),
        "has_id_token": bool(payload.get("id_token")),
        "has_token_type": bool(payload.get("token_type")),
        "has_scope": bool(payload.get("scope")),
        "has_expires_in": payload.get("expires_in") is not None,
        "token_type": payload.get("token_type"),
        "scope": payload.get("scope"),
        "access_token": _token_preview(_token_string(payload, "access_token")),
        "refresh_token": _token_preview(_token_string(payload, "refresh_token")),
        "id_token": _token_preview(_token_string(payload, "id_token")),
        "issued_at": payload.get("issued_at"),
        "expires_in": payload.get("expires_in"),
    }


def _safe_json_dict(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    return raw if isinstance(raw, dict) else None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_schwab_auth_config_from_env(token_file: str | Path | None = None) -> SchwabAuthConfig:
    """Load required Schwab auth fields from environment variables."""
    app_key = os.environ.get("SCHWAB_APP_KEY")
    app_secret = os.environ.get("SCHWAB_APP_SECRET")
    callback_url = os.environ.get("SCHWAB_CALLBACK_URL")
    if not app_key or not app_secret or not callback_url:
        raise SchwabAuthError(
            "SCHWAB_APP_KEY, SCHWAB_APP_SECRET, and SCHWAB_CALLBACK_URL must all be set."
        )

    if token_file is None:
        token_file = os.environ.get("SCHWAB_TOKEN_FILE", ".local/schwab/tokens.json")
    token_path = Path(token_file).expanduser()
    if not token_path.is_absolute():
        repo_root = Path(os.environ.get("REPO_ROOT", os.getcwd()))
        token_path = repo_root / token_path
    resolved_token_file = token_path.resolve(strict=False)

    return SchwabAuthConfig(
        app_key=app_key,
        app_secret=app_secret,
        callback_url=callback_url,
        token_store_path=resolved_token_file,
    )
