"""Schwab OAuth helpers and local token persistence."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from urllib.parse import unquote, urlencode

from .schwab_models import HttpRequest, JsonHttpTransport, SchwabAuthConfig, SchwabTokenSet


class SchwabAuthError(RuntimeError):
    """Raised when Schwab auth config or token state is invalid."""


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
        payload = self.transport.request_json(
            HttpRequest(
                method="POST",
                url=self.config.token_url,
                headers=self._token_headers(),
                form={
                    "grant_type": "authorization_code",
                    "code": normalized_code,
                    "redirect_uri": self.config.callback_url,
                    "client_id": self.config.app_key,
                },
            )
        )
        token_set = SchwabTokenSet.from_token_response(payload)
        self.token_store.save(token_set, auth_metadata=build_auth_metadata(self.config))
        return token_set

    def refresh_token(self, refresh_token: Optional[str] = None) -> SchwabTokenSet:
        token_to_refresh = refresh_token
        if token_to_refresh is None:
            stored = self.token_store.load()
            if stored is None or not stored.refresh_token:
                raise SchwabAuthError("No refresh token is available in the local token store.")
            token_to_refresh = stored.refresh_token

        payload = self.transport.request_json(
            HttpRequest(
                method="POST",
                url=self.config.token_url,
                headers=self._token_headers(),
                form={
                    "grant_type": "refresh_token",
                    "refresh_token": token_to_refresh,
                    "client_id": self.config.app_key,
                },
            )
        )
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
