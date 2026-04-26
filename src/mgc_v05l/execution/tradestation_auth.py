"""TradeStation auth and local token/account selection persistence."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from pathlib import Path
from typing import Any
from urllib.parse import urlencode


class TradeStationAuthError(RuntimeError):
    """Raised when TradeStation auth state is invalid."""


class TradeStationEnvironment(str, Enum):
    SIM = "sim"
    LIVE = "live"


TRADESTATION_AUTHORIZE_URL = "https://signin.tradestation.com/authorize"
TRADESTATION_API_AUDIENCE = "https://api.tradestation.com"


@dataclass(frozen=True)
class TradeStationOAuthConfig:
    client_id: str
    client_secret: str
    redirect_uri: str
    authorize_url: str = TRADESTATION_AUTHORIZE_URL
    audience: str = TRADESTATION_API_AUDIENCE


@dataclass(frozen=True)
class TradeStationOAuthClient:
    """Local authorize-URL helper for Stage 1B read-only bootstrap."""

    config: TradeStationOAuthConfig

    def build_authorize_url(self, *, state: str, scopes: list[str]) -> str:
        query = {
            "response_type": "code",
            "client_id": self.config.client_id,
            "redirect_uri": self.config.redirect_uri,
            "audience": self.config.audience,
            "scope": " ".join(str(item).strip() for item in scopes if str(item).strip()),
            "state": state,
        }
        return f"{self.config.authorize_url}?{urlencode(query)}"


@dataclass(frozen=True)
class TradeStationTokenSet:
    access_token: str
    refresh_token: str | None
    token_type: str = "Bearer"
    expires_in: int = 0
    scope: str | None = None
    issued_at: datetime = datetime.now(timezone.utc)

    @classmethod
    def from_json_dict(cls, payload: dict[str, Any]) -> "TradeStationTokenSet":
        issued_at = _parse_datetime(payload.get("issued_at")) or datetime.now(timezone.utc)
        return cls(
            access_token=str(payload.get("access_token") or ""),
            refresh_token=_clean_optional_text(payload.get("refresh_token")),
            token_type=str(payload.get("token_type") or "Bearer"),
            expires_in=int(payload.get("expires_in") or 0),
            scope=_clean_optional_text(payload.get("scope")),
            issued_at=issued_at,
        )

    def to_json_dict(self) -> dict[str, Any]:
        return {
            "access_token": self.access_token,
            "refresh_token": self.refresh_token,
            "token_type": self.token_type,
            "expires_in": self.expires_in,
            "scope": self.scope,
            "issued_at": self.issued_at.isoformat(),
        }

    def is_expired(self, *, now: datetime | None = None) -> bool:
        if self.expires_in <= 0:
            return False
        current = now or datetime.now(timezone.utc)
        expiry = self.issued_at + timedelta(seconds=self.expires_in)
        return current >= expiry


class TradeStationTokenStore:
    """Small JSON token store with one file per environment."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    @property
    def path(self) -> Path:
        return self._path

    def load(self) -> TradeStationTokenSet | None:
        if not self._path.exists():
            return None
        payload = json.loads(self._path.read_text(encoding="utf-8"))
        return TradeStationTokenSet.from_json_dict(payload)

    def save(self, token_set: TradeStationTokenSet) -> None:
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(token_set.to_json_dict(), indent=2, sort_keys=True), encoding="utf-8")


@dataclass(frozen=True)
class TradeStationSelectedAccounts:
    selected_margin_account_id: str | None = None
    selected_futures_account_id: str | None = None

    @classmethod
    def from_payload(cls, payload: dict[str, Any]) -> "TradeStationSelectedAccounts":
        return cls(
            selected_margin_account_id=_clean_optional_text(payload.get("selected_margin_account_id")),
            selected_futures_account_id=_clean_optional_text(payload.get("selected_futures_account_id")),
        )

    def to_payload(self) -> dict[str, Any]:
        return {
            "selected_margin_account_id": self.selected_margin_account_id,
            "selected_futures_account_id": self.selected_futures_account_id,
        }


class TradeStationSelectedAccountsStore:
    """Persists selected TradeStation accounts separately for SIM and LIVE."""

    def __init__(self, path: Path) -> None:
        self._path = Path(path)

    @property
    def path(self) -> Path:
        return self._path

    def load_all(self) -> dict[str, Any]:
        if not self._path.exists():
            return {}
        payload = json.loads(self._path.read_text(encoding="utf-8"))
        return payload if isinstance(payload, dict) else {}

    def load_environment(self, environment: TradeStationEnvironment) -> TradeStationSelectedAccounts:
        payload = self.load_all()
        env_payload = payload.get(environment.value)
        if not isinstance(env_payload, dict):
            return TradeStationSelectedAccounts()
        return TradeStationSelectedAccounts.from_payload(env_payload)

    def save_environment(self, environment: TradeStationEnvironment, selection: TradeStationSelectedAccounts) -> None:
        payload = self.load_all()
        payload[environment.value] = selection.to_payload()
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def _clean_optional_text(value: Any) -> str | None:
    text = str(value or "").strip()
    return text or None


def _parse_datetime(value: Any) -> datetime | None:
    text = _clean_optional_text(value)
    if text is None:
        return None
    try:
        parsed = datetime.fromisoformat(text)
    except ValueError:
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed
