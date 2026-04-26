"""Provider-aware TradeStation runtime config placeholders."""

from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from ..execution.tradestation_auth import TradeStationEnvironment


@dataclass(frozen=True)
class TradeStationConfig:
    client_id: str = ""
    client_secret: str = ""
    redirect_uri: str = "http://127.0.0.1:0/callback"
    request_timeout_seconds: int = 30
    sim_token_store_path: Path = Path("var/tradestation/sim_tokens.json")
    live_token_store_path: Path = Path("var/tradestation/live_tokens.json")
    selected_accounts_path: Path = Path("var/tradestation/selected_accounts.json")

    def token_store_path(self, environment: TradeStationEnvironment) -> Path:
        if environment == TradeStationEnvironment.SIM:
            return self.sim_token_store_path
        return self.live_token_store_path


def load_tradestation_config() -> TradeStationConfig:
    return TradeStationConfig(
        client_id=os.environ.get("TRADESTATION_CLIENT_ID", ""),
        client_secret=os.environ.get("TRADESTATION_CLIENT_SECRET", ""),
        redirect_uri=os.environ.get("TRADESTATION_REDIRECT_URI", "http://127.0.0.1:0/callback"),
        request_timeout_seconds=int(os.environ.get("TRADESTATION_REQUEST_TIMEOUT_SECONDS", "30")),
        sim_token_store_path=Path(
            os.environ.get("TRADESTATION_SIM_TOKEN_STORE_PATH", "var/tradestation/sim_tokens.json")
        ).expanduser(),
        live_token_store_path=Path(
            os.environ.get("TRADESTATION_LIVE_TOKEN_STORE_PATH", "var/tradestation/live_tokens.json")
        ).expanduser(),
        selected_accounts_path=Path(
            os.environ.get("TRADESTATION_SELECTED_ACCOUNTS_PATH", "var/tradestation/selected_accounts.json")
        ).expanduser(),
    )
