"""Provider-aware IBKR runtime config placeholders for the upcoming broker migration."""

from __future__ import annotations

import os
from dataclasses import dataclass


@dataclass(frozen=True)
class IbkrConfig:
    host: str = "127.0.0.1"
    port: int = 7497
    client_id: int = 7
    account_id: str | None = None
    mode: str = "paper"
    allow_live_orders: bool = False


def load_ibkr_config() -> IbkrConfig:
    return IbkrConfig(
        host=os.environ.get("IBKR_HOST", "127.0.0.1"),
        port=int(os.environ.get("IBKR_PORT", "7497")),
        client_id=int(os.environ.get("IBKR_CLIENT_ID", "7")),
        account_id=os.environ.get("IBKR_ACCOUNT_ID"),
        mode=os.environ.get("IBKR_MODE", "paper"),
        allow_live_orders=os.environ.get("IBKR_ALLOW_LIVE_ORDERS", "").strip().lower() == "true",
    )
