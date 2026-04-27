"""TradeStation account routing helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .tradestation_broker_family import ACCOUNT_TYPE_FUTURES


class TradeStationAccountRouteError(RuntimeError):
    """Raised when account routing cannot produce a safe execution route."""


@dataclass(frozen=True)
class TradeStationAccountRoute:
    account_id: str
    account_type: str
    environment: str


class TradeStationFuturesAccountRouter:
    """Resolve the selected TradeStation futures account from normalized truth."""

    def resolve_from_truth(self, truth_payload: dict[str, Any], *, environment: str) -> TradeStationAccountRoute:
        normalized_environment = str(environment or "").strip().lower()
        if normalized_environment != "sim":
            raise TradeStationAccountRouteError("Stage 2 futures dry-run tickets are restricted to the TradeStation SIM environment.")
        selected_accounts = dict(truth_payload.get("selected_accounts") or {})
        futures_account_id = str(selected_accounts.get("selected_futures_account_id") or "").strip()
        if not futures_account_id:
            raise TradeStationAccountRouteError("No selected TradeStation futures account id is present in the truth snapshot.")
        per_account = dict(truth_payload.get("per_account") or {})
        account_row = dict(per_account.get(futures_account_id) or {})
        account_type = str(account_row.get("account_type") or "").strip().lower()
        if account_type != ACCOUNT_TYPE_FUTURES:
            raise TradeStationAccountRouteError(
                f"Selected TradeStation futures account {futures_account_id} is classified as {account_type or 'unknown'}, not futures."
            )
        return TradeStationAccountRoute(
            account_id=futures_account_id,
            account_type=account_type,
            environment=normalized_environment,
        )
