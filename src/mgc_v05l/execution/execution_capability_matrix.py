"""Execution capability matrix for staged broker-family routing."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Final


@dataclass(frozen=True)
class ExecutionCapability:
    internal_symbol: str
    asset_class: str
    broker_family: str
    account_type: str
    environment: str
    execution_eligible: bool


STAGE2_FUTURES_SIM_CAPABILITIES: Final[dict[str, ExecutionCapability]] = {
    symbol: ExecutionCapability(
        internal_symbol=symbol,
        asset_class="FUTURE",
        broker_family="tradestation",
        account_type="futures",
        environment="sim",
        execution_eligible=True,
    )
    for symbol in ("ES", "MES", "NQ", "MNQ")
}


def resolve_stage2_futures_sim_capability(symbol: str, *, environment: str) -> ExecutionCapability | None:
    normalized_symbol = str(symbol or "").strip().upper()
    capability = STAGE2_FUTURES_SIM_CAPABILITIES.get(normalized_symbol)
    if capability is None:
        return None
    if str(environment or "").strip().lower() != capability.environment:
        return None
    return capability
