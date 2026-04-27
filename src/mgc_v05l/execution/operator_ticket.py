"""Immutable operator ticket models for staged dry-run execution."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field, is_dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Any


def _serialize_value(value: Any) -> Any:
    if is_dataclass(value):
        return {key: _serialize_value(item) for key, item in asdict(value).items()}
    if isinstance(value, dict):
        return {str(key): _serialize_value(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_serialize_value(item) for item in value]
    if isinstance(value, Decimal):
        return str(value)
    if isinstance(value, datetime):
        return value.isoformat()
    if isinstance(value, Enum):
        return value.value
    return value


class DryRunValidationStatus(str, Enum):
    VALID = "VALID"
    VALID_WITH_WARNINGS = "VALID_WITH_WARNINGS"
    REJECTED = "REJECTED"


@dataclass(frozen=True)
class OperatorExecutionTicket:
    ticket_id: str
    signal_id: str
    strategy_id: str
    generated_at: datetime
    environment: str
    asset_class: str
    internal_symbol: str
    broker_symbol: str
    account_id: str
    account_type: str
    decision_state: str
    timing_bucket: str
    side: str
    quantity: Decimal
    entry_order_type: str
    entry_reference_price: Decimal | None
    entry_limit_price: Decimal | None
    time_in_force: str
    session: str
    risk_profile: str
    fixed_stop_points: Decimal | None
    fixed_target_points: Decimal | None
    time_stop_minutes: int
    contract_metadata: dict[str, Any]
    quote_snapshot: dict[str, Any] | None
    truth_checked_at: datetime | None
    truth_snapshot_age_seconds: int | None
    warnings: tuple[str, ...] = ()
    trace: dict[str, str] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return _serialize_value(self)


@dataclass(frozen=True)
class DryRunTradeStationPayloadArtifact:
    dry_run_payload_id: str
    ticket_id: str
    provider_id: str
    generated_at: datetime
    validation_status: DryRunValidationStatus
    warnings: tuple[str, ...]
    payload: dict[str, Any] | None
    trace: dict[str, str]

    def to_dict(self) -> dict[str, Any]:
        return _serialize_value(self)
