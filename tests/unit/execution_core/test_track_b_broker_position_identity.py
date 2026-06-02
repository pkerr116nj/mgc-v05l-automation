from __future__ import annotations

from datetime import UTC, datetime
from decimal import Decimal

from mgc_v05l.execution_core.track_b_broker_position_identity import (
    IDENTITY_AMBIGUOUS,
    IDENTITY_NOT_READY,
    IDENTITY_READY,
    canonicalize_broker_position_identity,
)
from mgc_v05l.execution_core.track_b_central_trade_registry import TradeEvent, TradeEventType, reduce_trade_events


NOW = datetime(2026, 6, 1, 22, 10, tzinfo=UTC)


def test_missing_con_id_resolves_from_exact_registry_local_symbol_account() -> None:
    result = canonicalize_broker_position_identity(
        broker_position=_broker_position(con_id=None),
        registry_records=[_registry_record()],
    )

    assert result.classification == IDENTITY_READY
    assert result.canonical_position["con_id"] == 770561194
    assert result.source == "CENTRAL_TRADE_REGISTRY"


def test_ambiguous_local_symbol_mapping_blocks() -> None:
    first = _registry_record(trade_id="trade_a", lifecycle_id="life_a", con_id=770561194)
    second = _registry_record(trade_id="trade_b", lifecycle_id="life_b", con_id=999999999)

    result = canonicalize_broker_position_identity(
        broker_position=_broker_position(con_id=None),
        registry_records=[first, second],
    )

    assert result.classification == IDENTITY_AMBIGUOUS
    assert result.reason_codes == ("AMBIGUOUS_CON_ID_FOR_LOCAL_SYMBOL_ACCOUNT",)


def test_wrong_account_blocks_registry_canonicalization() -> None:
    result = canonicalize_broker_position_identity(
        broker_position={**_broker_position(con_id=None), "account_id": "OTHER"},
        registry_records=[_registry_record()],
    )

    assert result.classification == IDENTITY_NOT_READY


def test_wrong_local_symbol_blocks_registry_canonicalization() -> None:
    result = canonicalize_broker_position_identity(
        broker_position={**_broker_position(con_id=None), "local_symbol": "MNQM6", "symbol": "MNQ"},
        registry_records=[_registry_record()],
    )

    assert result.classification == IDENTITY_NOT_READY


def test_contract_resolver_can_fill_contract_identity_without_trade_ownership() -> None:
    result = canonicalize_broker_position_identity(
        broker_position=_broker_position(con_id=None),
        contract_resolver_status={
            "selected_contracts": {
                "MES": {
                    "symbol": "MES",
                    "local_symbol": "MESM6",
                    "con_id": 770561194,
                }
            }
        },
    )

    assert result.classification == IDENTITY_READY
    assert result.canonical_position["con_id"] == 770561194
    assert result.source == "CONTRACT_RESOLVER_STATUS"


def _registry_record(
    *,
    trade_id: str = "trade_mes",
    lifecycle_id: str = "life_mes",
    con_id: int = 770561194,
):
    return reduce_trade_events(
        [
            TradeEvent(
                event_id=f"{trade_id}_fill",
                event_type=TradeEventType.ENTRY_FILL_BROKER_BACKED,
                generated_at=NOW,
                trade_id=trade_id,
                lifecycle_id=lifecycle_id,
                lane_id="mes_globex_active_participation_short",
                thesis_strategy_id="mes_globex_active_participation_short",
                account_id="DUM882026",
                symbol="MES",
                con_id=con_id,
                local_symbol="MESM6",
                expiry="202606",
                side="SHORT",
                action="SELL",
                qty=Decimal("1"),
                source_artifact_path="outputs/test.json",
                order_id="1",
                client_id="111",
                perm_id="perm_mes",
                exec_id="exec_mes",
                price=Decimal("7598.75"),
            )
        ]
    )


def _broker_position(*, con_id: int | None) -> dict:
    row = {
        "account_id": "DUM882026",
        "symbol": "MES",
        "track_b_root": "MES",
        "local_symbol": "MESM6",
        "expiry": "20260618",
        "quantity": "-1",
    }
    if con_id is not None:
        row["con_id"] = con_id
    return row
