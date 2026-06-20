from __future__ import annotations

import pytest

from mgc_v05l.execution_core.track_b_broker_position_identity import (
    IDENTITY_READY,
    canonicalize_broker_position_identity,
)
from mgc_v05l.execution_core.track_b_contract_identity import normalize_track_b_contract_identity


@pytest.mark.parametrize(
    ("symbol", "local_symbol", "expiry", "con_id", "contract_key"),
    (
        ("MGC", "MGCQ6", "20260827", 732156883, "MGC-202608"),
        ("GC", "GCQ6", "20260827", 732156872, "GC-202608"),
        ("NQ", "NQU6", "20260918", 770561204, "NQ-202609"),
        ("ES", "ESU6", "20260918", 649180671, "ES-202609"),
        ("MNQ", "MNQU6", "20260918", 793356225, "MNQ-202609"),
        ("MES", "MESU6", "20260918", 793356217, "MES-202609"),
        ("ZT", "ZTU6", "20260930", 842590391, "ZT-202609"),
        ("ZF", "ZFU6", "20260930", 842590380, "ZF-202609"),
        ("ZN", "ZNU6", "20260921", 840227361, "ZN-202609"),
        ("ZB", "ZBU6", "20260921", 840227357, "ZB-202609"),
        ("BTC", "BTCU6", "20260925", 772435574, "BTC-202609"),
        ("MBT", "MBTU6", "20260925", 772435596, "MBT-202609"),
        ("ETH", "ETHU6", "20260925", 772435593, "ETH-202609"),
        ("MET", "METU6", "20260925", 772435602, "MET-202609"),
        ("SOL", "SOLU6", "20260925", 772435608, "SOL-202609"),
        ("MSL", "MSLU6", "20260925", 772435607, "MSL-202609"),
    ),
)
def test_validated_track_b_futures_normalize_from_broker_local_symbol(
    symbol: str,
    local_symbol: str,
    expiry: str,
    con_id: int,
    contract_key: str,
) -> None:
    identity = normalize_track_b_contract_identity(
        {
            "account_id": "DUM882026",
            "security_type": "FUT",
            "symbol": symbol,
            "local_symbol": local_symbol,
            "expiry": expiry,
            "quantity": "1.0",
        }
    )

    assert identity["classification"] == "TRACK_B_CONTRACT_IDENTITY_RESOLVED"
    assert identity["symbol"] == symbol
    assert identity["local_symbol"] == local_symbol
    assert identity["con_id"] == con_id
    assert identity["expiry"] == expiry
    assert identity["contract_key"] == contract_key


def test_normalizer_fails_closed_on_identity_contradiction() -> None:
    identity = normalize_track_b_contract_identity(
        {
            "account_id": "DUM882026",
            "security_type": "FUT",
            "symbol": "MGC",
            "local_symbol": "MGCQ6",
            "con_id": 732156872,
            "expiry": "20260827",
            "quantity": "1.0",
        }
    )

    assert identity["classification"] == "TRACK_B_CONTRACT_IDENTITY_CONTRADICTION"
    assert identity["resolved"] is False
    assert "con_id_mismatch" in identity["blockers"]


def test_unvalidated_rates_contract_identity_fails_closed() -> None:
    identity = normalize_track_b_contract_identity(
        {
            "account_id": "DUM882026",
            "security_type": "FUT",
            "symbol": "ZT",
            "local_symbol": "ZTZ6",
            "expiry": "20261231",
            "quantity": "1.0",
        }
    )

    assert identity["classification"] == "TRACK_B_CONTRACT_IDENTITY_UNRESOLVED"
    assert identity["resolved"] is False
    assert identity["blockers"] == ["contract_identity_not_in_validated_registry"]


def test_broker_position_identity_uses_shared_validated_contract_registry() -> None:
    result = canonicalize_broker_position_identity(
        broker_position={
            "account_id": "DUM882026",
            "security_type": "FUT",
            "symbol": "GC",
            "local_symbol": "GCQ6",
            "expiry": "20260827",
            "quantity": "1.0",
        }
    )

    assert result.classification == IDENTITY_READY
    assert result.source == "VALIDATED_TRACK_B_FUTURES_CONTRACT_REGISTRY"
    assert result.canonical_position["con_id"] == 732156872
    assert result.canonical_position["contract_key"] == "GC-202608"
