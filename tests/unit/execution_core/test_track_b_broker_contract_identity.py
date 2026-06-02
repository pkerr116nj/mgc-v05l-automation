from __future__ import annotations

import pytest

from mgc_v05l.execution_core.track_b_broker_contract_identity import (
    BrokerContractIdentityError,
    canonicalize_broker_bound_contract_identity,
)


def test_exact_local_symbol_resolves_canonical_mes_expiry_without_con_id() -> None:
    identity = canonicalize_broker_bound_contract_identity(
        base={
            "symbol": "MES",
            "local_symbol": "MESM6",
            "expiry": "202606",
        }
    )

    assert identity is not None
    assert identity.con_id == "770561194"
    assert identity.local_symbol == "MESM6"
    assert identity.expiry == "20260618"
    assert identity.exchange == "CME"
    assert identity.multiplier == "5"


def test_contract_identity_blocks_conid_expiry_mismatch() -> None:
    with pytest.raises(BrokerContractIdentityError, match="conflicts with canonical IBKR expiry"):
        canonicalize_broker_bound_contract_identity(
            base={
                "symbol": "MES",
                "local_symbol": "MESM6",
                "con_id": 770561194,
                "expiry": "20260718",
            }
        )


def test_contract_identity_blocks_ambiguous_exact_sources() -> None:
    with pytest.raises(BrokerContractIdentityError, match="ambiguous conId values"):
        canonicalize_broker_bound_contract_identity(
            base={"symbol": "MES", "local_symbol": "MESM6", "con_id": 770561194, "expiry": "20260618"},
            sources=({"symbol": "MES", "local_symbol": "MESM6", "con_id": 999999999, "expiry": "20260618"},),
        )
