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


def test_rates_contract_resolves_validated_cbot_exchange_even_with_stale_default_exchange() -> None:
    identity = canonicalize_broker_bound_contract_identity(
        base={
            "symbol": "ZF",
            "local_symbol": "ZFU6",
            "con_id": 842590380,
            "expiry": "202609",
            "exchange": "COMEX",
        }
    )

    assert identity is not None
    assert identity.con_id == "842590380"
    assert identity.local_symbol == "ZFU6"
    assert identity.expiry == "20260930"
    assert identity.exchange == "CBOT"
    assert identity.multiplier == "1000"


def test_btc_contract_resolves_ibkr_brr_root_with_btc_local_symbol() -> None:
    identity = canonicalize_broker_bound_contract_identity(
        base={
            "symbol": "BTC",
            "local_symbol": "BTCU6",
            "con_id": 772435574,
            "expiry": "202609",
        }
    )

    assert identity is not None
    assert identity.symbol == "BRR"
    assert identity.trading_class == "BTC"
    assert identity.local_symbol == "BTCU6"
    assert identity.con_id == "772435574"
    assert identity.expiry == "20260925"
    assert identity.multiplier == "5"


def test_mbt_contract_resolves_validated_micro_bitcoin_identity() -> None:
    identity = canonicalize_broker_bound_contract_identity(
        base={
            "symbol": "MBT",
            "local_symbol": "MBTU6",
            "con_id": 772435596,
            "expiry": "202609",
        }
    )

    assert identity is not None
    assert identity.symbol == "MBT"
    assert identity.trading_class == "MBT"
    assert identity.local_symbol == "MBTU6"
    assert identity.con_id == "772435596"
    assert identity.expiry == "20260925"
    assert identity.multiplier == "0.1"


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
