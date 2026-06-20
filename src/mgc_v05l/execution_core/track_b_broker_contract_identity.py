"""Canonical broker-bound futures contract identity helpers.

Strategy and lifecycle artifacts may carry a contract month such as ``202606``.
Broker-bound PAPER orders must carry the exact IBKR expiry and a coherent
conId/localSymbol pair.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Mapping


CANONICAL_FUTURES_CONTRACTS: dict[tuple[str, str], dict[str, str]] = {
    ("712565978", "MGCM6"): {
        "symbol": "MGC",
        "secType": "FUT",
        "exchange": "COMEX",
        "currency": "USD",
        "localSymbol": "MGCM6",
        "conId": "712565978",
        "lastTradeDateOrContractMonth": "20260626",
        "multiplier": "10",
    },
    ("770561201", "MNQM6"): {
        "symbol": "MNQ",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "MNQM6",
        "conId": "770561201",
        "lastTradeDateOrContractMonth": "20260618",
        "multiplier": "2",
    },
    ("770561194", "MESM6"): {
        "symbol": "MES",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "MESM6",
        "conId": "770561194",
        "lastTradeDateOrContractMonth": "20260618",
        "multiplier": "5",
    },
    ("732156883", "MGCQ6"): {
        "symbol": "MGC",
        "secType": "FUT",
        "exchange": "COMEX",
        "currency": "USD",
        "localSymbol": "MGCQ6",
        "conId": "732156883",
        "lastTradeDateOrContractMonth": "20260827",
        "multiplier": "10",
    },
    ("732156872", "GCQ6"): {
        "symbol": "GC",
        "secType": "FUT",
        "exchange": "COMEX",
        "currency": "USD",
        "localSymbol": "GCQ6",
        "conId": "732156872",
        "lastTradeDateOrContractMonth": "20260827",
        "multiplier": "100",
    },
    ("770561204", "NQU6"): {
        "symbol": "NQ",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "NQU6",
        "conId": "770561204",
        "lastTradeDateOrContractMonth": "20260918",
        "multiplier": "20",
    },
    ("793356225", "MNQU6"): {
        "symbol": "MNQ",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "MNQU6",
        "conId": "793356225",
        "lastTradeDateOrContractMonth": "20260918",
        "multiplier": "2",
    },
    ("649180671", "ESU6"): {
        "symbol": "ES",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "ESU6",
        "conId": "649180671",
        "lastTradeDateOrContractMonth": "20260918",
        "multiplier": "50",
    },
    ("793356217", "MESU6"): {
        "symbol": "MES",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "MESU6",
        "conId": "793356217",
        "lastTradeDateOrContractMonth": "20260918",
        "multiplier": "5",
    },
    ("842590391", "ZTU6"): {
        "symbol": "ZT",
        "secType": "FUT",
        "exchange": "CBOT",
        "currency": "USD",
        "localSymbol": "ZTU6",
        "conId": "842590391",
        "lastTradeDateOrContractMonth": "20260930",
        "multiplier": "2000",
    },
    ("842590380", "ZFU6"): {
        "symbol": "ZF",
        "secType": "FUT",
        "exchange": "CBOT",
        "currency": "USD",
        "localSymbol": "ZFU6",
        "conId": "842590380",
        "lastTradeDateOrContractMonth": "20260930",
        "multiplier": "1000",
    },
    ("840227361", "ZNU6"): {
        "symbol": "ZN",
        "secType": "FUT",
        "exchange": "CBOT",
        "currency": "USD",
        "localSymbol": "ZNU6",
        "conId": "840227361",
        "lastTradeDateOrContractMonth": "20260921",
        "multiplier": "1000",
    },
    ("840227357", "ZBU6"): {
        "symbol": "ZB",
        "secType": "FUT",
        "exchange": "CBOT",
        "currency": "USD",
        "localSymbol": "ZBU6",
        "conId": "840227357",
        "lastTradeDateOrContractMonth": "20260921",
        "multiplier": "1000",
    },
    ("772435574", "BTCU6"): {
        "symbol": "BRR",
        "track_b_symbol": "BTC",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "BTCU6",
        "conId": "772435574",
        "lastTradeDateOrContractMonth": "20260925",
        "multiplier": "5",
        "tradingClass": "BTC",
    },
    ("772435596", "MBTU6"): {
        "symbol": "MBT",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "MBTU6",
        "conId": "772435596",
        "lastTradeDateOrContractMonth": "20260925",
        "multiplier": "0.1",
        "tradingClass": "MBT",
    },
    ("772435593", "ETHU6"): {
        "symbol": "ETHUSDRR",
        "track_b_symbol": "ETH",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "ETHU6",
        "conId": "772435593",
        "lastTradeDateOrContractMonth": "20260925",
        "multiplier": "50",
        "tradingClass": "ETH",
    },
    ("772435602", "METU6"): {
        "symbol": "MET",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "METU6",
        "conId": "772435602",
        "lastTradeDateOrContractMonth": "20260925",
        "multiplier": "0.1",
        "tradingClass": "MET",
    },
    ("772435608", "SOLU6"): {
        "symbol": "SOL",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "SOLU6",
        "conId": "772435608",
        "lastTradeDateOrContractMonth": "20260925",
        "multiplier": "500",
        "tradingClass": "SOL",
    },
    ("772435607", "MSLU6"): {
        "symbol": "MSL",
        "secType": "FUT",
        "exchange": "CME",
        "currency": "USD",
        "localSymbol": "MSLU6",
        "conId": "772435607",
        "lastTradeDateOrContractMonth": "20260925",
        "multiplier": "25",
        "tradingClass": "MSL",
    },
}


class BrokerContractIdentityError(RuntimeError):
    """Raised when broker-bound contract identity is contradictory or ambiguous."""


@dataclass(frozen=True)
class CanonicalBrokerContractIdentity:
    symbol: str
    sec_type: str
    exchange: str
    currency: str
    local_symbol: str
    con_id: str
    expiry: str
    multiplier: str
    trading_class: str = ""
    source: str = "EXACT_SOURCE"

    def as_allowlist_entry(self, *, tick_size: Any = None) -> dict[str, Any]:
        entry: dict[str, Any] = {
            "symbol": self.symbol,
            "security_type": self.sec_type,
            "exchange": self.exchange,
            "currency": self.currency,
            "local_symbol": self.local_symbol,
            "con_id": int(self.con_id) if self.con_id.isdigit() else self.con_id,
            "contract_month": self.expiry[:6],
            "expiry": self.expiry,
            "multiplier": self.multiplier,
        }
        if self.trading_class:
            entry["trading_class"] = self.trading_class
        if tick_size not in {None, ""}:
            entry["tick_size"] = tick_size
        return entry

    def as_ibkr_fields(self) -> dict[str, str]:
        fields = {
            "symbol": self.symbol,
            "secType": self.sec_type,
            "exchange": self.exchange,
            "currency": self.currency,
            "localSymbol": self.local_symbol,
            "conId": self.con_id,
            "lastTradeDateOrContractMonth": self.expiry,
            "multiplier": self.multiplier,
        }
        if self.trading_class:
            fields["tradingClass"] = self.trading_class
        return fields


def canonicalize_broker_bound_contract_identity(
    *,
    base: Mapping[str, Any] | None = None,
    sources: Iterable[Mapping[str, Any] | None] = (),
) -> CanonicalBrokerContractIdentity | None:
    """Resolve exact broker-bound contract identity from exact sources.

    Returns ``None`` when identity is simply incomplete. Raises when provided
    identity is contradictory or ambiguous. The helper never guesses from
    symbol alone.
    """

    candidates = [_normalize_contract_source(base or {})]
    candidates.extend(_normalize_contract_source(source or {}) for source in sources)
    candidates = [candidate for candidate in candidates if _has_identity(candidate)]

    con_ids = {_digits(candidate.get("conId")) for candidate in candidates if _digits(candidate.get("conId"))}
    local_symbols = {
        str(candidate.get("localSymbol") or "").strip().upper()
        for candidate in candidates
        if str(candidate.get("localSymbol") or "").strip()
    }
    exact_expiries = {
        _digits(candidate.get("lastTradeDateOrContractMonth"))
        for candidate in candidates
        if len(_digits(candidate.get("lastTradeDateOrContractMonth"))) >= 8
    }
    months = {
        _digits(candidate.get("lastTradeDateOrContractMonth"))[:6]
        for candidate in candidates
        if len(_digits(candidate.get("lastTradeDateOrContractMonth"))) == 6
    }

    if len(con_ids) > 1:
        raise BrokerContractIdentityError(f"ambiguous conId values for broker-bound contract: {sorted(con_ids)}")
    if len(local_symbols) > 1:
        raise BrokerContractIdentityError(
            f"ambiguous localSymbol values for broker-bound contract: {sorted(local_symbols)}"
        )
    if len(exact_expiries) > 1:
        raise BrokerContractIdentityError(
            f"ambiguous exact expiry values for broker-bound contract: {sorted(exact_expiries)}"
        )

    canonical = _canonical_from_static(con_id=next(iter(con_ids), ""), local_symbol=next(iter(local_symbols), ""))
    if canonical is None:
        canonical = _canonical_from_exact_candidate(candidates)
    if canonical is None:
        return None

    canonical_expiry = _digits(canonical.get("lastTradeDateOrContractMonth"))[:8]
    for exact in exact_expiries:
        if exact[:8] != canonical_expiry:
            raise BrokerContractIdentityError(
                f"configured expiry {exact[:8]} conflicts with canonical IBKR expiry {canonical_expiry}"
            )
    for month in months:
        if month and not canonical_expiry.startswith(month):
            raise BrokerContractIdentityError(
                f"configured contract month {month} conflicts with canonical IBKR expiry {canonical_expiry}"
            )

    return CanonicalBrokerContractIdentity(
        symbol=str(canonical.get("symbol") or "").strip().upper(),
        sec_type=str(canonical.get("secType") or "FUT").strip().upper(),
        exchange=str(canonical.get("exchange") or "").strip().upper(),
        currency=str(canonical.get("currency") or "USD").strip().upper(),
        local_symbol=str(canonical.get("localSymbol") or "").strip().upper(),
        con_id=_digits(canonical.get("conId")),
        expiry=canonical_expiry,
        multiplier=str(canonical.get("multiplier") or "").strip(),
        trading_class=str(canonical.get("tradingClass") or canonical.get("trading_class") or "").strip().upper(),
        source=str(canonical.get("source") or "EXACT_SOURCE"),
    )


def _canonical_from_static(*, con_id: str, local_symbol: str) -> dict[str, str] | None:
    if con_id and local_symbol:
        canonical = CANONICAL_FUTURES_CONTRACTS.get((con_id, local_symbol))
        if canonical is not None:
            return {**canonical, "source": "STATIC_CANONICAL_CONTRACT"}
    if local_symbol:
        matches = [value for (_con_id, local), value in CANONICAL_FUTURES_CONTRACTS.items() if local == local_symbol]
        if len(matches) > 1:
            raise BrokerContractIdentityError(f"ambiguous canonical contracts for localSymbol {local_symbol}")
        if len(matches) == 1:
            if con_id and _digits(matches[0].get("conId")) != con_id:
                raise BrokerContractIdentityError(
                    f"configured conId {con_id} conflicts with canonical localSymbol {local_symbol}"
                )
            return {**matches[0], "source": "STATIC_CANONICAL_CONTRACT"}
    return None


def _canonical_from_exact_candidate(candidates: Iterable[Mapping[str, Any]]) -> dict[str, str] | None:
    exact_candidates: list[dict[str, str]] = []
    for candidate in candidates:
        expiry = _digits(candidate.get("lastTradeDateOrContractMonth"))
        if len(expiry) < 8:
            continue
        exact_candidates.append(
            {
                "symbol": str(candidate.get("symbol") or "").strip().upper(),
                "secType": str(candidate.get("secType") or "FUT").strip().upper(),
                "exchange": str(candidate.get("exchange") or "").strip().upper(),
                "currency": str(candidate.get("currency") or "USD").strip().upper(),
                "localSymbol": str(candidate.get("localSymbol") or "").strip().upper(),
                "conId": _digits(candidate.get("conId")),
                "lastTradeDateOrContractMonth": expiry[:8],
                "multiplier": str(candidate.get("multiplier") or "").strip(),
                "tradingClass": str(candidate.get("tradingClass") or "").strip().upper(),
                "source": str(candidate.get("source") or "EXACT_SOURCE"),
            }
        )
    identities = {
        (item["conId"], item["localSymbol"], item["lastTradeDateOrContractMonth"])
        for item in exact_candidates
    }
    if len(identities) > 1:
        raise BrokerContractIdentityError(f"ambiguous exact broker-bound contracts: {sorted(identities)}")
    return exact_candidates[0] if exact_candidates else None


def _normalize_contract_source(source: Mapping[str, Any]) -> dict[str, Any]:
    contract = source.get("contract") if isinstance(source.get("contract"), Mapping) else {}
    merged = {**dict(contract), **dict(source)}
    return {
        "symbol": _first(merged, "symbol", "instrument", "instrument_family"),
        "secType": _first(merged, "secType", "security_type") or "FUT",
        "exchange": _first(merged, "exchange", "primaryExchange"),
        "currency": _first(merged, "currency") or "USD",
        "localSymbol": _first(merged, "localSymbol", "local_symbol", "contract"),
        "conId": _first(merged, "conId", "con_id", "qualified_contract_identifier"),
        "lastTradeDateOrContractMonth": _first(
            merged,
            "lastTradeDateOrContractMonth",
            "expiry",
            "contract_expiry",
            "contract_month",
        ),
        "multiplier": _first(merged, "multiplier"),
        "tradingClass": _first(merged, "tradingClass", "trading_class"),
        "source": _first(merged, "source"),
    }


def _has_identity(candidate: Mapping[str, Any]) -> bool:
    return any(
        str(candidate.get(key) or "").strip()
        for key in ("conId", "localSymbol", "lastTradeDateOrContractMonth")
    )


def _first(source: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        value = source.get(key)
        if value not in {None, ""}:
            return value
    return None


def _digits(value: Any) -> str:
    return "".join(ch for ch in str(value or "").strip() if ch.isdigit())
