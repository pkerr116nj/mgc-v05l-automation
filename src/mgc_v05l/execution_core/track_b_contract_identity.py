"""Shared Track B futures contract identity normalization."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence


_MONTH_CODES = {
    "F": "01",
    "G": "02",
    "H": "03",
    "J": "04",
    "K": "05",
    "M": "06",
    "N": "07",
    "Q": "08",
    "U": "09",
    "V": "10",
    "X": "11",
    "Z": "12",
}


@dataclass(frozen=True)
class TrackBContractIdentity:
    symbol: str
    local_symbol: str
    con_id: int
    expiry: str
    contract_key: str
    exchange: str
    currency: str = "USD"
    multiplier: str | None = None
    min_tick: str | None = None
    account_id: str | None = None

    def as_dict(self) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "symbol": self.symbol,
            "track_b_root": self.symbol,
            "instrument_family": self.symbol,
            "local_symbol": self.local_symbol,
            "con_id": self.con_id,
            "expiry": self.expiry,
            "contract_key": self.contract_key,
            "exchange": self.exchange,
            "currency": self.currency,
            "qualified_contract_identifier": self.con_id,
        }
        if self.multiplier is not None:
            payload["multiplier"] = self.multiplier
        if self.min_tick is not None:
            payload["min_tick"] = self.min_tick
        if self.account_id:
            payload["account_id"] = self.account_id
        return payload


_VALIDATED_TRACK_B_FUTURES: tuple[TrackBContractIdentity, ...] = (
    TrackBContractIdentity(
        symbol="MGC",
        local_symbol="MGCQ6",
        con_id=732156883,
        expiry="20260827",
        contract_key="MGC-202608",
        exchange="COMEX",
        multiplier="10",
        min_tick="0.1",
    ),
    TrackBContractIdentity(
        symbol="GC",
        local_symbol="GCQ6",
        con_id=732156872,
        expiry="20260827",
        contract_key="GC-202608",
        exchange="COMEX",
        multiplier="100",
        min_tick="0.1",
    ),
    TrackBContractIdentity(
        symbol="NQ",
        local_symbol="NQU6",
        con_id=770561204,
        expiry="20260918",
        contract_key="NQ-202609",
        exchange="CME",
        multiplier="20",
        min_tick="0.25",
    ),
    TrackBContractIdentity(
        symbol="MNQ",
        local_symbol="MNQU6",
        con_id=793356225,
        expiry="20260918",
        contract_key="MNQ-202609",
        exchange="CME",
        multiplier="2",
        min_tick="0.25",
    ),
    TrackBContractIdentity(
        symbol="ES",
        local_symbol="ESU6",
        con_id=649180671,
        expiry="20260918",
        contract_key="ES-202609",
        exchange="CME",
        multiplier="50",
        min_tick="0.25",
    ),
    TrackBContractIdentity(
        symbol="MES",
        local_symbol="MESU6",
        con_id=793356217,
        expiry="20260918",
        contract_key="MES-202609",
        exchange="CME",
        multiplier="5",
        min_tick="0.25",
    ),
    TrackBContractIdentity(
        symbol="ZT",
        local_symbol="ZTU6",
        con_id=842590391,
        expiry="20260930",
        contract_key="ZT-202609",
        exchange="CBOT",
        multiplier="2000",
        min_tick="0.00390625",
    ),
    TrackBContractIdentity(
        symbol="ZF",
        local_symbol="ZFU6",
        con_id=842590380,
        expiry="20260930",
        contract_key="ZF-202609",
        exchange="CBOT",
        multiplier="1000",
        min_tick="0.0078125",
    ),
    TrackBContractIdentity(
        symbol="ZN",
        local_symbol="ZNU6",
        con_id=840227361,
        expiry="20260921",
        contract_key="ZN-202609",
        exchange="CBOT",
        multiplier="1000",
        min_tick="0.015625",
    ),
    TrackBContractIdentity(
        symbol="ZB",
        local_symbol="ZBU6",
        con_id=840227357,
        expiry="20260921",
        contract_key="ZB-202609",
        exchange="CBOT",
        multiplier="1000",
        min_tick="0.03125",
    ),
    TrackBContractIdentity(
        symbol="BTC",
        local_symbol="BTCU6",
        con_id=772435574,
        expiry="20260925",
        contract_key="BTC-202609",
        exchange="CME",
        multiplier="5",
        min_tick="5",
    ),
    TrackBContractIdentity(
        symbol="MBT",
        local_symbol="MBTU6",
        con_id=772435596,
        expiry="20260925",
        contract_key="MBT-202609",
        exchange="CME",
        multiplier="0.1",
        min_tick="5",
    ),
    TrackBContractIdentity(
        symbol="ETH",
        local_symbol="ETHU6",
        con_id=772435593,
        expiry="20260925",
        contract_key="ETH-202609",
        exchange="CME",
        multiplier="50",
        min_tick="0.5",
    ),
    TrackBContractIdentity(
        symbol="MET",
        local_symbol="METU6",
        con_id=772435602,
        expiry="20260925",
        contract_key="MET-202609",
        exchange="CME",
        multiplier="0.1",
        min_tick="0.5",
    ),
    TrackBContractIdentity(
        symbol="SOL",
        local_symbol="SOLU6",
        con_id=772435608,
        expiry="20260925",
        contract_key="SOL-202609",
        exchange="CME",
        multiplier="500",
        min_tick="0.05",
    ),
    TrackBContractIdentity(
        symbol="MSL",
        local_symbol="MSLU6",
        con_id=772435607,
        expiry="20260925",
        contract_key="MSL-202609",
        exchange="CME",
        multiplier="25",
        min_tick="0.05",
    ),
)

VALIDATED_TRACK_B_FUTURES_BY_SYMBOL: dict[str, TrackBContractIdentity] = {
    row.symbol: row for row in _VALIDATED_TRACK_B_FUTURES
}


def normalize_track_b_contract_identity(row: Mapping[str, Any], *, account_id: str | None = None) -> dict[str, Any]:
    """Return a conservative canonical identity for validated Track B futures rows."""

    observed = _observed_identity(row, account_id=account_id)
    candidates = _matching_validated_contracts(observed)
    if not candidates:
        return {
            **observed,
            "classification": "TRACK_B_CONTRACT_IDENTITY_UNRESOLVED",
            "resolved": False,
            "blockers": ["contract_identity_not_in_validated_registry"],
        }
    if len(candidates) > 1:
        contradiction_candidate, contradictions = _best_contradiction_candidate(observed, candidates)
        if contradiction_candidate is not None and contradictions:
            return {
                **observed,
                "classification": "TRACK_B_CONTRACT_IDENTITY_CONTRADICTION",
                "resolved": False,
                "blockers": contradictions,
                "candidate_contract": contradiction_candidate.as_dict(),
                "candidate_contracts": [candidate.as_dict() for candidate in candidates],
            }
        return {
            **observed,
            "classification": "TRACK_B_CONTRACT_IDENTITY_AMBIGUOUS",
            "resolved": False,
            "blockers": ["contract_identity_matches_multiple_validated_contracts"],
            "candidate_contracts": [candidate.as_dict() for candidate in candidates],
        }
    candidate = candidates[0]
    contradictions = _identity_contradictions(observed, candidate)
    if contradictions:
        return {
            **observed,
            "classification": "TRACK_B_CONTRACT_IDENTITY_CONTRADICTION",
            "resolved": False,
            "blockers": contradictions,
            "candidate_contract": candidate.as_dict(),
        }
    return {
        **candidate.as_dict(),
        "account_id": observed.get("account_id") or candidate.account_id,
        "classification": "TRACK_B_CONTRACT_IDENTITY_RESOLVED",
        "resolved": True,
        "blockers": [],
        "source": "VALIDATED_TRACK_B_FUTURES_CONTRACT_REGISTRY",
    }


def normalize_track_b_contract_row(row: Mapping[str, Any], *, account_id: str | None = None) -> dict[str, Any]:
    normalized = dict(row)
    identity = normalize_track_b_contract_identity(row, account_id=account_id)
    normalized["contract_identity"] = identity
    if identity.get("resolved") is True:
        normalized.update(
            {
                "symbol": identity["symbol"],
                "track_b_root": identity["symbol"],
                "instrument_family": identity["symbol"],
                "local_symbol": identity["local_symbol"],
                "con_id": identity["con_id"],
                "expiry": identity["expiry"],
                "contract_key": identity["contract_key"],
                "exchange": normalized.get("exchange") or identity.get("exchange"),
                "currency": normalized.get("currency") or identity.get("currency"),
                "multiplier": normalized.get("multiplier") or identity.get("multiplier"),
                "qualified_contract_identifier": identity["con_id"],
            }
        )
    return normalized


def _observed_identity(row: Mapping[str, Any], *, account_id: str | None = None) -> dict[str, Any]:
    local_symbol = _text(row.get("local_symbol") or row.get("localSymbol"))
    expiry = _text(row.get("expiry") or row.get("lastTradeDateOrContractMonth"))
    contract_key = _text(row.get("contract_key") or row.get("contractKey"))
    contract_month = _contract_month_from_row(
        contract_key=contract_key,
        expiry=expiry,
        local_symbol=local_symbol,
        explicit=_text(row.get("contract_month") or row.get("contractMonth")),
    )
    symbol = (
        _text(row.get("symbol") or row.get("track_b_root") or row.get("instrument") or row.get("instrument_family"))
        or _symbol_from_local_symbol(local_symbol)
        or _symbol_from_contract_key(contract_key)
    )
    return {
        "account_id": _text(row.get("account_id") or row.get("account") or account_id),
        "symbol": symbol,
        "track_b_root": symbol,
        "instrument_family": symbol,
        "local_symbol": local_symbol,
        "con_id": _int_or_none(row.get("con_id") or row.get("conId") or row.get("qualified_contract_identifier")),
        "expiry": expiry,
        "contract_key": contract_key,
        "contract_month": contract_month,
    }


def _matching_validated_contracts(observed: Mapping[str, Any]) -> list[TrackBContractIdentity]:
    con_id = observed.get("con_id")
    local_symbol = _text(observed.get("local_symbol"))
    symbol = _text(observed.get("symbol"))
    expiry = _text(observed.get("expiry"))
    contract_month = _text(observed.get("contract_month"))
    contract_key = _text(observed.get("contract_key"))
    matches: list[TrackBContractIdentity] = []
    for candidate in _VALIDATED_TRACK_B_FUTURES:
        if con_id is not None and candidate.con_id == con_id:
            matches.append(candidate)
            continue
        if local_symbol and candidate.local_symbol == local_symbol:
            matches.append(candidate)
            continue
        if contract_key and candidate.contract_key == contract_key:
            matches.append(candidate)
            continue
        if symbol and candidate.symbol == symbol:
            if expiry and candidate.expiry == expiry:
                matches.append(candidate)
                continue
            if contract_month and candidate.contract_key.endswith(f"-{contract_month}"):
                matches.append(candidate)
                continue
    return list(dict.fromkeys(matches))


def _best_contradiction_candidate(
    observed: Mapping[str, Any],
    candidates: Sequence[TrackBContractIdentity],
) -> tuple[TrackBContractIdentity | None, list[str]]:
    scored: list[tuple[int, int, TrackBContractIdentity, list[str]]] = []
    for candidate in candidates:
        contradictions = _identity_contradictions(observed, candidate)
        if not contradictions:
            continue
        scored.append((-_identity_match_score(observed, candidate), len(contradictions), candidate, contradictions))
    if not scored:
        return None, []
    _, _count, candidate, contradictions = min(scored, key=lambda item: (item[0], item[1], item[2].symbol))
    return candidate, contradictions


def _identity_match_score(observed: Mapping[str, Any], candidate: TrackBContractIdentity) -> int:
    score = 0
    contract_month = _text(observed.get("contract_month"))
    if observed.get("con_id") == candidate.con_id:
        score += 8
    if _text(observed.get("local_symbol")) == candidate.local_symbol:
        score += 6
    if _text(observed.get("contract_key")) == candidate.contract_key:
        score += 5
    if _text(observed.get("symbol")) == candidate.symbol:
        score += 3
    if _text(observed.get("expiry")) == candidate.expiry:
        score += 2
    if contract_month and candidate.contract_key.endswith(f"-{contract_month}"):
        score += 1
    return score


def _identity_contradictions(observed: Mapping[str, Any], candidate: TrackBContractIdentity) -> list[str]:
    contradictions: list[str] = []
    con_id = observed.get("con_id")
    if con_id is not None and con_id != candidate.con_id:
        contradictions.append("con_id_mismatch")
    local_symbol = _text(observed.get("local_symbol"))
    if local_symbol and local_symbol != candidate.local_symbol:
        contradictions.append("local_symbol_mismatch")
    symbol = _text(observed.get("symbol"))
    if symbol and symbol != candidate.symbol:
        contradictions.append("symbol_mismatch")
    expiry = _text(observed.get("expiry"))
    if expiry and expiry != candidate.expiry:
        contradictions.append("expiry_mismatch")
    contract_key = _text(observed.get("contract_key"))
    if contract_key and contract_key != candidate.contract_key:
        contradictions.append("contract_key_mismatch")
    return contradictions


def _contract_month_from_row(
    *,
    contract_key: str | None,
    expiry: str | None,
    local_symbol: str | None,
    explicit: str | None,
) -> str | None:
    if explicit:
        return explicit[:6]
    if contract_key and "-" in contract_key:
        suffix = contract_key.rsplit("-", 1)[-1]
        if suffix.isdigit() and len(suffix) >= 6:
            return suffix[:6]
    if expiry and len(expiry) >= 6 and expiry[:6].isdigit():
        return expiry[:6]
    if local_symbol and len(local_symbol) >= 2:
        month = _MONTH_CODES.get(local_symbol[-2:-1])
        year_digit = local_symbol[-1:]
        if month and year_digit.isdigit():
            return f"202{year_digit}{month}"
    return None


def _symbol_from_local_symbol(local_symbol: str | None) -> str | None:
    if not local_symbol:
        return None
    for symbol in sorted(VALIDATED_TRACK_B_FUTURES_BY_SYMBOL, key=len, reverse=True):
        if local_symbol.startswith(symbol):
            return symbol
    return None


def _symbol_from_contract_key(contract_key: str | None) -> str | None:
    if not contract_key or "-" not in contract_key:
        return None
    symbol = contract_key.split("-", 1)[0].strip().upper()
    return symbol if symbol in VALIDATED_TRACK_B_FUTURES_BY_SYMBOL else None


def _text(value: Any) -> str | None:
    text = str(value or "").strip().upper()
    return text or None


def _int_or_none(value: Any) -> int | None:
    try:
        if value in (None, ""):
            return None
        return int(value)
    except (TypeError, ValueError):
        return None
