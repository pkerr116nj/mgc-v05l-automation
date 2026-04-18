"""Pure IBKR contract qualification helpers for futures-first routing."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime

from ...execution.broker_requests import BrokerContractRequest

_MONTH_CODES = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z",
}


class IbkrContractResolutionError(ValueError):
    """Raised when an internal contract request cannot be qualified for IBKR."""


@dataclass(frozen=True)
class IbkrFuturesContractRule:
    internal_symbol: str
    broker_symbol_root: str
    exchange: str
    currency: str = "USD"
    multiplier: str | None = None
    trading_class: str | None = None
    allowed_exchanges: tuple[str, ...] = ()


@dataclass(frozen=True)
class IbkrQualifiedContract:
    internal_symbol: str
    broker_symbol: str
    local_symbol: str
    security_type: str
    exchange: str
    currency: str
    expiry: str
    multiplier: str | None = None
    trading_class: str | None = None
    con_id: int | None = None
    metadata: dict[str, str] | None = None


def _default_futures_rules() -> dict[str, IbkrFuturesContractRule]:
    rules = (
        IbkrFuturesContractRule(
            internal_symbol="MGC",
            broker_symbol_root="MGC",
            exchange="COMEX",
            multiplier="10",
            trading_class="MGC",
            allowed_exchanges=("COMEX", "NYMEX"),
        ),
        IbkrFuturesContractRule(
            internal_symbol="GC",
            broker_symbol_root="GC",
            exchange="COMEX",
            multiplier="100",
            trading_class="GC",
            allowed_exchanges=("COMEX",),
        ),
        IbkrFuturesContractRule(
            internal_symbol="MES",
            broker_symbol_root="MES",
            exchange="CME",
            multiplier="5",
            trading_class="MES",
            allowed_exchanges=("CME", "GLOBEX"),
        ),
        IbkrFuturesContractRule(
            internal_symbol="ES",
            broker_symbol_root="ES",
            exchange="CME",
            multiplier="50",
            trading_class="ES",
            allowed_exchanges=("CME", "GLOBEX"),
        ),
        IbkrFuturesContractRule(
            internal_symbol="MNQ",
            broker_symbol_root="MNQ",
            exchange="CME",
            multiplier="2",
            trading_class="MNQ",
            allowed_exchanges=("CME", "GLOBEX"),
        ),
        IbkrFuturesContractRule(
            internal_symbol="NQ",
            broker_symbol_root="NQ",
            exchange="CME",
            multiplier="20",
            trading_class="NQ",
            allowed_exchanges=("CME", "GLOBEX"),
        ),
        IbkrFuturesContractRule(
            internal_symbol="CL",
            broker_symbol_root="CL",
            exchange="NYMEX",
            multiplier="1000",
            trading_class="CL",
            allowed_exchanges=("NYMEX",),
        ),
        IbkrFuturesContractRule(
            internal_symbol="PL",
            broker_symbol_root="PL",
            exchange="NYMEX",
            multiplier="50",
            trading_class="PL",
            allowed_exchanges=("NYMEX",),
        ),
        IbkrFuturesContractRule(
            internal_symbol="HG",
            broker_symbol_root="HG",
            exchange="COMEX",
            multiplier="25000",
            trading_class="HG",
            allowed_exchanges=("COMEX",),
        ),
        IbkrFuturesContractRule(
            internal_symbol="6E",
            broker_symbol_root="EUR",
            exchange="CME",
            multiplier="125000",
            trading_class="6E",
            allowed_exchanges=("CME", "GLOBEX"),
        ),
        IbkrFuturesContractRule(
            internal_symbol="6J",
            broker_symbol_root="JPY",
            exchange="CME",
            multiplier="12500000",
            trading_class="6J",
            allowed_exchanges=("CME", "GLOBEX"),
        ),
    )
    return {rule.internal_symbol: rule for rule in rules}


def _normalize_symbol(symbol: str) -> str:
    return str(symbol or "").strip().upper()


def _normalize_expiry(expiry: str | None) -> str:
    normalized = str(expiry or "").strip()
    if not normalized:
        raise IbkrContractResolutionError("IBKR futures qualification requires an explicit expiry in YYYYMM format.")
    digits = "".join(character for character in normalized if character.isdigit())
    if len(digits) == 8:
        digits = digits[:6]
    if len(digits) != 6:
        raise IbkrContractResolutionError(f"Unsupported futures expiry {normalized!r}; expected YYYYMM or YYYYMMDD.")
    year = int(digits[:4])
    month = int(digits[4:6])
    if year < 2000 or month < 1 or month > 12:
        raise IbkrContractResolutionError(f"Unsupported futures expiry {normalized!r}; expected a real calendar month.")
    return digits


def _month_code(expiry_yyyymm: str) -> str:
    return _MONTH_CODES[int(expiry_yyyymm[4:6])]


def _local_symbol(root: str, expiry_yyyymm: str) -> str:
    year_suffix = expiry_yyyymm[2:4]
    return f"{root}{_month_code(expiry_yyyymm)}{year_suffix}"


def _front_month_expiry(now: date | datetime | None = None) -> str:
    anchor = now.date() if isinstance(now, datetime) else (now or date.today())
    return f"{anchor.year:04d}{anchor.month:02d}"


class IbkrContractResolver:
    """Pure resolver that validates and normalizes contract identity for IBKR."""

    def __init__(
        self,
        *,
        futures_rules: dict[str, IbkrFuturesContractRule] | None = None,
    ) -> None:
        self._futures_rules = futures_rules or _default_futures_rules()

    def qualify_contract(
        self,
        contract: BrokerContractRequest,
        *,
        now: date | datetime | None = None,
    ) -> IbkrQualifiedContract:
        asset_class = _normalize_symbol(contract.asset_class)
        if asset_class != "FUTURE":
            raise IbkrContractResolutionError(
                f"IBKR contract resolver currently supports FUTURE requests only; received {asset_class or 'UNKNOWN'}."
            )
        return self.qualify_futures(
            symbol=contract.symbol,
            exchange=contract.exchange,
            currency=contract.currency,
            expiry=contract.expiry,
            multiplier=contract.multiplier,
            broker_symbol=contract.broker_symbol,
            now=now,
        )

    def qualify_futures(
        self,
        *,
        symbol: str,
        exchange: str | None = None,
        currency: str | None = None,
        expiry: str | None = None,
        multiplier: str | None = None,
        broker_symbol: str | None = None,
        now: date | datetime | None = None,
    ) -> IbkrQualifiedContract:
        normalized_symbol = _normalize_symbol(symbol)
        if not normalized_symbol:
            raise IbkrContractResolutionError("IBKR futures qualification requires a non-empty internal symbol.")
        rule = self._futures_rules.get(normalized_symbol)
        if rule is None:
            supported = ", ".join(sorted(self._futures_rules))
            raise IbkrContractResolutionError(
                f"Unsupported IBKR futures symbol {normalized_symbol!r}. Supported symbols: {supported}."
            )

        resolved_exchange = _normalize_symbol(exchange) or rule.exchange
        allowed_exchanges = tuple(exchange_name.upper() for exchange_name in (rule.allowed_exchanges or (rule.exchange,)))
        if resolved_exchange not in allowed_exchanges:
            allowed_list = ", ".join(allowed_exchanges)
            raise IbkrContractResolutionError(
                f"IBKR futures symbol {normalized_symbol} does not allow exchange {resolved_exchange}; allowed exchanges: {allowed_list}."
            )

        resolved_expiry = _normalize_expiry(expiry) if expiry else _front_month_expiry(now=now)
        resolved_broker_symbol = _normalize_symbol(broker_symbol) or rule.broker_symbol_root
        resolved_currency = _normalize_symbol(currency) or rule.currency
        resolved_multiplier = str(multiplier or rule.multiplier or "").strip() or None
        local_symbol = _local_symbol(resolved_broker_symbol, resolved_expiry)
        metadata = {
            "contract_month": resolved_expiry,
            "month_code": _month_code(resolved_expiry),
            "local_symbol_hint": local_symbol,
        }
        return IbkrQualifiedContract(
            internal_symbol=normalized_symbol,
            broker_symbol=resolved_broker_symbol,
            local_symbol=local_symbol,
            security_type="FUT",
            exchange=resolved_exchange,
            currency=resolved_currency,
            expiry=resolved_expiry,
            multiplier=resolved_multiplier,
            trading_class=rule.trading_class,
            metadata=metadata,
        )

