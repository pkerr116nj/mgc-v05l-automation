"""Bounded Databento OPRA feed and Schwab/Databento terminal adapter.

Schwab remains authoritative for contract discovery and all broker state. Databento
supplies consolidated option bid/ask prices for the selected expiration through one
long-lived Live API session.
"""

from __future__ import annotations

import os
import threading
import time
from copy import deepcopy
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable

from .schwab import NdxpSchwabAdapter


OPRA_DATASET = "OPRA.PILLAR"
OPRA_SCHEMA = "cmbp-1"
SUBSCRIPTION_STRIKES_EACH_SIDE = 35
SCHWAB_CHAIN_REFRESH_SECONDS = 30.0
_PRICE_SCALE = 1_000_000_000


class DatabentoConfigurationError(RuntimeError):
    """Raised when the explicitly selected Databento mode cannot be configured."""


@dataclass(frozen=True)
class OpraQuote:
    symbol: str
    bid: float
    ask: float
    event_time_ns: int
    received_monotonic: float


class DatabentoOpraFeed:
    """One shared, incrementally subscribed Databento OPRA Live session."""

    def __init__(
        self,
        api_key: str,
        *,
        live_factory: Callable[[str], Any] | None = None,
    ) -> None:
        if not api_key.strip():
            raise DatabentoConfigurationError(
                "DATABENTO_API_KEY is required for --databento. Keep it in the Mars environment; do not paste it into chat."
            )
        self._api_key = api_key.strip()
        self._live_factory = live_factory or _default_live_factory
        self._lock = threading.RLock()
        self._client: Any | None = None
        self._started = False
        self._subscribed: set[str] = set()
        self._quotes: dict[str, OpraQuote] = {}
        self._instrument_symbols: dict[int, str] = {}
        self._pending_by_instrument: dict[int, tuple[float, float, int]] = {}
        self._last_error: str | None = None
        self._started_at: str | None = None
        self._records_received = 0

    def ensure_subscribed(self, symbols: Iterable[str]) -> None:
        normalized = sorted({_normalize_occ_symbol(symbol) for symbol in symbols if str(symbol).strip()})
        with self._lock:
            additions = [symbol for symbol in normalized if symbol not in self._subscribed]
            if not additions:
                return
            if self._client is None:
                self._client = self._live_factory(self._api_key)
                self._client.add_callback(self._on_record, self._on_exception)
            try:
                self._client.subscribe(
                    dataset=OPRA_DATASET,
                    schema=OPRA_SCHEMA,
                    symbols=additions,
                    stype_in="raw_symbol",
                    snapshot=True,
                )
                if not self._started:
                    self._client.start()
                    self._started = True
                    self._started_at = datetime.now(timezone.utc).isoformat()
                self._subscribed.update(additions)
                self._last_error = None
            except Exception as exc:
                self._last_error = f"{type(exc).__name__}: {exc}"
                raise

    def quotes(self, symbols: Iterable[str]) -> dict[str, OpraQuote]:
        wanted = {_normalize_occ_symbol(symbol) for symbol in symbols}
        with self._lock:
            return {symbol: quote for symbol, quote in self._quotes.items() if symbol in wanted}

    def status(self) -> dict[str, Any]:
        with self._lock:
            latest_ns = max((quote.event_time_ns for quote in self._quotes.values()), default=None)
            return {
                "provider": "DATABENTO",
                "dataset": OPRA_DATASET,
                "schema": OPRA_SCHEMA,
                "started": self._started,
                "started_at": self._started_at,
                "subscribed_symbol_count": len(self._subscribed),
                "mapped_instrument_count": len(self._instrument_symbols),
                "quote_count": len(self._quotes),
                "records_received": self._records_received,
                "latest_event_time_ms": latest_ns // 1_000_000 if latest_ns else None,
                "last_error": self._last_error,
            }

    def stop(self) -> None:
        with self._lock:
            client = self._client
            self._client = None
            self._started = False
        if client is not None:
            try:
                client.stop()
            except Exception:
                try:
                    client.terminate()
                except Exception:
                    pass

    def _on_exception(self, exc: Exception) -> None:
        with self._lock:
            self._last_error = f"{type(exc).__name__}: {exc}"

    def _on_record(self, record: Any) -> None:
        name = type(record).__name__.lower()
        with self._lock:
            self._records_received += 1
            if "symbolmapping" in name:
                instrument_id = _int_field(record, "instrument_id")
                symbol = _mapping_symbol(record)
                if instrument_id is not None and symbol:
                    normalized = _normalize_occ_symbol(symbol)
                    self._instrument_symbols[instrument_id] = normalized
                    pending = self._pending_by_instrument.pop(instrument_id, None)
                    if pending is not None:
                        self._store_quote(normalized, *pending)
                return
            if "cmbp" not in name and "mbp" not in name:
                return
            instrument_id = _int_field(record, "instrument_id")
            bid, ask = _bid_ask(record)
            event_ns = _int_field(record, "ts_event", "ts_recv")
            if instrument_id is None or bid is None or ask is None or event_ns is None or bid > ask:
                return
            symbol = self._instrument_symbols.get(instrument_id)
            if symbol is None:
                self._pending_by_instrument[instrument_id] = (bid, ask, event_ns)
                return
            self._store_quote(symbol, bid, ask, event_ns)

    def _store_quote(self, symbol: str, bid: float, ask: float, event_ns: int) -> None:
        self._quotes[symbol] = OpraQuote(
            symbol=symbol,
            bid=bid,
            ask=ask,
            event_time_ns=event_ns,
            received_monotonic=time.monotonic(),
        )


class NdxpDatabentoAdapter:
    """Use Databento for option NBBOs and Schwab for roster, spot, and broker truth."""

    mode = "DATABENTO_OPRA_SCHWAB_READ_ONLY"

    def __init__(
        self,
        repo_root: Path,
        *,
        schwab: NdxpSchwabAdapter | None = None,
        feed: DatabentoOpraFeed | None = None,
    ) -> None:
        self.schwab = schwab or NdxpSchwabAdapter(repo_root)
        self.feed = feed or DatabentoOpraFeed(os.environ.get("DATABENTO_API_KEY", ""))
        self.broker = self.schwab.broker
        self._selected_expiration: str | None = None
        self._cached_chain: dict[str, Any] | None = None
        self._last_chain_refresh = 0.0
        self._chain_refresh_seconds = float(
            os.environ.get("MGC_NDXP_CHAIN_REFRESH_SECONDS", SCHWAB_CHAIN_REFRESH_SECONDS)
        )

    def set_selected_expiration(self, expiration: str | None) -> None:
        self._selected_expiration = expiration or None

    def fetch_market(self, *, chain_symbol: str, quote_symbol: str) -> dict[str, Any]:
        started = time.monotonic()
        now = time.monotonic()
        fetch_chain = getattr(self.schwab, "fetch_chain", None)
        fetch_quote = getattr(self.schwab, "fetch_quote", None)
        if callable(fetch_chain) and callable(fetch_quote):
            if self._cached_chain is None or now - self._last_chain_refresh >= self._chain_refresh_seconds:
                self._cached_chain = fetch_chain(chain_symbol=chain_symbol)
                self._last_chain_refresh = now
            quote_payload = fetch_quote(quote_symbol=quote_symbol)
        else:  # Small injected fakes can continue exposing the original combined boundary.
            schwab_market = self.schwab.fetch_market(chain_symbol=chain_symbol, quote_symbol=quote_symbol)
            if self._cached_chain is None or now - self._last_chain_refresh >= self._chain_refresh_seconds:
                self._cached_chain = schwab_market.get("chain") or {}
                self._last_chain_refresh = now
            quote_payload = schwab_market.get("quote") or {}
        chain = deepcopy(self._cached_chain)
        expiration = self._selected_expiration or _first_expiration(chain)
        spot = _spot_from_quote(quote_payload) or _number(chain.get("underlyingPrice"))
        target_symbols = _bounded_symbols(chain, expiration=expiration, spot=spot)
        if target_symbols:
            self.feed.ensure_subscribed(target_symbols)
        quotes = self.feed.quotes(target_symbols)
        _overlay_databento_quotes(chain, selected_expiration=expiration, quotes=quotes)
        status = self.feed.status()
        status.update(
            {
                "selected_expiration": expiration,
                "target_symbol_count": len(target_symbols),
                "selected_quote_count": len(quotes),
                "source_label": "Databento OPRA NBBO · Schwab NDX spot",
            }
        )
        return {
            "chain": chain,
            "quote": quote_payload,
            "received_at": datetime.now(timezone.utc).isoformat(),
            "latency_ms": round((time.monotonic() - started) * 1000, 1),
            "databento": status,
            "market_source": status["source_label"],
            "option_quote_time_ms": max(
            (quote.event_time_ns // 1_000_000 for quote in quotes.values()),
            default=None,
            ),
        }

    def fetch_broker_truth(self) -> dict[str, Any]:
        return self.schwab.fetch_broker_truth()

    def access_check(self) -> dict[str, Any]:
        result = self.schwab.access_check()
        result["databento"] = self.feed.status()
        result["databento_opra_quote_received"] = bool(result["databento"].get("quote_count"))
        result["market_source"] = "Databento OPRA NBBO · Schwab NDX spot"
        return result

    def stop(self) -> None:
        self.feed.stop()


def _default_live_factory(api_key: str) -> Any:
    try:
        import databento as db
    except ImportError as exc:
        raise DatabentoConfigurationError(
            "The Databento client is not installed. Install the project with: pip install -e '.[databento]'"
        ) from exc
    return db.Live(key=api_key)


def _first_expiration(chain: dict[str, Any]) -> str | None:
    expirations: set[str] = set()
    for key in ("callExpDateMap", "putExpDateMap"):
        mapping = chain.get(key) if isinstance(chain.get(key), dict) else {}
        expirations.update(str(value).split(":", 1)[0] for value in mapping)
    return min(expirations) if expirations else None


def _spot_from_quote(payload: dict[str, Any]) -> float | None:
    row = next(iter(payload.values()), {}) if isinstance(payload, dict) else {}
    quote = row.get("quote") if isinstance(row, dict) and isinstance(row.get("quote"), dict) else row
    if not isinstance(quote, dict):
        return None
    return _number(quote.get("lastPrice")) or _number(quote.get("mark"))


def _bounded_symbols(chain: dict[str, Any], *, expiration: str | None, spot: float | None) -> list[str]:
    if not expiration:
        return []
    rows: dict[float, list[str]] = {}
    for key in ("callExpDateMap", "putExpDateMap"):
        expiration_map = chain.get(key) if isinstance(chain.get(key), dict) else {}
        for expiration_key, strike_map in expiration_map.items():
            if str(expiration_key).split(":", 1)[0] != expiration or not isinstance(strike_map, dict):
                continue
            for strike_key, contracts in strike_map.items():
                strike = _number(strike_key)
                if strike is None:
                    continue
                for contract in contracts if isinstance(contracts, list) else []:
                    symbol = contract.get("symbol") if isinstance(contract, dict) else None
                    if symbol:
                        rows.setdefault(strike, []).append(_normalize_occ_symbol(symbol))
    strikes = sorted(rows)
    if not strikes:
        return []
    center = spot if spot is not None else strikes[len(strikes) // 2]
    below = [strike for strike in strikes if strike <= center][-SUBSCRIPTION_STRIKES_EACH_SIDE:]
    above = [strike for strike in strikes if strike > center][:SUBSCRIPTION_STRIKES_EACH_SIDE]
    selected = set(below + above)
    return sorted({symbol for strike in selected for symbol in rows[strike]})


def _overlay_databento_quotes(
    chain: dict[str, Any],
    *,
    selected_expiration: str | None,
    quotes: dict[str, OpraQuote],
) -> None:
    for key in ("callExpDateMap", "putExpDateMap"):
        expiration_map = chain.get(key) if isinstance(chain.get(key), dict) else {}
        for expiration_key, strike_map in expiration_map.items():
            is_selected = str(expiration_key).split(":", 1)[0] == selected_expiration
            if not isinstance(strike_map, dict):
                continue
            for contracts in strike_map.values():
                for contract in contracts if isinstance(contracts, list) else []:
                    if not isinstance(contract, dict):
                        continue
                    quote = quotes.get(_normalize_occ_symbol(contract.get("symbol", ""))) if is_selected else None
                    contract["bid"] = quote.bid if quote else None
                    contract["ask"] = quote.ask if quote else None
                    contract["mark"] = round((quote.bid + quote.ask) / 2, 4) if quote else None
                    contract["quoteTimeInLong"] = quote.event_time_ns // 1_000_000 if quote else None
                    contract["quoteSource"] = "DATABENTO_OPRA_NBBO" if quote else "DATABENTO_PENDING"


def _normalize_occ_symbol(value: Any) -> str:
    return str(value or "").rstrip().upper()


def _bid_ask(record: Any) -> tuple[float | None, float | None]:
    direct_bid = _number(_field(record, "bid_px_00"))
    direct_ask = _number(_field(record, "ask_px_00"))
    if direct_bid is not None and direct_ask is not None:
        return _decode_price(direct_bid), _decode_price(direct_ask)
    levels = _field(record, "levels")
    try:
        level = levels[0]
    except (TypeError, IndexError, KeyError):
        return None, None
    bid = _number(_field(level, "bid_px"))
    ask = _number(_field(level, "ask_px"))
    return _decode_price(bid), _decode_price(ask)


def _decode_price(value: float | None) -> float | None:
    if value is None:
        return None
    decoded = value / _PRICE_SCALE if abs(value) > 1_000_000 else value
    return round(decoded, 9) if 0 <= decoded <= 1_000_000 else None


def _field(value: Any, name: str) -> Any:
    if isinstance(value, dict):
        return value.get(name)
    return getattr(value, name, None)


def _int_field(value: Any, *names: str) -> int | None:
    for name in names:
        raw = _field(value, name)
        try:
            return int(raw)
        except (TypeError, ValueError):
            continue
    return None


def _text_field(value: Any, *names: str) -> str | None:
    for name in names:
        raw = _field(value, name)
        if raw is not None and str(raw).strip():
            return str(raw)
    return None


def _mapping_symbol(record: Any) -> str | None:
    # With a raw_symbol -> instrument_id subscription, Databento places the OCC
    # symbol in stype_in_symbol and the numeric ID in stype_out_symbol. Accept the
    # reverse direction too, but never mistake a numeric mapping target for OCC.
    candidates = (
        _text_field(record, "stype_in_symbol"),
        _text_field(record, "stype_out_symbol"),
        _text_field(record, "raw_symbol", "symbol"),
    )
    return next((value for value in candidates if value and not value.strip().isdigit()), None)


def _number(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
