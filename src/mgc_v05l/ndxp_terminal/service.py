"""Background-polling state service for the local NDXP terminal."""

from __future__ import annotations

import json
import os
import threading
import time
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from .diagnostics import classify_diagnostics
from .orders import (
    LIVE_TRANSMISSION_COMPILED,
    LockedSchwabMutationGateway,
    NdxpSpreadRequest,
    SpreadValidationError,
    TransmissionDisabledError,
    build_vertical_order_payload,
    validate_spread_request,
)
from .schwab import NdxpSchwabAdapter


class TerminalAdapter(Protocol):
    broker: Any

    def fetch_market(self, *, chain_symbol: str, quote_symbol: str) -> dict[str, Any]: ...

    def fetch_broker_truth(self) -> dict[str, Any]: ...

    def access_check(self) -> dict[str, Any]: ...


class NdxpTerminalService:
    def __init__(
        self,
        repo_root: Path,
        *,
        adapter: TerminalAdapter | None = None,
        market_interval_seconds: float = 1.0,
        broker_interval_seconds: float = 5.0,
    ) -> None:
        self.repo_root = repo_root
        self.adapter = adapter or NdxpSchwabAdapter(repo_root)
        self.market_interval_seconds = market_interval_seconds
        self.broker_interval_seconds = broker_interval_seconds
        self.chain_symbol = os.environ.get("MGC_NDXP_CHAIN_SYMBOL", "NDX")
        self.quote_symbol = os.environ.get("MGC_NDXP_QUOTE_SYMBOL", "$NDX")
        self._lock = threading.RLock()
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._market: dict[str, Any] | None = None
        self._broker: dict[str, Any] | None = None
        self._market_error: str | None = None
        self._broker_error: str | None = None
        self._last_market_attempt = 0.0
        self._last_broker_attempt = 0.0
        self._last_market_success_wall: float | None = None
        self._last_worker_tick: float | None = None
        self._max_worker_gap_ms: float | None = None
        self._last_client_heartbeat: float | None = None
        self._last_client_gap_ms: float | None = None
        self._request_sequence = 0
        self._selected_expiration: str | None = None
        self._selected_option_type = "CALL"
        self._diagnostic_path = repo_root / "outputs" / "ndxp_terminal" / "diagnostics.jsonl"
        self._mutation_gateway = LockedSchwabMutationGateway(self.adapter.broker)

    def start(self) -> None:
        with self._lock:
            if self._thread and self._thread.is_alive():
                return
            self._stop.clear()
            self._thread = threading.Thread(target=self._poll_loop, name="ndxp-schwab-poller", daemon=True)
            self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread:
            self._thread.join(timeout=3)

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._request_sequence += 1
            market = deepcopy(self._market)
            broker = deepcopy(self._broker)
            normalized_market = _normalize_market(market, selected_expiration=self._selected_expiration)
            if self._selected_expiration is None and normalized_market["expirations"]:
                self._selected_expiration = normalized_market["expirations"][0]
                normalized_market = _normalize_market(market, selected_expiration=self._selected_expiration)
            normalized_broker = _normalize_broker(broker)
            now_wall = time.time()
            source_age_ms = _source_age_ms(normalized_market.get("latest_source_time_ms"), now_wall)
            market_poll_age_ms = (
                (now_wall - self._last_market_success_wall) * 1000 if self._last_market_success_wall is not None else None
            )
            diagnostics = classify_diagnostics(
                market=market,
                broker=broker,
                market_error=self._market_error,
                broker_error=self._broker_error,
                worker_gap_ms=self._max_worker_gap_ms,
                client_gap_ms=self._last_client_gap_ms,
                source_age_ms=source_age_ms,
                market_poll_age_ms=market_poll_age_ms,
            )
            return {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "request_sequence": self._request_sequence,
                "mode": str(getattr(self.adapter, "mode", "SCHWAB_LIVE_READ_ONLY")),
                "transmission": {
                    "compiled": LIVE_TRANSMISSION_COMPILED,
                    "runtime_enabled": os.environ.get("MGC_NDXP_LIVE_TRANSMISSION_ENABLED") == "1",
                    "effective_enabled": self._mutation_gateway.enabled,
                    "label": "TRANSMISSION LOCKED" if not self._mutation_gateway.enabled else "LIVE PILOT ENABLED",
                },
                "selection": {
                    "expiration": self._selected_expiration,
                    "option_type": self._selected_option_type,
                },
                "market": normalized_market,
                "broker": normalized_broker,
                "diagnostics": diagnostics,
                "errors": {"market": self._market_error, "broker": self._broker_error},
            }

    def update_selection(self, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            expiration = str(payload.get("expiration") or "").strip()
            option_type = str(payload.get("option_type") or self._selected_option_type).strip().upper()
            expirations = _normalize_market(self._market, selected_expiration=None)["expirations"]
            if expiration and expiration not in expirations:
                raise SpreadValidationError("Selected expiration is not present in the current Schwab chain.")
            if option_type not in {"CALL", "PUT"}:
                raise SpreadValidationError("option_type must be CALL or PUT.")
            self._selected_expiration = expiration or self._selected_expiration
            self._selected_option_type = option_type
        return self.snapshot()

    def client_heartbeat(self, payload: dict[str, Any]) -> dict[str, Any]:
        now = time.monotonic()
        with self._lock:
            observed = None if self._last_client_heartbeat is None else (now - self._last_client_heartbeat) * 1000
            reported = _float_or_none(payload.get("interval_ms"))
            self._last_client_gap_ms = max(value for value in (observed, reported) if value is not None) if any(
                value is not None for value in (observed, reported)
            ) else None
            self._last_client_heartbeat = now
        return {"ok": True, "server_time": datetime.now(timezone.utc).isoformat()}

    def preview(self, payload: dict[str, Any]) -> dict[str, Any]:
        request = NdxpSpreadRequest.from_json(payload)
        with self._lock:
            market = _normalize_market(self._market, selected_expiration=None)
            broker = _normalize_broker(self._broker)
        chain_symbols = {row["symbol"] for expiry in market["chains"].values() for side in expiry.values() for row in side}
        if request.short_symbol not in chain_symbols or request.long_symbol not in chain_symbols:
            raise SpreadValidationError("Both option symbols must be present in the latest Schwab option chain.")
        account_hashes = {row["hash"] for row in broker["accounts"]}
        if request.account_hash not in account_hashes:
            raise SpreadValidationError("The selected account hash is not present in current Schwab account truth.")
        if request.action == "CLOSE":
            positions = {
                (row.get("account_hash"), row.get("symbol")): row
                for row in broker["positions"]
            }
            short_position = positions.get((request.account_hash, request.short_symbol), {})
            long_position = positions.get((request.account_hash, request.long_symbol), {})
            if float(short_position.get("short_quantity") or 0) < request.quantity:
                raise SpreadValidationError("The selected account does not hold enough of the exact short leg to close.")
            if float(long_position.get("long_quantity") or 0) < request.quantity:
                raise SpreadValidationError("The selected account does not hold enough of the exact protective long leg to close.")
        risk = validate_spread_request(request)
        return {
            "ok": True,
            "preview_only": True,
            "transmission_enabled": False,
            "summary": risk,
            "order_payload": build_vertical_order_payload(request),
            "checks": [
                "Both legs were found in the current Schwab chain.",
                "The account was found in current Schwab account truth.",
                "The spread is an exact 10-point defined-risk NDX/NDXP vertical.",
                f"The order is NORMAL session, DAY duration, and {'NET_CREDIT' if request.action == 'OPEN' else 'NET_DEBIT'} priced.",
                "Broker transmission remains source-locked.",
            ],
        }

    def access_check(self) -> dict[str, Any]:
        return self.adapter.access_check()

    def mutate(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        if action == "submit":
            return self._mutation_gateway.submit(NdxpSpreadRequest.from_json(payload))
        if action == "cancel":
            return self._mutation_gateway.cancel(
                account_hash=str(payload.get("account_hash") or ""),
                broker_order_id=str(payload.get("broker_order_id") or ""),
            )
        if action == "replace":
            return self._mutation_gateway.replace(
                broker_order_id=str(payload.get("broker_order_id") or ""),
                request=NdxpSpreadRequest.from_json(payload),
            )
        raise SpreadValidationError(f"Unknown mutation action: {action}.")

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            tick = time.monotonic()
            with self._lock:
                if self._last_worker_tick is not None:
                    gap = (tick - self._last_worker_tick) * 1000
                    self._max_worker_gap_ms = gap if self._max_worker_gap_ms is None else max(gap, self._max_worker_gap_ms * 0.9)
                self._last_worker_tick = tick
            if tick - self._last_market_attempt >= self.market_interval_seconds:
                self._last_market_attempt = tick
                self._poll_market()
            if tick - self._last_broker_attempt >= self.broker_interval_seconds:
                self._last_broker_attempt = tick
                self._poll_broker()
            self._stop.wait(0.1)

    def _poll_market(self) -> None:
        try:
            payload = self.adapter.fetch_market(chain_symbol=self.chain_symbol, quote_symbol=self.quote_symbol)
            with self._lock:
                self._market = payload
                self._market_error = None
                self._last_market_success_wall = time.time()
        except Exception as exc:
            with self._lock:
                self._market_error = f"{type(exc).__name__}: {exc}"

    def _poll_broker(self) -> None:
        try:
            payload = self.adapter.fetch_broker_truth()
            with self._lock:
                self._broker = payload
                self._broker_error = None
        except Exception as exc:
            with self._lock:
                self._broker_error = f"{type(exc).__name__}: {exc}"
        self._persist_diagnostics()

    def _persist_diagnostics(self) -> None:
        try:
            snapshot = self.snapshot()
            record = {
                "generated_at": snapshot["generated_at"],
                "diagnostics": snapshot["diagnostics"],
                "errors": snapshot["errors"],
            }
            self._diagnostic_path.parent.mkdir(parents=True, exist_ok=True)
            with self._diagnostic_path.open("a", encoding="utf-8") as handle:
                handle.write(json.dumps(record, sort_keys=True) + "\n")
        except Exception:
            return


def _normalize_market(payload: dict[str, Any] | None, *, selected_expiration: str | None) -> dict[str, Any]:
    payload = payload or {}
    chain = payload.get("chain") if isinstance(payload.get("chain"), dict) else {}
    quote_payload = payload.get("quote") if isinstance(payload.get("quote"), dict) else {}
    chains: dict[str, dict[str, list[dict[str, Any]]]] = {}
    latest_source_time_ms: int | None = None
    for side, source_key in (("CALL", "callExpDateMap"), ("PUT", "putExpDateMap")):
        expiration_map = chain.get(source_key) if isinstance(chain.get(source_key), dict) else {}
        for expiration_key, strike_map in expiration_map.items():
            expiration = str(expiration_key).split(":", 1)[0]
            if not isinstance(strike_map, dict):
                continue
            rows = chains.setdefault(expiration, {"CALL": [], "PUT": []})[side]
            for strike_key, contracts in strike_map.items():
                for contract in contracts if isinstance(contracts, list) else []:
                    if not isinstance(contract, dict) or not contract.get("symbol"):
                        continue
                    bid = _float_or_none(contract.get("bid"))
                    ask = _float_or_none(contract.get("ask"))
                    source_ms = _max_timestamp_ms(contract)
                    if source_ms is not None:
                        latest_source_time_ms = source_ms if latest_source_time_ms is None else max(latest_source_time_ms, source_ms)
                    rows.append(
                        {
                            "symbol": str(contract["symbol"]).strip().upper(),
                            "strike": _float_or_none(contract.get("strikePrice")) or _float_or_none(strike_key),
                            "bid": bid,
                            "ask": ask,
                            "mid": round((bid + ask) / 2, 2) if bid is not None and ask is not None else None,
                            "mark": _float_or_none(contract.get("mark")),
                            "last": _float_or_none(contract.get("last")),
                            "net_change": _float_or_none(contract.get("netChange")),
                            "percent_change": _float_or_none(contract.get("percentChange")),
                            "delta": _float_or_none(contract.get("delta")),
                            "gamma": _float_or_none(contract.get("gamma")),
                            "theta": _float_or_none(contract.get("theta")),
                            "iv": _float_or_none(contract.get("volatility")),
                            "volume": contract.get("totalVolume"),
                            "open_interest": contract.get("openInterest"),
                            "quote_time_ms": source_ms,
                        }
                    )
    for expiration in chains:
        chains[expiration]["CALL"].sort(key=lambda row: row.get("strike") or 0)
        chains[expiration]["PUT"].sort(key=lambda row: row.get("strike") or 0)
    quote_row = next(iter(quote_payload.values()), {}) if quote_payload else {}
    if isinstance(quote_row, dict):
        underlying_quote = quote_row.get("quote") if isinstance(quote_row.get("quote"), dict) else quote_row
        quote_time_ms = _max_timestamp_ms(underlying_quote)
        if quote_time_ms is not None:
            latest_source_time_ms = quote_time_ms if latest_source_time_ms is None else max(latest_source_time_ms, quote_time_ms)
    else:
        underlying_quote = {}
    spot = (
        _float_or_none(chain.get("underlyingPrice"))
        or _float_or_none(underlying_quote.get("lastPrice"))
        or _float_or_none(underlying_quote.get("mark"))
    )
    expirations = sorted(chains)
    selected = selected_expiration if selected_expiration in chains else (expirations[0] if expirations else None)
    return {
        "symbol": chain.get("symbol") or "NDX",
        "spot": spot,
        "received_at": payload.get("received_at"),
        "latency_ms": payload.get("latency_ms"),
        "latest_source_time_ms": latest_source_time_ms,
        "expirations": expirations,
        "selected_expiration": selected,
        "selected_chain": chains.get(selected, {"CALL": [], "PUT": []}),
        "chains": chains,
    }


def _normalize_broker(payload: dict[str, Any] | None) -> dict[str, Any]:
    payload = payload or {}
    account_index = {
        str(row.get("hashValue") or ""): str(row.get("accountNumber") or "")
        for row in payload.get("account_numbers", [])
        if isinstance(row, dict) and row.get("hashValue")
    }
    accounts: list[dict[str, Any]] = []
    positions: list[dict[str, Any]] = []
    for account_wrapper in payload.get("accounts", []):
        if not isinstance(account_wrapper, dict):
            continue
        account = account_wrapper.get("securitiesAccount") if isinstance(account_wrapper.get("securitiesAccount"), dict) else account_wrapper
        account_number = str(account.get("accountNumber") or "")
        account_hash = next((key for key, value in account_index.items() if value == account_number), "")
        accounts.append({"hash": account_hash, "number_masked": _mask(account_number), "type": account.get("type")})
        for position in account.get("positions", []) if isinstance(account.get("positions"), list) else []:
            instrument = position.get("instrument") if isinstance(position.get("instrument"), dict) else {}
            symbol = str(instrument.get("symbol") or "").strip().upper()
            if not symbol.startswith("NDX"):
                continue
            positions.append(
                {
                    "account_hash": account_hash,
                    "symbol": symbol,
                    "description": instrument.get("description"),
                    "asset_type": instrument.get("assetType"),
                    "long_quantity": position.get("longQuantity"),
                    "short_quantity": position.get("shortQuantity"),
                    "average_price": position.get("averagePrice"),
                    "market_value": position.get("marketValue"),
                }
            )
    orders = []
    for order in payload.get("working_orders", []):
        if not isinstance(order, dict):
            continue
        legs = []
        for leg in order.get("orderLegCollection", []) if isinstance(order.get("orderLegCollection"), list) else []:
            instrument = leg.get("instrument") if isinstance(leg.get("instrument"), dict) else {}
            if str(instrument.get("symbol") or "").strip().upper().startswith("NDX"):
                legs.append(
                    {
                        "instruction": leg.get("instruction"),
                        "quantity": leg.get("quantity"),
                        "symbol": instrument.get("symbol"),
                    }
                )
        if legs:
            orders.append(
                {
                    "order_id": str(order.get("orderId") or ""),
                    "status": order.get("status"),
                    "order_type": order.get("orderType"),
                    "price": order.get("price"),
                    "entered_time": order.get("enteredTime"),
                    "legs": legs,
                }
            )
    return {
        "received_at": payload.get("received_at"),
        "latency_ms": payload.get("latency_ms"),
        "selected_account_hash": payload.get("selected_account_hash"),
        "accounts": accounts,
        "positions": positions,
        "working_orders": orders,
    }


def _max_timestamp_ms(value: Any) -> int | None:
    if not isinstance(value, dict):
        return None
    candidates = []
    for key in ("quoteTimeInLong", "tradeTimeInLong", "regularMarketTradeTimeInLong"):
        raw = value.get(key)
        try:
            numeric = int(raw)
        except (TypeError, ValueError):
            continue
        if numeric > 0:
            candidates.append(numeric)
    return max(candidates) if candidates else None


def _source_age_ms(source_ms: Any, now_wall: float) -> float | None:
    try:
        return max(0.0, now_wall * 1000 - float(source_ms))
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mask(value: str) -> str:
    return f"••••{value[-4:]}" if value else "Unknown"
