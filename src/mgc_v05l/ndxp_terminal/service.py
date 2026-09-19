"""Background-polling state service for the local NDXP terminal."""

from __future__ import annotations

import hashlib
import json
import os
import secrets
import threading
import time
from copy import deepcopy
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Protocol

from .analytics import derive_expiration_analytics
from .diagnostics import classify_diagnostics, ndxp_regular_session_open
from .orders import (
    LIVE_TRANSMISSION_COMPILED,
    LockedSchwabMutationGateway,
    NdxpSpreadRequest,
    SpreadValidationError,
    TransmissionDisabledError,
    build_vertical_order_payload,
    parse_option_symbol,
    validate_spread_request,
)
from .schwab import NdxpSchwabAdapter


class TerminalAdapter(Protocol):
    broker: Any

    def fetch_market(self, *, chain_symbol: str, quote_symbol: str) -> dict[str, Any]: ...

    def fetch_broker_truth(self) -> dict[str, Any]: ...

    def access_check(self) -> dict[str, Any]: ...

    def set_selected_expiration(self, expiration: str | None) -> None: ...

    def stop(self) -> None: ...


class NdxpTerminalService:
    def __init__(
        self,
        repo_root: Path,
        *,
        adapter: TerminalAdapter | None = None,
        market_interval_seconds: float = 1.0,
        broker_interval_seconds: float = 5.0,
        live_trading_requested: bool = False,
    ) -> None:
        self.repo_root = repo_root
        self.adapter = adapter or NdxpSchwabAdapter(repo_root)
        self.market_interval_seconds = market_interval_seconds
        self.broker_interval_seconds = broker_interval_seconds
        self.chain_symbol = os.environ.get("MGC_NDXP_CHAIN_SYMBOL", "$NDX")
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
        self._live_trading_requested = live_trading_requested
        self._preview_tokens: dict[str, tuple[float, str]] = {}
        self._consumed_preview_tokens: set[str] = set()
        self._diagnostic_path = repo_root / "outputs" / "ndxp_terminal" / "diagnostics.jsonl"
        self._mutation_gateway = LockedSchwabMutationGateway(
            self.adapter.broker,
            live_trading_requested=live_trading_requested,
        )

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
        stop_adapter = getattr(self.adapter, "stop", None)
        if callable(stop_adapter):
            stop_adapter()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            self._request_sequence += 1
            market = deepcopy(self._market)
            broker = deepcopy(self._broker)
            normalized_market = _normalize_market(market, selected_expiration=self._selected_expiration)
            resolved_expiration = normalized_market.get("selected_expiration")
            if resolved_expiration and resolved_expiration != self._selected_expiration:
                self._selected_expiration = resolved_expiration
                self._notify_selected_expiration()
                normalized_market = _normalize_market(market, selected_expiration=self._selected_expiration)
            normalized_broker = _normalize_broker(broker)
            now_wall = time.time()
            normalized_market["analytics"] = derive_expiration_analytics(
                spot=normalized_market.get("spot"),
                expiration=normalized_market.get("selected_expiration"),
                chain=normalized_market.get("selected_chain", {}),
                spot_quote_time_ms=normalized_market.get("spot_quote_time_ms"),
                allow_closed_snapshot=not ndxp_regular_session_open(
                    datetime.fromtimestamp(now_wall, tz=timezone.utc)
                ),
                now=datetime.fromtimestamp(now_wall, tz=timezone.utc),
            )
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
                now=datetime.fromtimestamp(now_wall, tz=timezone.utc),
            )
            return {
                "generated_at": datetime.now(timezone.utc).isoformat(),
                "request_sequence": self._request_sequence,
                "mode": str(getattr(self.adapter, "mode", "SCHWAB_LIVE_READ_ONLY")),
                "transmission": {
                    "compiled": LIVE_TRANSMISSION_COMPILED,
                    "launch_requested": self._live_trading_requested,
                    "runtime_enabled": os.environ.get("MGC_NDXP_LIVE_TRANSMISSION_ENABLED") == "1",
                    "effective_enabled": self._mutation_gateway.enabled,
                    "label": "TRANSMISSION LOCKED" if not self._mutation_gateway.enabled else "LIVE TRADING ENABLED",
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
            self._notify_selected_expiration()
        return self.snapshot()

    def _notify_selected_expiration(self) -> None:
        setter = getattr(self.adapter, "set_selected_expiration", None)
        if callable(setter):
            setter(self._selected_expiration)

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

    def preview(self, payload: dict[str, Any], *, allow_live_token: bool = False) -> dict[str, Any]:
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
        result = {
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
                "Broker submission requires the authorized-client, single-use live-trading gate.",
            ],
        }
        if allow_live_token and self._mutation_gateway.enabled:
            if request.account_hash != str(broker.get("selected_account_hash") or ""):
                raise SpreadValidationError("Live order preview is restricted to the currently selected Schwab account.")
            self._validate_live_contracts(request, market)
            self._assert_live_data_ready(market, request=request)
            token = secrets.token_urlsafe(32)
            with self._lock:
                self._purge_preview_tokens()
                self._preview_tokens[token] = (time.monotonic() + 60.0, _request_digest(request))
            result.update(
                transmission_enabled=True,
                preview_token=token,
                preview_token_expires_seconds=60,
            )
            result["checks"].append("Single-use live-trading token issued for this exact payload.")
        return result

    def access_check(self) -> dict[str, Any]:
        return self.adapter.access_check()

    def mutate(self, action: str, payload: dict[str, Any]) -> dict[str, Any]:
        with self._lock:
            broker = _normalize_broker(self._broker)
        selected_account_hash = str(broker.get("selected_account_hash") or "")
        requested_account_hash = str(payload.get("account_hash") or "").strip()
        if not selected_account_hash or requested_account_hash != selected_account_hash:
            raise SpreadValidationError("Live mutation is restricted to the currently selected Schwab account.")
        if action == "submit":
            request = NdxpSpreadRequest.from_json(payload)
            if not self._mutation_gateway.enabled:
                raise TransmissionDisabledError("Schwab submission is not enabled by every live-trading gate.")
            with self._lock:
                market = _normalize_market(self._market, selected_expiration=None)
            self._assert_live_data_ready(market, request=request)
            self._consume_preview_token(str(payload.get("preview_token") or ""), request)
            self._journal_mutation("SUBMIT_ATTEMPT", request=request)
            try:
                result = self._mutation_gateway.submit(request)
            except Exception as exc:
                self._journal_mutation("SUBMIT_UNKNOWN", request=request, error=f"{type(exc).__name__}: {exc}")
                raise
            self._journal_mutation("SUBMIT_ACK", request=request, result=result)
            return result
        if action == "cancel":
            broker_order_id = str(payload.get("broker_order_id") or "")
            self._journal_mutation("CANCEL_ATTEMPT", broker_order_id=broker_order_id)
            try:
                result = self._mutation_gateway.cancel(
                    account_hash=requested_account_hash,
                    broker_order_id=broker_order_id,
                )
            except Exception as exc:
                self._journal_mutation("CANCEL_UNKNOWN", broker_order_id=broker_order_id, error=f"{type(exc).__name__}: {exc}")
                raise
            self._journal_mutation("CANCEL_ACK", broker_order_id=broker_order_id, result=result)
            return result
        if action == "replace":
            request = NdxpSpreadRequest.from_json(payload)
            broker_order_id = str(payload.get("broker_order_id") or "").strip()
            working_order = next(
                (row for row in broker.get("working_orders", []) if row.get("order_id") == broker_order_id),
                None,
            )
            if working_order is None:
                raise SpreadValidationError("The order is not present in current Schwab working-order truth.")
            if not working_order.get("editable"):
                raise SpreadValidationError("The working order is not a recognized editable NDX vertical.")
            identity = (
                working_order.get("short_symbol"),
                working_order.get("long_symbol"),
                working_order.get("action"),
            )
            if identity != (request.short_symbol, request.long_symbol, request.action):
                raise SpreadValidationError("Replacement may change only the working order's quantity and price.")
            with self._lock:
                market = _normalize_market(self._market, selected_expiration=None)
            self._validate_live_contracts(request, market)
            self._assert_live_data_ready(market, request=request)
            self._journal_mutation("REPLACE_ATTEMPT", request=request, broker_order_id=broker_order_id)
            try:
                result = self._mutation_gateway.replace(
                    broker_order_id=broker_order_id,
                    request=request,
                )
            except Exception as exc:
                self._journal_mutation(
                    "REPLACE_UNKNOWN",
                    request=request,
                    broker_order_id=broker_order_id,
                    error=f"{type(exc).__name__}: {exc}",
                )
                raise
            self._journal_mutation(
                "REPLACE_ACK",
                request=request,
                broker_order_id=broker_order_id,
                result=result,
            )
            return {**result, "broker_order_id": result.get("broker_order_id") or broker_order_id}
        raise SpreadValidationError(f"Unknown mutation action: {action}.")

    def _consume_preview_token(self, token: str, request: NdxpSpreadRequest) -> None:
        if not token:
            raise SpreadValidationError("A current single-use preview token is required for submission.")
        with self._lock:
            self._purge_preview_tokens()
            if token in self._consumed_preview_tokens:
                raise SpreadValidationError("This preview token was already consumed; the order was not resubmitted.")
            record = self._preview_tokens.pop(token, None)
            if record is None or record[0] < time.monotonic():
                raise SpreadValidationError("The preview token is missing or expired; build a fresh preview.")
            if record[1] != _request_digest(request):
                raise SpreadValidationError("The order changed after preview; build a fresh preview.")
            self._consumed_preview_tokens.add(token)

    def _purge_preview_tokens(self) -> None:
        now = time.monotonic()
        self._preview_tokens = {token: record for token, record in self._preview_tokens.items() if record[0] >= now}

    def _validate_live_contracts(self, request: NdxpSpreadRequest, market: dict[str, Any]) -> None:
        selected_expiration = str(market.get("selected_expiration") or "")
        expected_expiration = selected_expiration.replace("-", "")[2:]
        short = parse_option_symbol(request.short_symbol)
        if not expected_expiration or short.expiration != expected_expiration:
            raise SpreadValidationError("The live spread must use the currently selected expiration.")
        selected_symbols = {
            row["symbol"]
            for side in market.get("selected_chain", {}).values()
            for row in side
        }
        if request.short_symbol not in selected_symbols or request.long_symbol not in selected_symbols:
            raise SpreadValidationError("Both live-order legs must be in the currently selected option chain.")

    def _assert_live_data_ready(self, market: dict[str, Any], *, request: NdxpSpreadRequest) -> None:
        with self._lock:
            market_error = self._market_error
            broker_error = self._broker_error
            last_market_success_wall = self._last_market_success_wall
        if market_error or broker_error:
            raise SpreadValidationError(f"Live order blocked by feed/broker error: {market_error or broker_error}")
        now_wall = time.time()
        poll_age_ms = None if last_market_success_wall is None else (now_wall - last_market_success_wall) * 1000
        if poll_age_ms is None or poll_age_ms > 5000:
            raise SpreadValidationError("Live order blocked because the most recent successful market poll is over five seconds old.")
        if ndxp_regular_session_open(datetime.fromtimestamp(now_wall, tz=timezone.utc)):
            source_age_ms = _source_age_ms(market.get("latest_source_time_ms"), now_wall)
            if source_age_ms is None or source_age_ms > 5000:
                raise SpreadValidationError("Live order blocked because option quotes are missing or over five seconds old.")
            exact_legs = {
                row.get("symbol"): row
                for side in market.get("selected_chain", {}).values()
                for row in side
                if row.get("symbol") in {request.short_symbol, request.long_symbol}
            }
            for symbol in (request.short_symbol, request.long_symbol):
                leg_age_ms = _source_age_ms(exact_legs.get(symbol, {}).get("quote_time_ms"), now_wall)
                if leg_age_ms is None or leg_age_ms > 5000:
                    raise SpreadValidationError(f"Live order blocked because quote data for {symbol} is missing or stale.")

    def _journal_mutation(
        self,
        event: str,
        *,
        request: NdxpSpreadRequest | None = None,
        broker_order_id: str | None = None,
        result: dict[str, Any] | None = None,
        error: str | None = None,
    ) -> None:
        record: dict[str, Any] = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "event": event,
        }
        if request is not None:
            record["account_fingerprint"] = hashlib.sha256(request.account_hash.encode("utf-8")).hexdigest()[:12]
            record["request"] = {
                "short_symbol": request.short_symbol,
                "long_symbol": request.long_symbol,
                "quantity": request.quantity,
                "limit_price": str(request.limit_price),
                "action": request.action,
            }
        if broker_order_id:
            record["broker_order_id"] = broker_order_id
        if result is not None:
            record["result"] = {
                key: result.get(key)
                for key in ("status_code", "broker_order_id", "location")
                if result.get(key) is not None
            }
        if error:
            record["error"] = error
        path = self.repo_root / "outputs" / "ndxp_terminal" / "mutations.jsonl"
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, separators=(",", ":"), default=str) + "\n")

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
                if not _raw_market_has_contracts(payload):
                    prior_chain = self._market.get("chain") if isinstance(self._market, dict) else None
                    if _raw_chain_has_contracts(prior_chain):
                        retained = deepcopy(payload) if isinstance(payload, dict) else {}
                        retained["chain"] = deepcopy(prior_chain)
                        self._market = retained
                        self._market_error = "Schwab returned no available option contracts; retaining the last valid chain."
                        return
                    self._market = payload
                    self._market_error = "Schwab returned no available NDX option expiration."
                    return
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
    latest_option_time_ms: int | None = None
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
                        latest_option_time_ms = source_ms if latest_option_time_ms is None else max(latest_option_time_ms, source_ms)
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
    spot, quote_time_ms, spot_source = _spot_observation(chain=chain, quote_row=quote_row)
    if quote_time_ms is not None:
        latest_source_time_ms = quote_time_ms if latest_source_time_ms is None else max(latest_source_time_ms, quote_time_ms)
    expirations = sorted(chains)
    selected = selected_expiration if selected_expiration in chains else (expirations[0] if expirations else None)
    return {
        "symbol": chain.get("symbol") or "NDX",
        "spot": spot,
        "received_at": payload.get("received_at"),
        "latency_ms": payload.get("latency_ms"),
        "latest_source_time_ms": payload.get("option_quote_time_ms") or latest_option_time_ms or latest_source_time_ms,
        "spot_quote_time_ms": quote_time_ms,
        "spot_source": spot_source,
        "market_source": payload.get("market_source") or "Schwab market data",
        "databento": payload.get("databento") if isinstance(payload.get("databento"), dict) else None,
        "expirations": expirations,
        "selected_expiration": selected,
        "selected_chain": chains.get(selected, {"CALL": [], "PUT": []}),
        "chains": chains,
    }


def _raw_market_has_contracts(payload: Any) -> bool:
    chain = payload.get("chain") if isinstance(payload, dict) else None
    return _raw_chain_has_contracts(chain)


def _raw_chain_has_contracts(chain: Any) -> bool:
    if not isinstance(chain, dict):
        return False
    for key in ("callExpDateMap", "putExpDateMap"):
        expiration_map = chain.get(key)
        if not isinstance(expiration_map, dict):
            continue
        for strike_map in expiration_map.values():
            if not isinstance(strike_map, dict):
                continue
            if any(isinstance(contracts, list) and bool(contracts) for contracts in strike_map.values()):
                return True
    return False


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
    working_orders = _normalize_orders(payload.get("working_orders"), editable=True)
    recent_orders = _normalize_orders(payload.get("recent_orders"), editable=False)
    return {
        "received_at": payload.get("received_at"),
        "latency_ms": payload.get("latency_ms"),
        "selected_account_hash": payload.get("selected_account_hash"),
        "accounts": accounts,
        "positions": positions,
        "working_orders": working_orders,
        "recent_orders": recent_orders,
    }


def _normalize_orders(value: Any, *, editable: bool) -> list[dict[str, Any]]:
    orders: list[dict[str, Any]] = []
    for order in value if isinstance(value, list) else []:
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
            short_leg = next(
                (leg for leg in legs if leg.get("instruction") in {"SELL_TO_OPEN", "BUY_TO_CLOSE"}),
                None,
            )
            long_leg = next(
                (leg for leg in legs if leg.get("instruction") in {"BUY_TO_OPEN", "SELL_TO_CLOSE"}),
                None,
            )
            action = (
                "OPEN"
                if short_leg and long_leg and short_leg.get("instruction") == "SELL_TO_OPEN"
                and long_leg.get("instruction") == "BUY_TO_OPEN"
                else "CLOSE"
                if short_leg and long_leg and short_leg.get("instruction") == "BUY_TO_CLOSE"
                and long_leg.get("instruction") == "SELL_TO_CLOSE"
                else None
            )
            leg_quantities: set[int] = set()
            for leg in legs:
                try:
                    quantity_value = float(leg.get("quantity"))
                except (TypeError, ValueError):
                    continue
                if quantity_value > 0 and quantity_value.is_integer():
                    leg_quantities.add(int(quantity_value))
            orders.append(
                {
                    "order_id": str(order.get("orderId") or ""),
                    "status": order.get("status"),
                    "order_type": order.get("orderType"),
                    "price": order.get("price"),
                    "entered_time": order.get("enteredTime"),
                    "close_time": order.get("closeTime"),
                    "filled_quantity": order.get("filledQuantity"),
                    "remaining_quantity": order.get("remainingQuantity"),
                    "action": action,
                    "short_symbol": short_leg.get("symbol") if short_leg else None,
                    "long_symbol": long_leg.get("symbol") if long_leg else None,
                    "quantity": next(iter(leg_quantities)) if len(leg_quantities) == 1 else None,
                    "editable": bool(editable and action and short_leg and long_leg and len(legs) == 2 and len(leg_quantities) == 1),
                    "legs": legs,
                }
            )
    return orders


def _request_digest(request: NdxpSpreadRequest) -> str:
    canonical = json.dumps(
        {
            "account_hash": request.account_hash,
            "short_symbol": request.short_symbol,
            "long_symbol": request.long_symbol,
            "quantity": request.quantity,
            "limit_price": str(request.limit_price),
            "action": request.action,
            "duration": request.duration,
            "session": request.session,
        },
        sort_keys=True,
        separators=(",", ":"),
    )
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


def _max_timestamp_ms(value: Any) -> int | None:
    candidates = [
        timestamp
        for key in (
            "quoteTimeInLong", "tradeTimeInLong", "regularMarketTradeTimeInLong",
            "quoteTime", "tradeTime", "regularMarketTradeTime",
        )
        if (timestamp := _timestamp_for_keys(value, (key,))) is not None
    ]
    return max(candidates) if candidates else None


def _timestamp_for_keys(value: Any, keys: tuple[str, ...]) -> int | None:
    by_key: dict[str, list[int]] = {key: [] for key in keys}

    def visit(candidate: Any) -> None:
        if not isinstance(candidate, dict):
            return
        for key, raw in candidate.items():
            if key in by_key:
                normalized = _timestamp_ms(raw)
                if normalized is not None:
                    by_key[key].append(normalized)
            elif isinstance(raw, dict):
                visit(raw)

    visit(value)
    for key in keys:
        if by_key[key]:
            return max(by_key[key])
    return None


def _timestamp_ms(value: Any) -> int | None:
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    if numeric <= 0:
        return None
    if numeric < 100_000_000_000:
        return numeric * 1_000
    if numeric >= 100_000_000_000_000_000:
        return numeric // 1_000_000
    if numeric >= 100_000_000_000_000:
        return numeric // 1_000
    return numeric


def _spot_observation(*, chain: dict[str, Any], quote_row: Any) -> tuple[float | None, int | None, str]:
    if isinstance(quote_row, dict):
        quote = quote_row.get("quote") if isinstance(quote_row.get("quote"), dict) else quote_row
        fields = (
            ("lastPrice", "last", ("tradeTimeInLong", "tradeTime", "regularMarketTradeTimeInLong", "regularMarketTradeTime", "quoteTimeInLong", "quoteTime")),
            ("mark", "mark", ("quoteTimeInLong", "quoteTime", "tradeTimeInLong", "tradeTime")),
            ("closePrice", "close", ("regularMarketTradeTimeInLong", "regularMarketTradeTime", "tradeTimeInLong", "tradeTime")),
        )
        for field, label, timestamp_keys in fields:
            value = _float_or_none(quote.get(field))
            if value is not None and value > 0:
                return value, _timestamp_for_keys(quote_row, timestamp_keys), f"Schwab index {label}"
    chain_spot = _float_or_none(chain.get("underlyingPrice"))
    if chain_spot is not None and chain_spot > 0:
        return chain_spot, _max_timestamp_ms(chain), "Schwab chain underlying"
    return None, None, "NDX value unavailable"


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
