"""Dependency-free server for the locked NDXP spread terminal."""

from __future__ import annotations

import argparse
import json
import math
import threading
import time
import webbrowser
from datetime import date, datetime, timedelta, timezone
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from .databento import NdxpDatabentoAdapter
from .orders import SpreadValidationError, TransmissionDisabledError
from .service import NdxpTerminalService


STATIC_ROOT = Path(__file__).with_name("static")
DEMO_STRIKE_INTERVAL = 10
DEMO_STRIKES_EACH_SIDE = 30
EASTERN = ZoneInfo("America/New_York")


class NdxpTerminalHandler(BaseHTTPRequestHandler):
    service: NdxpTerminalService

    def do_GET(self) -> None:  # noqa: N802
        if self.path == "/api/state":
            self._json(HTTPStatus.OK, self.service.snapshot())
            return
        if self.path == "/api/access-check":
            try:
                self._json(HTTPStatus.OK, self.service.access_check())
            except Exception as exc:
                self._json(HTTPStatus.SERVICE_UNAVAILABLE, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})
            return
        path = "/index.html" if self.path in {"/", ""} else self.path.split("?", 1)[0]
        candidate = (STATIC_ROOT / path.lstrip("/")).resolve()
        if STATIC_ROOT.resolve() not in candidate.parents or not candidate.is_file():
            self.send_error(HTTPStatus.NOT_FOUND)
            return
        content_type = "text/html; charset=utf-8"
        if candidate.suffix == ".css":
            content_type = "text/css; charset=utf-8"
        elif candidate.suffix == ".js":
            content_type = "application/javascript; charset=utf-8"
        data = candidate.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.send_header("Content-Security-Policy", "default-src 'self'; script-src 'self'; style-src 'self'; connect-src 'self'")
        self.end_headers()
        self.wfile.write(data)

    def do_POST(self) -> None:  # noqa: N802
        try:
            payload = self._read_json()
            if self.path == "/api/client-heartbeat":
                result = self.service.client_heartbeat(payload)
            elif self.path == "/api/selection":
                result = self.service.update_selection(payload)
            elif self.path == "/api/preview":
                result = self.service.preview(payload)
            elif self.path in {"/api/submit", "/api/cancel", "/api/replace"}:
                result = self.service.mutate(self.path.rsplit("/", 1)[-1], payload)
            else:
                self._json(HTTPStatus.NOT_FOUND, {"ok": False, "error": "Unknown endpoint."})
                return
            self._json(HTTPStatus.OK, result)
        except TransmissionDisabledError as exc:
            self._json(HTTPStatus.LOCKED, {"ok": False, "transmission_locked": True, "error": str(exc)})
        except (SpreadValidationError, ValueError, json.JSONDecodeError) as exc:
            self._json(HTTPStatus.BAD_REQUEST, {"ok": False, "error": str(exc)})
        except Exception as exc:
            self._json(HTTPStatus.INTERNAL_SERVER_ERROR, {"ok": False, "error": f"{type(exc).__name__}: {exc}"})

    def log_message(self, format: str, *args: Any) -> None:
        return

    def _read_json(self) -> dict[str, Any]:
        length = int(self.headers.get("Content-Length") or 0)
        if length > 64_000:
            raise ValueError("Request payload is too large.")
        payload = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
        if not isinstance(payload, dict):
            raise ValueError("Expected a JSON object.")
        return payload

    def _json(self, status: HTTPStatus, payload: dict[str, Any]) -> None:
        data = json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.send_header("X-Content-Type-Options", "nosniff")
        self.end_headers()
        self.wfile.write(data)


def run_server(
    *,
    repo_root: Path,
    host: str,
    port: int,
    open_browser: bool,
    demo: bool,
    databento: bool = False,
    allow_remote_demo: bool = False,
) -> None:
    loopback = host in {"127.0.0.1", "localhost", "::1"}
    if not loopback and not (demo and allow_remote_demo):
        raise ValueError(
            "A non-loopback bind is permitted only for the credential-free demo "
            "with --demo --teleport-demo."
        )
    adapter = DemoSchwabAdapter() if demo else (NdxpDatabentoAdapter(repo_root) if databento else None)
    service = NdxpTerminalService(repo_root, adapter=adapter)
    service.start()
    handler = type("BoundNdxpTerminalHandler", (NdxpTerminalHandler,), {"service": service})
    server = ThreadingHTTPServer((host, port), handler)
    url = f"http://{host}:{server.server_port}/"
    display_url = f"http://<MARS_LAN_IP>:{server.server_port}/" if not loopback else url
    print(
        json.dumps(
            {
                "url": display_url,
                "listen": url,
                "mode": "TELEPORT_DEMO" if not loopback else (
                    "DEMO" if demo else ("DATABENTO_OPRA_SCHWAB_READ_ONLY" if databento else "SCHWAB_LIVE_READ_ONLY")
                ),
                "schwab_credentials_loaded": False if not loopback else None,
                "transmission": "LOCKED",
            }
        )
    )
    if open_browser:
        threading.Timer(0.25, lambda: webbrowser.open(url)).start()
    try:
        server.serve_forever(poll_interval=0.25)
    finally:
        service.stop()
        server.server_close()


class _DemoBroker:
    def submit_order(self, *_args: Any, **_kwargs: Any) -> dict[str, Any]:
        raise AssertionError("The source lock must prevent demo submission.")

    cancel_order = submit_order
    replace_order = submit_order


class DemoSchwabAdapter:
    mode = "DEMO"
    broker = _DemoBroker()

    def fetch_market(self, *, chain_symbol: str, quote_symbol: str) -> dict[str, Any]:
        now = datetime.now(timezone.utc)
        spot = 29318 + math.sin(time.monotonic() / 5) * 8
        expirations = [_next_weekday(_demo_start_day(now), offset) for offset in range(3)]
        call_map: dict[str, Any] = {}
        put_map: dict[str, Any] = {}
        for day in expirations:
            days = max(0, (day - date.today()).days)
            calls: dict[str, Any] = {}
            puts: dict[str, Any] = {}
            center_strike = round(spot / DEMO_STRIKE_INTERVAL) * DEMO_STRIKE_INTERVAL
            first_strike = center_strike - DEMO_STRIKES_EACH_SIDE * DEMO_STRIKE_INTERVAL
            last_strike = center_strike + DEMO_STRIKES_EACH_SIDE * DEMO_STRIKE_INTERVAL
            for strike in range(first_strike, last_strike + DEMO_STRIKE_INTERVAL, DEMO_STRIKE_INTERVAL):
                expiry_at = datetime.combine(day, datetime.min.time().replace(hour=16), tzinfo=EASTERN).astimezone(timezone.utc)
                time_years = max((expiry_at - now).total_seconds(), 60) / (365.25 * 24 * 60 * 60)
                call_mid = _demo_black_price("CALL", spot, strike, time_years, 0.142)
                put_mid = _demo_black_price("PUT", spot, strike, time_years, 0.142)
                calls[f"{strike:.1f}"] = [_demo_contract(day, "C", strike, call_mid, now, spot)]
                puts[f"{strike:.1f}"] = [_demo_contract(day, "P", strike, put_mid, now, spot)]
            call_map[f"{day.isoformat()}:{days}"] = calls
            put_map[f"{day.isoformat()}:{days}"] = puts
        epoch_ms = int(now.timestamp() * 1000)
        return {
            "chain": {
                "symbol": chain_symbol,
                "underlyingPrice": round(spot, 2),
                "callExpDateMap": call_map,
                "putExpDateMap": put_map,
            },
            "quote": {quote_symbol: {"quote": {"lastPrice": round(spot, 2), "quoteTimeInLong": epoch_ms}}},
            "latency_ms": 42.0,
            "received_at": now.isoformat(),
        }

    def fetch_broker_truth(self) -> dict[str, Any]:
        now = datetime.now(timezone.utc).isoformat()
        expiration = _next_weekday(_demo_start_day(datetime.now(timezone.utc)), 0).strftime("%y%m%d")
        short_call = f"NDXP  {expiration}C29330000"
        long_call = f"NDXP  {expiration}C29340000"
        return {
            "account_numbers": [{"accountNumber": "12345678", "hashValue": "demo-account-hash"}],
            "accounts": [
                {
                    "securitiesAccount": {
                        "accountNumber": "12345678",
                        "type": "MARGIN",
                        "positions": [
                            {
                                "instrument": {"symbol": short_call, "assetType": "OPTION", "description": "Demo short call leg"},
                                "longQuantity": 0,
                                "shortQuantity": 20,
                                "averagePrice": 4.25,
                                "marketValue": -8500,
                            },
                            {
                                "instrument": {"symbol": long_call, "assetType": "OPTION", "description": "Demo protective call leg"},
                                "longQuantity": 20,
                                "shortQuantity": 0,
                                "averagePrice": 2.10,
                                "marketValue": 4200,
                            },
                        ],
                    }
                }
            ],
            "selected_account_hash": "demo-account-hash",
            "working_orders": [],
            "latency_ms": 65.0,
            "received_at": now,
        }

    def access_check(self) -> dict[str, Any]:
        return {
            "ok": True,
            "checked_at": datetime.now(timezone.utc).isoformat(),
            "account_count": 1,
            "account_and_trading_access": True,
            "ndx_chain_access": True,
            "market_latency_ms": 42,
            "broker_latency_ms": 65,
            "mutation_attempted": False,
            "demo": True,
        }


def _demo_contract(day: date, option_code: str, strike: int, mid: float, now: datetime, spot: float) -> dict[str, Any]:
    root = "NDXP  "
    symbol = f"{root}{day.strftime('%y%m%d')}{option_code}{strike * 1000:08d}"
    bid = max(0.01, mid - 0.10)
    ask = max(0.02, mid + 0.10)
    distance = abs(strike - spot)
    return {
        "symbol": symbol,
        "strikePrice": strike,
        "bid": round(bid, 2),
        "ask": round(ask, 2),
        "mark": round(mid, 2),
        "last": round(mid, 2),
        "netChange": 0.0,
        "percentChange": 0.0,
        "delta": round(max(0.03, 0.5 - distance / 300), 3) * (1 if option_code == "C" else -1),
        "gamma": 0.006,
        "theta": -1.25,
        "volatility": 14.2,
        "totalVolume": 120,
        "openInterest": 840,
        "quoteTimeInLong": int(now.timestamp() * 1000),
    }


def _demo_black_price(option_type: str, forward: float, strike: float, time_years: float, volatility: float) -> float:
    root_time = math.sqrt(time_years)
    d1 = (math.log(forward / strike) + 0.5 * volatility * volatility * time_years) / (volatility * root_time)
    d2 = d1 - volatility * root_time
    cdf = lambda value: 0.5 * (1.0 + math.erf(value / math.sqrt(2.0)))
    if option_type == "CALL":
        return max(0.01, forward * cdf(d1) - strike * cdf(d2))
    return max(0.01, strike * cdf(-d2) - forward * cdf(-d1))


def _next_weekday(start: date, offset: int) -> date:
    day = start
    found = -1
    while found < offset:
        if day.weekday() < 5:
            found += 1
            if found == offset:
                return day
        day += timedelta(days=1)
    return day


def _demo_start_day(now: datetime) -> date:
    eastern_now = now.astimezone(EASTERN)
    if eastern_now.time() >= datetime.min.time().replace(hour=16):
        return eastern_now.date() + timedelta(days=1)
    return eastern_now.date()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ndxp-terminal")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8810)
    parser.add_argument("--no-browser", action="store_true")
    parser.add_argument("--demo", action="store_true", help="Run with moving local sample data and no Schwab access.")
    parser.add_argument(
        "--databento",
        action="store_true",
        help="Use Databento OPRA NBBOs for options while retaining Schwab spot and broker truth.",
    )
    parser.add_argument(
        "--teleport-demo",
        action="store_true",
        help="Expose only demo mode to the trusted LAN/VPN; requires --demo and binds all interfaces by default.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.demo and args.databento:
        parser.error("--demo and --databento are mutually exclusive.")
    if args.teleport_demo and not args.demo:
        parser.error("--teleport-demo requires --demo; remote live-Schwab access is disabled.")
    host = "0.0.0.0" if args.teleport_demo and args.host == "127.0.0.1" else args.host
    repo_root = Path(__file__).resolve().parents[3]
    run_server(
        repo_root=repo_root,
        host=host,
        port=args.port,
        open_browser=not args.no_browser and not args.teleport_demo,
        demo=args.demo,
        databento=args.databento,
        allow_remote_demo=args.teleport_demo,
    )
    return 0
