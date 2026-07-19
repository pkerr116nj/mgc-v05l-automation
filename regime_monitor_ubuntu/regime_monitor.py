#!/usr/bin/env python3
"""Low-power Flask dashboard and Databento regime monitor.

One process owns Databento ingestion, the current regime state, the /data JSON
endpoint, and the dashboard served at /. The browser-side update boundary is
intentionally isolated so it can later be fed by Server-Sent Events without
changing the rendering code.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
from dataclasses import asdict, dataclass
from datetime import datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

from flask import Flask, Response, jsonify


VALID_REGIMES = {"LONG", "SHORT", "NO_TRADE"}
REGIME_COLORS = {
    "LONG": "#00e676",
    "SHORT": "#ff3333",
    "NO_TRADE": "#9e9e9e",
    "NO DATA": "#ffcc33",
}
EASTERN_TZ = ZoneInfo("America/New_York")


@dataclass(frozen=True)
class RegimeSnapshot:
    regime: str
    confidence: float | None
    timestamp: str
    connection_status: str
    error: str | None = None
    received_at: str | None = None

    def to_payload(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["colors"] = REGIME_COLORS
        return payload


@dataclass(frozen=True)
class DatabentoFeedConfig:
    api_key: str | None
    dataset: str = "GLBX.MDP3"
    schema: str = "trades"
    symbols: tuple[str, ...] = ("MBT.FUT",)
    stype_in: str = "parent"
    reconnect_interval: float = 5.0


class PriceRegimeState:
    def __init__(self, *, max_prices: int = 20) -> None:
        self._lock = threading.Lock()
        self._latest_price: float | None = None
        self._prices: list[float] = []
        self._max_prices = max_prices
        self._connection_status = "STARTING"
        self._error: str | None = None

    def mark_status(self, status: str, error: str | None = None) -> None:
        with self._lock:
            self._connection_status = status
            self._error = error

    def record_message(self, msg: object) -> None:
        price_value = getattr(msg, "px", None)
        if price_value is None:
            price_value = getattr(msg, "price", None)
        if price_value is None:
            return

        price = float(price_value) / 1e9
        with self._lock:
            self._latest_price = price
            self._prices.append(price)
            if len(self._prices) > self._max_prices:
                self._prices.pop(0)
            self._connection_status = "CONNECTED"
            self._error = None

    def snapshot(self) -> RegimeSnapshot:
        now = datetime.now(EASTERN_TZ)
        with self._lock:
            latest_price = self._latest_price
            prices = list(self._prices)
            connection_status = self._connection_status
            error = self._error

        timestamp = now.strftime("%H:%M:%S")
        if not latest_price:
            return RegimeSnapshot(
                regime="NO_TRADE",
                confidence=0,
                timestamp=timestamp,
                connection_status=connection_status,
                error=error,
                received_at=utc_now_text(),
            )

        ma = sum(prices) / len(prices) if prices else latest_price
        regime = "LONG" if latest_price > ma else "SHORT"
        confidence = abs(latest_price - ma) / ma if ma != 0 else 0
        return RegimeSnapshot(
            regime=regime,
            confidence=round(confidence, 4),
            timestamp=timestamp,
            connection_status=connection_status,
            error=error,
            received_at=utc_now_text(),
        )


def utc_now_text() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def load_config(path: Path) -> dict[str, object]:
    if not path.exists():
        return {}
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError):
        return {}
    return payload if isinstance(payload, dict) else {}


def run_databento_feed(
    *,
    config: DatabentoFeedConfig,
    state: PriceRegimeState,
    stop: threading.Event,
) -> None:
    if not config.api_key:
        state.mark_status("NO_API_KEY", "DATABENTO_API_KEY is not configured")
        return

    while not stop.is_set():
        client: Any | None = None
        try:
            import databento as db  # type: ignore[import-not-found]

            state.mark_status("CONNECTING")
            client = db.Live(key=config.api_key)
            client.subscribe(
                dataset=config.dataset,
                schema=config.schema,
                symbols=list(config.symbols),
                stype_in=config.stype_in,
            )
            state.mark_status("CONNECTED")
            for msg in client:
                if stop.is_set():
                    break
                print(msg, flush=True)
                state.record_message(msg)
            if not stop.is_set():
                state.mark_status("DISCONNECTED", "Databento live feed ended")
        except ModuleNotFoundError as exc:
            if exc.name == "databento":
                state.mark_status("DATABENTO_PACKAGE_MISSING", "Install the databento Python package")
                return
            raise
        except Exception as exc:
            state.mark_status("DISCONNECTED", str(exc))
        finally:
            close = getattr(client, "close", None)
            stop_client = getattr(client, "stop", None)
            if callable(close):
                try:
                    close()
                except Exception:
                    pass
            elif callable(stop_client):
                try:
                    stop_client()
                except Exception:
                    pass

        stop.wait(max(1.0, config.reconnect_interval))


def create_app(*, state: PriceRegimeState) -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def dashboard() -> Response:
        return Response(DASHBOARD_HTML, mimetype="text/html")

    @app.get("/data")
    def data() -> Response:
        response = jsonify(state.snapshot().to_payload())
        response.headers["Cache-Control"] = "no-store"
        return response

    return app


DASHBOARD_HTML = """<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Regime Monitor</title>
  <style>
    :root {
      color-scheme: dark;
      --bg: #000;
      --text: #f4f4f4;
      --muted: #777;
      --long: #00e676;
      --short: #ff3333;
      --no-trade: #9e9e9e;
      --no-data: #ffcc33;
    }
    * { box-sizing: border-box; }
    html, body {
      width: 100%;
      height: 100%;
      margin: 0;
      overflow: hidden;
      background: var(--bg);
      color: var(--text);
      cursor: none;
      font-family: system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
    }
    main {
      min-height: 100vh;
      display: grid;
      grid-template-rows: 1fr auto;
      background: #000;
    }
    .center {
      display: flex;
      flex-direction: column;
      align-items: center;
      justify-content: center;
      gap: clamp(1rem, 4vh, 3rem);
      padding: 4vw;
      text-align: center;
    }
    #regime {
      margin: 0;
      font-size: clamp(4rem, 18vw, 18rem);
      line-height: 0.9;
      letter-spacing: 0;
      font-weight: 800;
      color: var(--no-data);
      overflow-wrap: anywhere;
    }
    #confidence {
      margin: 0;
      font-size: clamp(1.6rem, 5vw, 5rem);
      line-height: 1.1;
      color: #ddd;
      font-weight: 500;
    }
    footer {
      display: grid;
      gap: 0.35rem;
      padding: 0 2vw 2vh;
      color: var(--muted);
      text-align: center;
      font-size: clamp(0.85rem, 1.6vw, 1.5rem);
      line-height: 1.25;
    }
    #connection[data-state="CONNECTED"] { color: var(--long); }
    #connection[data-state="CONNECTING"] { color: #ddd; }
    #connection[data-state="DISCONNECTED"],
    #connection[data-state="NO_API_KEY"],
    #connection[data-state="DATABENTO_PACKAGE_MISSING"],
    #connection[data-state="STARTING"] { color: var(--no-data); }
    .hidden { display: none; }
  </style>
</head>
<body>
  <main>
    <section class="center" aria-live="polite" aria-atomic="true">
      <h1 id="regime">NO DATA</h1>
      <p id="confidence">Confidence: --</p>
    </section>
    <footer>
      <div id="connection" data-state="STARTING">STARTING</div>
      <div id="timestamp">Timestamp unavailable</div>
      <div id="error" class="hidden"></div>
    </footer>
  </main>
  <script>
    const DATA_ENDPOINT = "/data";
    const POLL_INTERVAL_MS = 250;
    const colors = {
      LONG: "var(--long)",
      SHORT: "var(--short)",
      NO_TRADE: "var(--no-trade)",
      "NO DATA": "var(--no-data)",
    };
    const nodes = {
      regime: document.getElementById("regime"),
      confidence: document.getElementById("confidence"),
      timestamp: document.getElementById("timestamp"),
      connection: document.getElementById("connection"),
      error: document.getElementById("error"),
    };
    const current = {};

    function setText(key, value) {
      const next = value == null || value === "" ? "" : String(value);
      if (current[key] === next) return;
      current[key] = next;
      nodes[key].textContent = next;
    }

    function setConnectionStatus(status) {
      const next = status || "UNKNOWN";
      if (current.connectionStatus === next) return;
      current.connectionStatus = next;
      nodes.connection.dataset.state = next;
      nodes.connection.textContent = next;
    }

    function updateDashboard(payload) {
      const regime = payload.regime || "NO DATA";
      setText("regime", regime);
      const color = colors[regime] || colors["NO DATA"];
      if (current.regimeColor !== color) {
        current.regimeColor = color;
        nodes.regime.style.color = color;
      }
      const confidence = typeof payload.confidence === "number"
        ? `Confidence: ${(payload.confidence * 100).toFixed(1)}%`
        : "Confidence: --";
      setText("confidence", confidence);
      setText("timestamp", payload.timestamp || "Timestamp unavailable");
      setConnectionStatus(payload.connection_status || "UNKNOWN");
      const error = payload.error || "";
      if (current.error !== error) {
        current.error = error;
        nodes.error.textContent = error;
        nodes.error.classList.toggle("hidden", error === "");
      }
    }

    // Future SSE migration point: EventSource messages can call this function
    // directly without changing the render/update logic.
    function handleSnapshotMessage(data) {
      updateDashboard(data);
    }

    async function pollOnce() {
      try {
        const response = await fetch(DATA_ENDPOINT, { cache: "no-store" });
        if (!response.ok) throw new Error(`HTTP ${response.status}`);
        handleSnapshotMessage(await response.json());
      } catch (error) {
        handleSnapshotMessage({
          regime: "NO DATA",
          confidence: null,
          timestamp: new Date().toISOString(),
          connection_status: "DASHBOARD_DISCONNECTED",
          error: error.message,
        });
      }
    }

    setInterval(pollOnce, POLL_INTERVAL_MS);
    pollOnce();
  </script>
</body>
</html>
"""


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Flask-served regime monitor dashboard")
    parser.add_argument("--config", default="~/.config/regime-monitor/config.json")
    parser.add_argument("--api-key-env", help="Environment variable containing the Databento API key.")
    parser.add_argument("--dataset", help="Databento dataset.")
    parser.add_argument("--schema", help="Databento schema.")
    parser.add_argument("--symbols", help="Comma-separated Databento symbols.")
    parser.add_argument("--stype-in", help="Databento input symbol type.")
    parser.add_argument("--reconnect-interval", type=float, help="Databento reconnect interval in seconds.")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=5000)
    return parser.parse_args()


def _symbols_from_config(value: object) -> tuple[str, ...]:
    if isinstance(value, str):
        symbols = [part.strip() for part in value.split(",")]
    elif isinstance(value, list):
        symbols = [str(part).strip() for part in value]
    else:
        symbols = []
    return tuple(symbol for symbol in symbols if symbol) or ("MBT.FUT",)


def main() -> int:
    args = parse_args()
    config = load_config(Path(args.config).expanduser())
    api_key_env = str(args.api_key_env or config.get("api_key_env") or "DATABENTO_API_KEY").strip()
    api_key = os.environ.get(api_key_env) or str(config.get("api_key") or "").strip() or None
    dataset = str(args.dataset or config.get("dataset") or "GLBX.MDP3").strip()
    schema = str(args.schema or config.get("schema") or "trades").strip()
    symbol_value = args.symbols if args.symbols is not None else config.get("symbols")
    symbols = _symbols_from_config(symbol_value)
    stype_in = str(args.stype_in or config.get("stype_in") or "parent").strip()
    reconnect_interval = float(args.reconnect_interval or config.get("reconnect_interval", 5.0))
    host = str(config.get("host") or args.host)
    port = int(config.get("port") or args.port)
    state = PriceRegimeState()
    stop_event = threading.Event()
    worker = threading.Thread(
        target=run_databento_feed,
        kwargs={
            "config": DatabentoFeedConfig(
                api_key=api_key,
                dataset=dataset,
                schema=schema,
                symbols=symbols,
                stype_in=stype_in,
                reconnect_interval=reconnect_interval,
            ),
            "state": state,
            "stop": stop_event,
        },
        daemon=True,
    )
    worker.start()
    app = create_app(state=state)
    try:
        app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    finally:
        stop_event.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
