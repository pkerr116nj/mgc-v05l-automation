#!/usr/bin/env python3
"""Low-power Flask dashboard for a regime monitor kiosk.

The server keeps the existing HTTP JSON polling behavior in a background
worker and exposes the current snapshot through /data. The root route serves a
small self-contained dashboard that polls /data and updates only changed DOM
fields. The client update boundary is intentionally isolated so it can later be
fed by Server-Sent Events without changing the rendering code.
"""

from __future__ import annotations

import argparse
import json
import os
import threading
import time
import urllib.error
import urllib.request
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

from flask import Flask, Response, jsonify


VALID_REGIMES = {"LONG", "SHORT", "NO_TRADE"}
REGIME_COLORS = {
    "LONG": "#00e676",
    "SHORT": "#ff3333",
    "NO_TRADE": "#9e9e9e",
    "NO DATA": "#ffcc33",
}


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


class SnapshotStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._snapshot = RegimeSnapshot(
            regime="NO DATA",
            confidence=None,
            timestamp="",
            connection_status="STARTING",
            error=None,
            received_at=None,
        )

    def get(self) -> RegimeSnapshot:
        with self._lock:
            return self._snapshot

    def set(self, snapshot: RegimeSnapshot) -> None:
        with self._lock:
            self._snapshot = snapshot


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


def normalize_snapshot(payload: object) -> RegimeSnapshot:
    if not isinstance(payload, dict):
        raise ValueError("bad JSON")
    regime = str(payload.get("regime") or "").strip().upper()
    if regime not in VALID_REGIMES:
        regime = "NO DATA"
    confidence_raw = payload.get("confidence")
    confidence = None
    if confidence_raw not in (None, ""):
        try:
            confidence = float(confidence_raw)
        except (TypeError, ValueError):
            confidence = None
    timestamp = str(payload.get("timestamp") or "").strip()
    return RegimeSnapshot(
        regime=regime,
        confidence=confidence,
        timestamp=timestamp,
        connection_status="CONNECTED",
        error=None,
        received_at=utc_now_text(),
    )


def fetch_snapshot(url: str, timeout: float) -> RegimeSnapshot:
    request = urllib.request.Request(
        url,
        headers={
            "Accept": "application/json",
            "User-Agent": "regime-monitor-antix/2.0",
        },
    )
    with urllib.request.urlopen(request, timeout=timeout) as response:
        raw = response.read(1024 * 64)
    return normalize_snapshot(json.loads(raw.decode("utf-8")))


def polling_worker(
    *,
    url: str | None,
    interval: float,
    timeout: float,
    store: SnapshotStore,
    stop: threading.Event,
) -> None:
    while not stop.is_set():
        if not url:
            snapshot = RegimeSnapshot(
                regime="NO DATA",
                confidence=None,
                timestamp=utc_now_text(),
                connection_status="NO_ENDPOINT",
                error="REGIME_MONITOR_URL is not configured",
                received_at=utc_now_text(),
            )
        else:
            try:
                snapshot = fetch_snapshot(url, timeout)
            except (OSError, urllib.error.URLError, json.JSONDecodeError, ValueError) as exc:
                snapshot = RegimeSnapshot(
                    regime="NO DATA",
                    confidence=None,
                    timestamp=utc_now_text(),
                    connection_status="DISCONNECTED",
                    error=str(exc),
                    received_at=utc_now_text(),
                )
        store.set(snapshot)
        stop.wait(interval)


def create_app(*, store: SnapshotStore) -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def dashboard() -> Response:
        return Response(DASHBOARD_HTML, mimetype="text/html")

    @app.get("/data")
    def data() -> Response:
        response = jsonify(store.get().to_payload())
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
    #connection[data-state="DISCONNECTED"],
    #connection[data-state="NO_ENDPOINT"],
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
    parser.add_argument("--url", help="HTTP JSON endpoint. Overrides config/env.")
    parser.add_argument("--config", default="~/.config/regime-monitor/config.json")
    parser.add_argument("--interval", type=float, default=1.0, help="Upstream endpoint polling interval.")
    parser.add_argument("--timeout", type=float, default=0.8, help="Upstream endpoint timeout.")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    config = load_config(Path(args.config).expanduser())
    url = args.url or os.environ.get("REGIME_MONITOR_URL") or str(config.get("url") or "").strip() or None
    interval = float(config.get("interval", args.interval))
    timeout = float(config.get("timeout", args.timeout))
    host = str(config.get("host") or args.host)
    port = int(config.get("port") or args.port)
    store = SnapshotStore()
    stop_event = threading.Event()
    worker = threading.Thread(
        target=polling_worker,
        kwargs={
            "url": url,
            "interval": max(0.2, interval),
            "timeout": max(0.2, timeout),
            "store": store,
            "stop": stop_event,
        },
        daemon=True,
    )
    worker.start()
    app = create_app(store=store)
    try:
        app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    finally:
        stop_event.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
