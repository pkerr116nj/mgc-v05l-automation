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
from datetime import datetime, timezone
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
DEFAULT_CHART_RUNTIME_CANDLE_ROOT = (
    Path("outputs") / "track_b_execution_core" / "phase1_runtime_market_data"
)
DEFAULT_CHART_BAR_LIMIT = 72


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


@dataclass(frozen=True)
class ChartConfig:
    runtime_candle_root: Path
    symbol: str
    bar_limit: int = DEFAULT_CHART_BAR_LIMIT

    @property
    def completed_5m_path(self) -> Path:
        return self.runtime_candle_root / self.symbol / "5m" / "latest_runtime_candles.json"

    @property
    def one_minute_path(self) -> Path:
        return self.runtime_candle_root / self.symbol / "1m" / "latest_runtime_candles.json"


class CanonicalCandleSource:
    def __init__(self, *, config: ChartConfig) -> None:
        self.config = config

    def payload(self) -> dict[str, Any]:
        completed_payload = _read_json_payload(self.config.completed_5m_path)
        one_minute_payload = _read_json_payload(self.config.one_minute_path)
        completed_bars = _normalize_canonical_bars(completed_payload.get("bars"))
        latest_completed_end = _latest_bar_end(completed_bars)
        forming_bar = _forming_five_minute_bar(
            one_minute_payload.get("bars"),
            latest_completed_end=latest_completed_end,
        )
        bars_by_end = {bar["time"]: bar for bar in completed_bars}
        if forming_bar is not None:
            bars_by_end[forming_bar["time"]] = forming_bar
        bars = [bars_by_end[key] for key in sorted(bars_by_end, key=_parse_datetime_for_sort)]
        bars = bars[-max(1, int(self.config.bar_limit)) :]
        source_errors = [
            str(error)
            for error in (completed_payload.get("error"), one_minute_payload.get("error"))
            if error
        ]
        return {
            "schema_version": "regime_monitor_canonical_5m_chart_v1",
            "source": "execution_core_phase1_runtime_market_data",
            "source_detail": "completed 5m latest_runtime_candles plus current display bucket from canonical 1m latest_runtime_candles",
            "symbol": self.config.symbol,
            "timeframe": "5m",
            "bar_limit": self.config.bar_limit,
            "bar_count": len(bars),
            "generated_at": utc_now_text(),
            "source_5m_path": str(self.config.completed_5m_path),
            "source_1m_path": str(self.config.one_minute_path),
            "source_5m_generated_at": completed_payload.get("generated_at"),
            "source_1m_generated_at": one_minute_payload.get("generated_at"),
            "bars": bars,
            "error": "; ".join(source_errors) if source_errors else None,
        }


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


def _read_json_payload(path: Path) -> dict[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except FileNotFoundError:
        return {"bars": [], "error": f"missing canonical candle artifact: {path}"}
    except (OSError, json.JSONDecodeError) as exc:
        return {"bars": [], "error": f"unreadable canonical candle artifact: {exc}"}
    return payload if isinstance(payload, dict) else {"bars": [], "error": f"unexpected candle payload: {path}"}


def _normalize_canonical_bars(raw_bars: object) -> list[dict[str, Any]]:
    if not isinstance(raw_bars, list):
        return []
    normalized: list[dict[str, Any]] = []
    for raw in raw_bars:
        if not isinstance(raw, dict):
            continue
        bar = _normalize_canonical_bar(raw, default_completed=raw.get("completed") is not False)
        if bar is not None:
            normalized.append(bar)
    return sorted(normalized, key=lambda bar: _parse_datetime_for_sort(bar["time"]))


def _normalize_canonical_bar(raw: dict[str, Any], *, default_completed: bool) -> dict[str, Any] | None:
    bar_end = raw.get("bar_end") or raw.get("candle_timestamp") or raw.get("timestamp")
    if not bar_end:
        return None
    open_value = _float_or_none(raw.get("open"))
    high_value = _float_or_none(raw.get("high"))
    low_value = _float_or_none(raw.get("low"))
    close_value = _float_or_none(raw.get("close"))
    if None in (open_value, high_value, low_value, close_value):
        return None
    return {
        "time": str(bar_end),
        "start": None if raw.get("bar_start") is None else str(raw.get("bar_start")),
        "open": open_value,
        "high": high_value,
        "low": low_value,
        "close": close_value,
        "volume": _float_or_none(raw.get("volume")) or 0.0,
        "completed": bool(raw.get("completed", default_completed)),
        "source_bar_count": int(_float_or_none(raw.get("source_bar_count")) or 0),
    }


def _forming_five_minute_bar(raw_one_minute_bars: object, *, latest_completed_end: str | None) -> dict[str, Any] | None:
    one_minute_bars = _normalize_one_minute_bars(raw_one_minute_bars)
    if not one_minute_bars:
        return None
    latest_completed_dt = _parse_datetime_or_none(latest_completed_end)
    grouped: dict[datetime, list[dict[str, Any]]] = {}
    for bar in one_minute_bars:
        end = _parse_datetime_or_none(bar["time"])
        if end is None:
            continue
        bucket_end = _five_minute_bucket_end(end)
        if latest_completed_dt is not None and bucket_end <= latest_completed_dt:
            continue
        grouped.setdefault(bucket_end, []).append(bar)
    if not grouped:
        return None
    bucket_end = max(grouped)
    rows = sorted(grouped[bucket_end], key=lambda row: _parse_datetime_for_sort(row["time"]))
    if not rows:
        return None
    return {
        "time": bucket_end.isoformat(),
        "start": rows[0].get("start"),
        "open": rows[0]["open"],
        "high": max(row["high"] for row in rows),
        "low": min(row["low"] for row in rows),
        "close": rows[-1]["close"],
        "volume": sum(row.get("volume") or 0.0 for row in rows),
        "completed": False,
        "source_bar_count": len(rows),
    }


def _normalize_one_minute_bars(raw_bars: object) -> list[dict[str, Any]]:
    if not isinstance(raw_bars, list):
        return []
    bars: list[dict[str, Any]] = []
    for raw in raw_bars:
        if not isinstance(raw, dict):
            continue
        bar = _normalize_canonical_bar(raw, default_completed=True)
        if bar is not None:
            bars.append(bar)
    return sorted(bars, key=lambda bar: _parse_datetime_for_sort(bar["time"]))


def _latest_bar_end(bars: list[dict[str, Any]]) -> str | None:
    if not bars:
        return None
    return bars[-1]["time"]


def _five_minute_bucket_end(value: datetime) -> datetime:
    utc_value = value.astimezone(timezone.utc)
    epoch_minutes = int(utc_value.timestamp() // 60)
    bucket_epoch_minutes = ((epoch_minutes + 4) // 5) * 5
    return datetime.fromtimestamp(bucket_epoch_minutes * 60, tz=timezone.utc)


def _parse_datetime_for_sort(value: object) -> datetime:
    parsed = _parse_datetime_or_none(value)
    return parsed or datetime.min.replace(tzinfo=timezone.utc)


def _parse_datetime_or_none(value: object) -> datetime | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        text = str(value).strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _float_or_none(value: object) -> float | None:
    if value in (None, ""):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


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


def create_app(*, state: PriceRegimeState, candle_source: CanonicalCandleSource | None = None) -> Flask:
    app = Flask(__name__)

    @app.get("/")
    def dashboard() -> Response:
        return Response(DASHBOARD_HTML, mimetype="text/html")

    @app.get("/data")
    def data() -> Response:
        payload = state.snapshot().to_payload()
        if candle_source is not None:
            payload["chart"] = candle_source.payload()
        response = jsonify(payload)
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
      grid-template-rows: minmax(0, 0.64fr) minmax(180px, 0.28fr) auto;
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
    .chart-panel {
      width: 100%;
      min-height: 0;
      padding: 0 3vw 1vh;
    }
    #chart {
      display: block;
      width: 100%;
      height: 100%;
      min-height: 180px;
      background: #020202;
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
    <section class="chart-panel" aria-label="Five-minute candlestick chart">
      <canvas id="chart"></canvas>
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
    const chartCanvas = document.getElementById("chart");
    const chartContext = chartCanvas.getContext("2d");
    let lastChartPayload = null;
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
      updateChart(payload.chart || null);
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

    function updateChart(chart) {
      const bars = chart && Array.isArray(chart.bars) ? chart.bars : [];
      const key = JSON.stringify(bars);
      if (current.chartKey === key) return;
      current.chartKey = key;
      lastChartPayload = chart || { bars: [] };
      drawChart(lastChartPayload);
    }

    function resizeChartCanvas() {
      const rect = chartCanvas.getBoundingClientRect();
      const ratio = window.devicePixelRatio || 1;
      const width = Math.max(1, Math.floor(rect.width * ratio));
      const height = Math.max(1, Math.floor(rect.height * ratio));
      if (chartCanvas.width !== width || chartCanvas.height !== height) {
        chartCanvas.width = width;
        chartCanvas.height = height;
        chartContext.setTransform(ratio, 0, 0, ratio, 0, 0);
      }
    }

    function drawChart(chart) {
      resizeChartCanvas();
      const rect = chartCanvas.getBoundingClientRect();
      const width = rect.width;
      const height = rect.height;
      chartContext.clearRect(0, 0, width, height);
      chartContext.fillStyle = "#020202";
      chartContext.fillRect(0, 0, width, height);

      const bars = chart && Array.isArray(chart.bars) ? chart.bars : [];
      const valid = bars.filter((bar) =>
        Number.isFinite(bar.open) &&
        Number.isFinite(bar.high) &&
        Number.isFinite(bar.low) &&
        Number.isFinite(bar.close)
      );
      const left = 50;
      const right = 58;
      const top = 10;
      const bottom = 24;
      const plotWidth = Math.max(1, width - left - right);
      const plotHeight = Math.max(1, height - top - bottom);

      chartContext.strokeStyle = "#151515";
      chartContext.lineWidth = 1;
      for (let i = 0; i <= 4; i += 1) {
        const y = top + (plotHeight * i / 4);
        chartContext.beginPath();
        chartContext.moveTo(left, y);
        chartContext.lineTo(width - right, y);
        chartContext.stroke();
      }

      if (valid.length === 0) {
        chartContext.fillStyle = "#555";
        chartContext.font = "14px system-ui, sans-serif";
        chartContext.textAlign = "center";
        chartContext.fillText("Waiting for canonical 5m candles", width / 2, height / 2);
        return;
      }

      const highs = valid.map((bar) => bar.high);
      const lows = valid.map((bar) => bar.low);
      let minPrice = Math.min(...lows);
      let maxPrice = Math.max(...highs);
      const span = Math.max(maxPrice - minPrice, Math.abs(maxPrice) * 0.0005, 1);
      const padding = span * 0.08;
      minPrice -= padding;
      maxPrice += padding;
      const priceToY = (price) => top + ((maxPrice - price) / (maxPrice - minPrice)) * plotHeight;
      const candleStep = plotWidth / Math.max(valid.length, 1);
      const bodyWidth = Math.max(2, Math.min(16, candleStep * 0.62));

      chartContext.font = "12px system-ui, sans-serif";
      chartContext.textAlign = "right";
      chartContext.fillStyle = "#777";
      for (let i = 0; i <= 4; i += 1) {
        const price = maxPrice - ((maxPrice - minPrice) * i / 4);
        chartContext.fillText(formatPrice(price), left - 8, top + (plotHeight * i / 4) + 4);
      }

      valid.forEach((bar, index) => {
        const x = left + candleStep * index + candleStep / 2;
        const openY = priceToY(bar.open);
        const closeY = priceToY(bar.close);
        const highY = priceToY(bar.high);
        const lowY = priceToY(bar.low);
        const rising = bar.close >= bar.open;
        const color = rising ? "#00e676" : "#ff3333";
        chartContext.strokeStyle = color;
        chartContext.fillStyle = color;
        chartContext.globalAlpha = bar.completed === false ? 0.55 : 1;
        chartContext.beginPath();
        chartContext.moveTo(x, highY);
        chartContext.lineTo(x, lowY);
        chartContext.stroke();
        const bodyTop = Math.min(openY, closeY);
        const bodyHeight = Math.max(2, Math.abs(closeY - openY));
        chartContext.fillRect(x - bodyWidth / 2, bodyTop, bodyWidth, bodyHeight);
        chartContext.globalAlpha = 1;

        if (index % 12 === 0 || index === valid.length - 1) {
          chartContext.fillStyle = "#777";
          chartContext.textAlign = index === valid.length - 1 ? "right" : "center";
          chartContext.fillText(formatTimeLabel(bar.time), x, height - 7);
        }
      });
    }

    function formatPrice(value) {
      return Math.abs(value) >= 1000 ? value.toFixed(0) : value.toFixed(2);
    }

    function formatTimeLabel(value) {
      const date = new Date(value);
      if (Number.isNaN(date.getTime())) return "";
      return date.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    }

    window.addEventListener("resize", () => {
      resizeChartCanvas();
      drawChart(lastChartPayload || { bars: [] });
    });
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
    parser.add_argument("--chart-symbol", help="Canonical Phase-1 runtime candle symbol for the dashboard chart.")
    parser.add_argument("--chart-runtime-candle-root", type=Path, help="Canonical Phase-1 runtime candle root.")
    parser.add_argument("--chart-bar-limit", type=int, help="Maximum five-minute candles to display.")
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


def _default_chart_symbol(symbols: tuple[str, ...]) -> str:
    first = symbols[0] if symbols else "MBT.FUT"
    root = str(first).split(".", 1)[0].strip().upper()
    return root or "MBT"


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
    chart_symbol = str(args.chart_symbol or config.get("chart_symbol") or _default_chart_symbol(symbols)).strip().upper()
    chart_root_value = args.chart_runtime_candle_root or config.get("chart_runtime_candle_root")
    chart_root = Path(str(chart_root_value)).expanduser() if chart_root_value else DEFAULT_CHART_RUNTIME_CANDLE_ROOT
    chart_bar_limit = int(args.chart_bar_limit or config.get("chart_bar_limit", DEFAULT_CHART_BAR_LIMIT))
    host = str(config.get("host") or args.host)
    port = int(config.get("port") or args.port)
    state = PriceRegimeState()
    candle_source = CanonicalCandleSource(
        config=ChartConfig(
            runtime_candle_root=chart_root,
            symbol=chart_symbol,
            bar_limit=chart_bar_limit,
        )
    )
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
    app = create_app(state=state, candle_source=candle_source)
    try:
        app.run(host=host, port=port, debug=False, use_reloader=False, threaded=True)
    finally:
        stop_event.set()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
