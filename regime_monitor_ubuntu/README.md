# Ubuntu Regime Monitor Server

Ubuntu runs the only application server. This one Flask process owns:

- Databento Live ingestion
- the current 20-trade moving-average regime calculation
- the `/data` JSON endpoint
- the dashboard served at `/`
- a display-only candlestick chart sourced from canonical Phase-1 runtime
  candle artifacts

The AntiX display device does not run Flask, Python, configuration, Databento
ingestion, regime calculation, `/data`, or a local polling proxy. It only opens
the Ubuntu dashboard in Chromium kiosk mode.

## JSON Endpoint Contract

`GET /data` returns the current regime snapshot:

```json
{
  "regime": "LONG",
  "confidence": 0.0031,
  "timestamp": "14:30:04",
  "connection_status": "CONNECTED",
  "error": null,
  "received_at": "2026-07-19T18:30:04Z",
  "chart": {
    "schema_version": "regime_monitor_canonical_5m_chart_v1",
    "source": "execution_core_phase1_runtime_market_data",
    "symbol": "MBT",
    "timeframe": "5m",
    "bar_limit": 72,
    "bar_count": 72,
    "bars": []
  }
}
```

Regime values are `LONG`, `SHORT`, and `NO_TRADE`. Before the first usable
Databento price, or when the feed is unavailable, the endpoint reports
`NO_TRADE` with confidence `0`, preserving the existing monitor behavior.

## Databento Feed

Default subscription:

```python
client.subscribe(
    dataset="GLBX.MDP3",
    schema="trades",
    symbols=["MBT.FUT"],
    stype_in="parent",
)
```

The service preserves the current message handling:

- prefer `msg.px` when present
- otherwise use `msg.price`
- divide the integer Databento price by `1e9`
- keep the latest 20 prices
- compute `LONG` when latest price is above the simple average, otherwise
  `SHORT`
- confidence is `abs(latest_price - average) / average`, rounded to 4 decimals

## Candlestick Chart Source

The dashboard does not independently build completed five-minute history.
Completed chart bars are read from the existing canonical Phase-1 output:

```text
outputs/track_b_execution_core/phase1_runtime_market_data/<SYMBOL>/5m/latest_runtime_candles.json
```

That artifact is produced by `phase1_databento_live_runtime_candles.py`, which
uses the repository's canonical 1m-to-3m/5m OHLC conversion path. For a live
display of the currently forming five-minute candle, the monitor reads the
matching canonical 1m artifact and builds a display-only partial bucket from
1m rows newer than the last completed 5m bar:

```text
outputs/track_b_execution_core/phase1_runtime_market_data/<SYMBOL>/1m/latest_runtime_candles.json
```

The chart shows the latest 72 five-minute candles, roughly six hours. On page
reload or service restart, it immediately recovers from those canonical recent
bar artifacts instead of waiting for six hours of new data.

Path resolution is intentionally portable:

1. If `REGIME_MONITOR_REPO_ROOT` is set, chart candles are read from
   `$REGIME_MONITOR_REPO_ROOT/outputs/track_b_execution_core/phase1_runtime_market_data`.
2. Otherwise, the service derives the same relative path from the deployed app
   directory: `Path(__file__).resolve().parent / outputs / ...`.

The simple `/opt/regime-monitor-ubuntu` copy layout below installs only the
monitor app, config, and requirements. It does not include the repository
`outputs` tree, so production deployments that use that layout should set
`REGIME_MONITOR_REPO_ROOT` in `/etc/regime-monitor/regime-monitor.env`.

## Install On Ubuntu

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-flask
sudo mkdir -p /opt/mgc-v05l-automation
# Keep or deploy the repository checkout at /opt/mgc-v05l-automation so
# Phase-1 can publish outputs/track_b_execution_core/phase1_runtime_market_data.
sudo python3 -m pip install -r regime_monitor_ubuntu/requirements.txt
sudo mkdir -p /opt/regime-monitor-ubuntu
sudo cp regime_monitor_ubuntu/regime_monitor.py regime_monitor_ubuntu/config.json.example regime_monitor_ubuntu/requirements.txt /opt/regime-monitor-ubuntu/
sudo chmod +x /opt/regime-monitor-ubuntu/regime_monitor.py
sudo mkdir -p /etc/regime-monitor
sudo cp regime_monitor_ubuntu/config.json.example /etc/regime-monitor/config.json
```

Create the protected environment file for the Databento API key:

```bash
sudo install -m 600 -o root -g root /dev/null /etc/regime-monitor/regime-monitor.env
sudo sh -c 'printf "%s\n" "DATABENTO_API_KEY=your_databento_key_here" > /etc/regime-monitor/regime-monitor.env'
sudo sh -c 'printf "%s\n" "REGIME_MONITOR_REPO_ROOT=/opt/mgc-v05l-automation" >> /etc/regime-monitor/regime-monitor.env'
```

Edit the runtime configuration if needed:

```bash
sudo nano /etc/regime-monitor/config.json
```

Example:

```json
{
  "api_key_env": "DATABENTO_API_KEY",
  "dataset": "GLBX.MDP3",
  "schema": "trades",
  "symbols": ["MBT.FUT"],
  "stype_in": "parent",
  "reconnect_interval": 5.0,
  "repo_root_env": "REGIME_MONITOR_REPO_ROOT",
  "chart_symbol": "MBT",
  "chart_runtime_candle_root": "outputs/track_b_execution_core/phase1_runtime_market_data",
  "chart_bar_limit": 72,
  "host": "0.0.0.0",
  "port": 5000
}
```

## Exact Ubuntu Startup Command

```bash
set -a
. /etc/regime-monitor/regime-monitor.env
set +a
python3 /opt/regime-monitor-ubuntu/regime_monitor.py --config /etc/regime-monitor/config.json --host 0.0.0.0 --port 5000
```

Then verify from another machine:

```bash
curl http://192.168.1.80:5000/data
```

## Ubuntu systemd Service

```bash
sudo cp regime_monitor_ubuntu/regime-monitor-ubuntu.service /etc/systemd/system/regime-monitor-ubuntu.service
sudo systemctl daemon-reload
sudo systemctl enable --now regime-monitor-ubuntu.service
```

Useful checks:

```bash
systemctl status regime-monitor-ubuntu.service
journalctl -u regime-monitor-ubuntu.service -f
```

## Dashboard Behavior

- Served directly by Ubuntu Flask at `http://192.168.1.80:5000/`.
- Browser polls `/data` every `250 ms`.
- Only changed fields are updated in the DOM.
- The browser renders candlesticks with native canvas JavaScript, with no CDN
  dependency.
- The price axis auto-scales to the visible candle high/low with modest padding.
- Time labels are shown at roughly hourly intervals.
- `handleSnapshotMessage(...)` is the future SSE migration boundary.

## Files

```text
/opt/regime-monitor-ubuntu/
  regime_monitor.py
  config.json.example
  requirements.txt

/etc/regime-monitor/
  config.json
  regime-monitor.env
```
