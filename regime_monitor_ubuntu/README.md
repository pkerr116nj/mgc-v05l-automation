# Ubuntu Regime Monitor Server

Ubuntu runs the only application server for this display. The monitor is
self-contained and does not read Magic repository outputs or Mac-hosted candle
artifacts.

Runtime layout:

```text
/opt/regime-monitor-ubuntu/     application code
/etc/regime-monitor/            configuration and secrets
/var/lib/regime-monitor/        bounded runtime candle state
```

One Flask process owns:

- Databento Live ingestion
- independent 20-trade moving-average regime calculation for MNQ, MES, MGC,
  and MBT
- independent current one-minute OHLC state for each instrument
- independent rolling five-minute OHLC chart state for each instrument
- the `/data` JSON endpoint
- the dashboard served at `/`

The AntiX display device does not run Flask, Python, configuration, Databento
ingestion, regime calculation, `/data`, or a local polling proxy. It only opens
the Ubuntu dashboard in Chromium kiosk mode.

## JSON Endpoint Contract

`GET /data` returns an instrument-keyed snapshot:

```json
{
  "schema_version": "regime_monitor_multi_instrument_v1",
  "generated_at": "2026-07-19T18:30:04Z",
  "routing": {
    "subscriptions": [
      {
        "requested_at": "2026-07-19T18:30:01Z",
        "dataset": "GLBX.MDP3",
        "schema": "trades",
        "symbols": ["MNQ.v.0", "MES.v.0", "MGC.v.0", "MBT.v.0"],
        "stype_in": "continuous"
      }
    ],
    "instrument_map": {
      "123456": "MNQ"
    },
    "unknown_instrument_ids": [],
    "records_seen": 12,
    "records_routed": 12,
    "records_rejected": 0,
    "first_records": []
  },
  "instruments": {
    "MNQ": {
      "instrument": "MNQ",
      "name": "Micro Nasdaq",
      "symbol": "MNQ.v.0",
      "regime": "LONG",
      "confidence": 0.0031,
      "timestamp": "14:30:04",
      "connection_status": "CONNECTED",
      "resolved_instrument_id": 123456,
      "last_source_symbol": "MNQ.v.0",
      "routing_status": "MAPPED",
      "routing_error": null,
      "error": null,
      "chart": {
        "schema_version": "regime_monitor_in_process_5m_chart_v1",
        "source": "regime_monitor_databento_live",
        "symbol": "MNQ",
        "timeframe": "5m",
        "bar_limit": 72,
        "bar_count": 72,
        "bars": []
      }
    }
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
    symbols=["MNQ.v.0", "MES.v.0", "MGC.v.0", "MBT.v.0"],
    stype_in="continuous",
)
```

The monitor uses Databento continuous front-month futures symbology. `MBT` is
the CME Micro Bitcoin futures root, so the Bitcoin panel subscribes to
`MBT.v.0`.

Databento symbol-mapping messages are handled before price records. The service
maintains a stable `instrument_id` to panel mapping, and an incoming price
record with an unknown or unmapped `instrument_id` is ignored instead of being
assigned to a fallback panel. Each `/data` panel includes the current
`resolved_instrument_id` and `last_source_symbol` so routing can be checked
from the kiosk browser.

The service preserves the current message handling:

- prefer `msg.px` when present
- otherwise use `msg.price`
- divide the integer Databento price by `1e9`
- keep the latest 20 prices
- compute `LONG` when latest price is above the simple average, otherwise
  `SHORT`
- confidence is `abs(latest_price - average) / average`, rounded to 4 decimals

## Candlestick Chart State

The chart state is built from the monitor's own Databento stream. Each accepted
trade is routed to one instrument and updates both that instrument's regime
price window and in-process candle state:

- current one-minute OHLC bar
- current five-minute OHLC bar
- latest completed five-minute bars

Each panel's `/data` chart payload returns the latest 72 five-minute candles,
including the currently forming five-minute candle when present. Each visible
price axis is scaled independently in the browser to the high/low range of that
panel's displayed candles with modest padding.

State is persisted atomically to:

```text
/var/lib/regime-monitor/candle_state.json
```

The state file atomically stores the bounded recent candle window and current
forming bars for all four instruments. It is used only for restart continuity,
so service restart or browser reload does not reset the charts to empty when
recent state exists. There is no general logging or historical archive
subsystem.

The persisted multi-instrument candle schema is versioned. Deploying the
instrument-id routing repair ignores older multi-panel candle state so a prior
cross-contaminated series is not reloaded into all four panels.

For local development only, the state directory can be overridden with
`REGIME_MONITOR_STATE_DIR` or the `state_dir` config value. Production should
use `/var/lib/regime-monitor`.

## Install On Ubuntu

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-flask
sudo python3 -m pip install -r regime_monitor_ubuntu/requirements.txt
sudo mkdir -p /opt/regime-monitor-ubuntu
sudo cp regime_monitor_ubuntu/regime_monitor.py regime_monitor_ubuntu/config.json.example regime_monitor_ubuntu/requirements.txt /opt/regime-monitor-ubuntu/
sudo chmod +x /opt/regime-monitor-ubuntu/regime_monitor.py
sudo mkdir -p /etc/regime-monitor
sudo cp regime_monitor_ubuntu/config.json.example /etc/regime-monitor/config.json
sudo install -d -m 750 /var/lib/regime-monitor
```

Create the protected environment file for the Databento API key:

```bash
sudo install -m 600 -o root -g root /dev/null /etc/regime-monitor/regime-monitor.env
sudo sh -c 'printf "%s\n" "DATABENTO_API_KEY=your_databento_key_here" > /etc/regime-monitor/regime-monitor.env'
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
  "stype_in": "continuous",
  "instruments": [
    {"key": "MNQ", "name": "Micro Nasdaq", "symbol": "MNQ.v.0"},
    {"key": "MES", "name": "Micro S&P", "symbol": "MES.v.0"},
    {"key": "MGC", "name": "Micro Gold", "symbol": "MGC.v.0"},
    {"key": "MBT", "name": "Micro Bitcoin", "symbol": "MBT.v.0"}
  ],
  "reconnect_interval": 5.0,
  "chart_bar_limit": 72,
  "state_dir_env": "REGIME_MONITOR_STATE_DIR",
  "state_dir": "/var/lib/regime-monitor",
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

The unit uses `StateDirectory=regime-monitor`, so systemd creates
`/var/lib/regime-monitor` before startup with bounded runtime-state
permissions.

Useful checks:

```bash
systemctl status regime-monitor-ubuntu.service
journalctl -u regime-monitor-ubuntu.service -f
```

## Dashboard Behavior

- Served directly by Ubuntu Flask at `http://192.168.1.80:5000/`.
- 2 columns by 2 rows at the 1920x1080 AntiX kiosk resolution.
- Panel order: MNQ top-left, MES top-right, MGC bottom-left, MBT bottom-right.
- Browser polls `/data` every `250 ms`.
- Only changed fields are updated in the DOM.
- The browser renders candlesticks with native canvas JavaScript, with no CDN
  dependency.
- The price axis auto-scales to the visible candle high/low with modest padding.
- Y-axis price labels render at 22px medium weight.
- X-axis time labels render at 20px medium weight.
- Time-label frequency is reduced automatically when needed so the 72-candle
  view stays readable on kiosk-width displays.
- `handleSnapshotMessage(...)` is the future SSE migration boundary.

## AntiX Kiosk Command

```bash
chromium --kiosk --noerrdialogs --disable-infobars http://192.168.1.80:5000/
```
