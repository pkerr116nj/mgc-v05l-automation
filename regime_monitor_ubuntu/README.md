# Ubuntu Regime Monitor Server

Ubuntu runs the only application server. This one Flask process owns:

- Databento Live ingestion
- the current 20-trade moving-average regime calculation
- the `/data` JSON endpoint
- the dashboard served at `/`

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
  "received_at": "2026-07-19T18:30:04Z"
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
  "symbols": ["MBT.FUT"],
  "stype_in": "parent",
  "reconnect_interval": 5.0,
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
