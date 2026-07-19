# Ubuntu Regime Monitor Server

Ubuntu is the only application server. It owns Databento/regime ingestion,
normalization, `/data`, and the dashboard served at `/`.

The AntiX display device should not run Flask, Python, a config file, or any
local polling proxy. It only opens this Ubuntu dashboard in Chromium kiosk mode.

## JSON Endpoint Contract

The upstream regime source consumed by the Ubuntu app returns:

```json
{
  "regime": "LONG",
  "confidence": 0.82,
  "timestamp": "2026-07-19T09:30:00-04:00"
}
```

Allowed `regime` values are `LONG`, `SHORT`, and `NO_TRADE`. Invalid regimes
are rendered as `NO DATA`, preserving the existing display normalization.

## Install On Ubuntu

```bash
sudo apt update
sudo apt install -y python3 python3-flask
sudo mkdir -p /opt/regime-monitor-ubuntu
sudo cp regime_monitor.py config.json.example requirements.txt /opt/regime-monitor-ubuntu/
sudo chmod +x /opt/regime-monitor-ubuntu/regime_monitor.py
sudo mkdir -p /etc/regime-monitor
sudo cp config.json.example /etc/regime-monitor/config.json
```

Edit the upstream endpoint:

```bash
sudo nano /etc/regime-monitor/config.json
```

Example:

```json
{
  "url": "http://your-regime-source:8080/regime.json",
  "interval": 1.0,
  "timeout": 0.8,
  "host": "0.0.0.0",
  "port": 5000
}
```

## Exact Ubuntu Startup Command

```bash
python3 /opt/regime-monitor-ubuntu/regime_monitor.py --config /etc/regime-monitor/config.json --host 0.0.0.0 --port 5000
```

Then verify from another machine:

```bash
curl http://192.168.1.80:5000/data
```

## Optional Ubuntu systemd Service

```bash
sudo cp regime-monitor-ubuntu.service /etc/systemd/system/regime-monitor-ubuntu.service
sudo systemctl daemon-reload
sudo systemctl enable --now regime-monitor-ubuntu.service
```

## Dashboard Behavior

- Served directly by Ubuntu Flask at `http://192.168.1.80:5000/`.
- Browser polls `/data` every `250 ms`.
- Only changed fields are updated in the DOM.
- `handleSnapshotMessage(...)` is the future SSE migration boundary.
- No trading or regime calculation logic is changed by the dashboard.

## Files

```text
/opt/regime-monitor-ubuntu/
  regime_monitor.py
  config.json.example
  requirements.txt

/etc/regime-monitor/
  config.json
```
