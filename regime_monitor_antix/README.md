# AntiX Regime Monitor

Low-power fullscreen regime monitor for Linux/X11 kiosks. It runs a small
Flask server and displays a browser-based dashboard.

## JSON Endpoint

The app expects an HTTP JSON response like:

```json
{
  "regime": "LONG",
  "confidence": 0.82,
  "timestamp": "2026-07-18T09:30:00-04:00"
}
```

Allowed `regime` values are `LONG`, `SHORT`, and `NO_TRADE`.

## Install

On AntiX/Debian-style systems:

```bash
sudo apt update
sudo apt install -y python3 python3-flask chromium x11-xserver-utils unclutter
sudo mkdir -p /opt/regime-monitor
sudo cp regime_monitor.py start-regime-monitor.sh config.json.example requirements.txt /opt/regime-monitor/
sudo chmod +x /opt/regime-monitor/start-regime-monitor.sh /opt/regime-monitor/regime_monitor.py
sudo mkdir -p /home/$USER/.config/regime-monitor
cp config.json.example /home/$USER/.config/regime-monitor/config.json
```

Edit:

```bash
nano /home/$USER/.config/regime-monitor/config.json
```

Set your real endpoint:

```json
{
  "url": "http://your-server:8080/regime.json",
  "interval": 1.0,
  "timeout": 0.8,
  "host": "127.0.0.1",
  "port": 8765
}
```

## Test Manually

From an X desktop session:

```bash
/opt/regime-monitor/start-regime-monitor.sh
```

For server-only testing:

```bash
python3 /opt/regime-monitor/regime_monitor.py --host 127.0.0.1 --port 8765
```

Then open `http://127.0.0.1:8765/`.

If your AntiX package repo does not provide `python3-flask`, install Flask in a
small virtual environment and run the launcher with that interpreter instead:

```bash
python3 -m venv /opt/regime-monitor/.venv
/opt/regime-monitor/.venv/bin/pip install -r /opt/regime-monitor/requirements.txt
```

## AntiX / IceWM Autostart

AntiX commonly uses IceWM rather than systemd user services. This is usually
the most reliable kiosk startup path.

Create or edit:

```bash
nano ~/.icewm/startup
```

Add:

```bash
#!/usr/bin/env bash
sleep 3
/opt/regime-monitor/start-regime-monitor.sh &
```

Then:

```bash
chmod +x ~/.icewm/startup
```

Log out and back in, or reboot.

## .xinitrc Alternative

For a minimal X session:

```bash
nano ~/.xinitrc
```

Use:

```bash
#!/usr/bin/env bash
xset s off
xset -dpms
xset s noblank
/opt/regime-monitor/start-regime-monitor.sh
```

Then:

```bash
chmod +x ~/.xinitrc
```

## Optional systemd User Service

Use this only if your AntiX install has systemd user services enabled.

```bash
mkdir -p ~/.config/systemd/user
cp regime-monitor.service ~/.config/systemd/user/
nano ~/.config/systemd/user/regime-monitor.service
```

Update `REGIME_MONITOR_URL` and `ExecStart` if needed, then:

```bash
systemctl --user daemon-reload
systemctl --user enable --now regime-monitor.service
loginctl enable-linger "$USER"
```

## Behavior

- Black background.
- Large centered regime text.
- `LONG` is green.
- `SHORT` is red.
- `NO_TRADE` is gray.
- Network or JSON failure displays `NO DATA`.
- The server refreshes the upstream endpoint every 1 second by default.
- The browser dashboard polls `/data` every 250 ms and updates only fields
  whose values changed.
- Mouse cursor is hidden.
- Screen blanking is disabled by the startup script when `xset` is available.
- The JavaScript render path is isolated in `handleSnapshotMessage(...)` so it
  can later be driven by Server-Sent Events instead of polling.

## File Layout

Recommended deployment:

```text
/opt/regime-monitor/
  regime_monitor.py
  start-regime-monitor.sh
  config.json.example
  requirements.txt

~/.config/regime-monitor/
  config.json
```
