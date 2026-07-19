# AntiX Regime Monitor Kiosk Client

The AntiX device is display-only. It does not run Flask, Python, configuration,
Databento ingestion, regime calculation, `/data`, or a local polling proxy.

It only launches a browser in kiosk mode pointed at the Ubuntu dashboard:

```text
http://192.168.1.80:5000/
```

## Install On AntiX

```bash
sudo apt update
sudo apt install -y chromium x11-xserver-utils unclutter
sudo mkdir -p /opt/regime-monitor-antix
sudo cp start-regime-monitor.sh /opt/regime-monitor-antix/
sudo chmod +x /opt/regime-monitor-antix/start-regime-monitor.sh
```

If your AntiX package uses `chromium-browser` instead of `chromium`, the
launcher will detect that automatically. Firefox kiosk mode is also supported
as a fallback.

## Exact AntiX Kiosk Command

```bash
chromium --kiosk --noerrdialogs --disable-infobars http://192.168.1.80:5000/
```

Or use the launcher:

```bash
/opt/regime-monitor-antix/start-regime-monitor.sh
```

To point at a different Ubuntu host without editing the script:

```bash
REGIME_MONITOR_DASHBOARD_URL=http://192.168.1.80:5000/ /opt/regime-monitor-antix/start-regime-monitor.sh
```

## AntiX / IceWM Autostart

Create or edit:

```bash
nano ~/.icewm/startup
```

Add:

```bash
#!/usr/bin/env bash
sleep 3
/opt/regime-monitor-antix/start-regime-monitor.sh &
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
/opt/regime-monitor-antix/start-regime-monitor.sh
```

Then:

```bash
chmod +x ~/.xinitrc
```
