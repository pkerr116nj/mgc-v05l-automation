# IBKR Paper Monitor Launchd Template

This repo includes a launchd template for Patrick's local machine, but it is not installed automatically.

Use it only for:

- paper monitoring
- local `127.0.0.1:7497`
- account `DUM882026`

Do not use it for live trading.

Template path:

- `ops/launchd/com.mgc_v05l.ibkr_paper_strategy_monitor.plist.template`

Recommended workflow:

1. Confirm `bash scripts/start-paper-monitor` works first.
2. Copy the template into `~/Library/LaunchAgents/` with the repo path substituted.
3. Load it manually with `launchctl`.
4. Verify `var/paper_strategy_monitor_runtime_status.json` stays fresh.
5. Confirm the bridge blocks if the service is stopped or stale.
