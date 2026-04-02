# Research Daily Capture Scheduling

Primary Mac-friendly schedule:

- launchd label: `com.mgc_v05l.research_daily_capture.daily`
- local schedule: every day at `18:15` in the Mac user's local timezone
- runtime root: `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime`
- launchd launcher path: `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/research_daily_capture_runner.sh`
- runtime env path: `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/schwab_env.sh`
- runtime token path: `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/tokens.json`
- runtime config paths:
  - `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/config/base.yaml`
  - `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/config/replay.yaml`
  - `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/config/schwab.local.json`
  - `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/config/data_storage_policy.json`
- underlying capture command: `mgc-v05l.app.main research-daily-capture --config .../config/base.yaml --config .../config/replay.yaml --policy-config .../config/data_storage_policy.json --schwab-config .../config/schwab.local.json`

What now lives outside the repo at run time:

- Schwab auth env
- Schwab token file
- Schwab market-data config copy
- launchd runtime launcher
- launchd log files
- data storage policy copy used by the scheduled job

What intentionally remains in the repo:

- source code
- virtualenv / Python dependencies
- forever-retained research database and manifests

Install:

```bash
bash /Users/patrick/Documents/MGC-v05l-automation/scripts/install_research_daily_capture_launchd.sh
```

Reload after edits:

```bash
bash /Users/patrick/Documents/MGC-v05l-automation/scripts/install_research_daily_capture_launchd.sh
```

Verify:

```bash
launchctl print "gui/$(id -u)/com.mgc_v05l.research_daily_capture.daily"
```

Manual run-now check:

```bash
launchctl kickstart -k "gui/$(id -u)/com.mgc_v05l.research_daily_capture.daily"
```

Verify the launched job is using runtime paths:

```bash
launchctl print "gui/$(id -u)/com.mgc_v05l.research_daily_capture.daily"
tail -n 100 "/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/logs/daily_capture.stdout.log"
tail -n 100 "/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/logs/daily_capture.stderr.log"
```

Disable safely:

```bash
bash /Users/patrick/Documents/MGC-v05l-automation/scripts/uninstall_research_daily_capture_launchd.sh
```

Log paths:

- stdout: `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/logs/daily_capture.stdout.log`
- stderr: `/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/logs/daily_capture.stderr.log`

Cron-safe fallback:

```cron
15 18 * * * cd /Users/patrick/Documents/MGC-v05l-automation && bash /Users/patrick/Documents/MGC-v05l-automation/scripts/run_research_daily_capture.sh >> "/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/logs/daily_capture.stdout.log" 2>> "/Users/patrick/Library/Application Support/mgc_v05l/research_daily_capture_runtime/logs/daily_capture.stderr.log"
```
