# Track B Weekly Maintenance Contract

Status: design and operator contract. This document does not install a scheduler,
start or stop runtime services, connect to brokers, create order intent, mutate
lifecycle state, or grant live-money eligibility.

## Scope

Track B uses two different maintenance lanes. They must remain separate.

### Weekly Artifact Hygiene

`mgc_v05l.app.weekly_data_maintenance` is artifact and storage hygiene. It is
diagnostic-only and non-proof-blocking.

Purpose:

- classify HOT runtime artifacts, WARM evidence/state, COLD archive candidates,
  deferred research/offline files, and disposable build/cache metadata;
- detect old-root contamination from archived Documents/iCloud roots;
- write a human-readable maintenance report for review;
- preserve runtime truth and broker/reconciliation evidence.

It does not fetch market data, update Databento files, update Phase-1 runtime
artifacts, evaluate strategy signals, provide proof authority, or permit any
runtime action.

Normal command:

```bash
./.venv/bin/python -m mgc_v05l.app.weekly_data_maintenance \
  --mode dry-run \
  --repo-root /Users/patrick/Dev/MGC-v05l-automation \
  --output-root outputs/reports/weekly_data_maintenance
```

Artifacts:

- `outputs/reports/weekly_data_maintenance/latest_weekly_data_maintenance_report.json`
- `outputs/reports/weekly_data_maintenance/weekly_data_maintenance_<week-ending>.md`
- optional stdout/stderr logs under `outputs/reports/weekly_data_maintenance/`

Proof stance:

- Missing or stale weekly artifact hygiene is not, by itself, a Sunday PAPER
  proof blocker.
- A report with `review_required=true` should be reviewed before proof because
  it can surface old-root contamination, but the weekly hygiene report is still
  not proof authority.

### Historical MGC Data Maintenance

`mgc_v05l.execution_core.track_b_data_maintenance_cli` is historical MGC 1m maintenance.
It is market-data maintenance for research, replay, backtesting, and historical
feature context.

Purpose:

- maintain the local rolling MGC 1m history store;
- fetch or merge bounded Databento historical `ohlcv-1m` bars;
- validate ordering, duplicates, gaps, minimum bars, and completion through an
  intended cutoff such as Friday close;
- export latest-good historical context for explicit consumers.

Current enabled maintenance scope:

- instrument: `MGC`
- execution/local symbol: `MGCM6`
- Databento continuous symbol: `MGC.v.0`
- dataset: `GLBX.MDP3`
- schema/timeframe: `ohlcv-1m` / `1m`

Artifacts:

- `outputs/track_b_execution_core/track_b_data_maintenance/latest_good_mgc_1m_history.json`
- `outputs/track_b_execution_core/track_b_data_maintenance/latest_track_b_data_maintenance_report.json`

Proof stance:

- Historical MGC data maintenance can be required only for workflows that
  explicitly consume maintained MGC historical context.
- It is not realtime market-data evidence.
- It is not a substitute for Phase-1 runtime market-data artifacts or runtime
  ingestion proof.
- It does not provide proof authority by itself.

## Sunday PAPER Proof Readiness

Sunday PAPER proof readiness is governed by current runtime/control evidence,
not weekly artifact hygiene.

Required authorities include:

- fresh Control Plane Snapshot evidence;
- Phase-1 readiness and runtime market-data provenance;
- broker truth lease and clean broker reconciliation;
- runtime ingestion proof where submit-capable runtime state is considered;
- explicit proof readiness checks and operator authorization.

Weekly artifact hygiene is diagnostic-only. Historical MGC data maintenance is
historical context only. Neither lane may promote itself to proof authority.

## Proposed Diagnostic Schedule

The weekly artifact-hygiene dry run may be scheduled after the Saturday/Sunday
maintenance window and before Sunday preflight.

Proposed local schedule:

- label: `com.mgc_v05l.weekly_data_maintenance.dry_run`
- cadence: Sunday 09:00 America/New_York
- command: `mgc_v05l.app.weekly_data_maintenance --mode dry-run`
- working directory: `/Users/patrick/Dev/MGC-v05l-automation`
- stdout/stderr: `outputs/reports/weekly_data_maintenance/`
- install status: template only, not installed

The launchd template is:

- `deploy/launchd/templates/com.mgc_v05l.weekly_data_maintenance.dry_run.plist`

Do not schedule apply mode. If apply mode is ever considered, it must be a
separate operator-approved workflow limited to disposable build/cache metadata.
