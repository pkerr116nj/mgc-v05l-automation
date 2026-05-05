# Operator Dashboard Log Retention

The operator dashboard keeps its live API/status path bounded. The dashboard may tail the active hot log, but it must not scan rotated logs or cold archives while serving `/api/dashboard`.

## Active Log Rotation

Active hot log:

- `outputs/operator_dashboard/action_log.jsonl`

Rotation policy:

- The active log rotates when it exceeds `MGC_OPERATOR_ACTION_LOG_MAX_BYTES`.
- Default max active size: `50 MB`.
- Rotated logs are retained locally up to `MGC_OPERATOR_ACTION_LOG_ROTATED_KEEP`.
- Default local rotated retention: `10` files.
- Rotated logs are compressed by default when `MGC_OPERATOR_ACTION_LOG_COMPRESS_ROTATED=true`.
- New dashboard actions always append to the fresh active `action_log.jsonl`.
- Retention only deletes rotated `action_log.*.jsonl` or `action_log.*.jsonl.gz` files inside `outputs/operator_dashboard`.

Environment knobs:

- `MGC_OPERATOR_ACTION_LOG_MAX_BYTES`
- `MGC_OPERATOR_ACTION_LOG_ROTATED_KEEP`
- `MGC_OPERATOR_ACTION_LOG_COMPRESS_ROTATED`
- `MGC_OPERATOR_ARCHIVE_ROOT`

`MGC_OPERATOR_ARCHIVE_ROOT` is reserved for a future cold-storage mover. It is not read by `/api/dashboard` and no archive movement is active by default.

## Artifact Tiers

HOT files are bounded live-status inputs. They are safe for the dashboard/API hot path:

- `outputs/operator_dashboard/action_log.jsonl`
- `outputs/track_b_execution_core/operator_status/latest_operator_status_summary.json`
- latest Track B runtime-cycle report
- latest Track B market-data/observer reports
- latest files needed by `/api/dashboard` and desktop live status

WARM files are recent local history with bounded retention:

- recent rotated `outputs/operator_dashboard/action_log.*.jsonl.gz`
- recent paper-proof reports
- recent runtime-cycle reports
- recent dashboard snapshots

COLD files are historical archives and research/replay material:

- old compressed dashboard logs
- old JSONL streams
- old replay/research artifacts
- old paper-proof/runtime-cycle archives
- future Linux storage box or mounted archive volume, for example `MGC_ARCHIVE_ROOT=/mnt/mgc_archive` or `MGC_ARCHIVE_ROOT=/Volumes/mgc_archive`

Hard rule: cold archive data must not be read by `/api/dashboard`.
