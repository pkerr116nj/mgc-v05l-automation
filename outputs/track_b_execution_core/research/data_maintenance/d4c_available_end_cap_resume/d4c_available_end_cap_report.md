# D4C Databento Available-End Cap Report

- generated_at: `2026-07-01T09:45:00-04:00`
- phase: `D4C`
- scope: research-only Databento minute backfill
- broker_actions: `false`
- runtime_restart: `false`
- managed_exit_restart: `false`
- strategy_changes: `false`
- resume_download_executed: `false`

## Current Behavior Audited

Before D4C, `databento_research_minute_backfill` converted an explicit `--end-date` into the full UTC day ending at `23:59:59`. For current-day requests this could ask Databento for data beyond the dataset available end. D4B hit this exact case: `GLBX.MDP3` was available through `2026-07-01T13:10:00Z`, but the request asked through `2026-07-01T23:59:59Z`.

The CLI did not expose a provider metadata probe before chunk requests. The low-risk D4C fix therefore adds explicit end-boundary resolution that can be driven by a known provider available-end without making a network request during dry-run planning.

## D4C Fix

Added end-boundary controls:

- `--end-boundary-mode closed-day`
  - default weekly/maintenance mode
  - caps current/future requests to the latest fully closed UTC day
- `--end-boundary-mode intraday-available-end`
  - explicit intraday/resume mode
  - may use `--provider-available-end`
- `--provider-available-end`
  - optional timestamp used to cap the effective request end

Reports now include:

- `requested_end`
- `effective_requested_end`
- `planned_fetch_end`
- `end_boundary_mode`
- `end_boundary_capped`
- `end_boundary_cap_reason`
- `provider_available_end`
- `scope_estimate.end_resolution`

## Safety Contract Preserved

- configured research universe still enforced
- unknown symbols still rejected
- explicit date ranges still required
- dry-run/preflight still estimates scope
- large refreshes still require approval
- existing complete partitions are skipped unless `--force`
- no broker/runtime/Managed Exit/strategy paths added

## Validation Summary

- unit tests: `19 passed`
- compileall: passed
- git diff check: passed
- AST safety scan: clean
- D4C dry-run provider errors: `0`

