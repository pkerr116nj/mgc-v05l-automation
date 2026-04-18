# Research Platform Tranche 3

## Scope

This tranche consolidated the highest-leverage debt left after tranche 2:

- reroute the remaining legacy ATP evaluator path onto shared bundle truth
- standardize ATP-family registry registration
- continue structured config externalization
- move the application closer to direct research-analytics consumption
- measure what still dominates bounded ATP run wall time

## Dependency Flow

### Before

- raw bars
- app-local source/context loading
- ATP feature generation
- ATP entry/timing generation
- duplicate ATP trade reconstruction in `strategy_universe_retest.py`
- runner-local reports
- partial registry/analytics publishing
- app consuming mixed snapshot and replay-linked truth

### After

- raw bars
- project-level source/context service
- ATP feature bundles
- ATP scope bundles
- shared ATP outcome path
- ATP runners consuming persisted scope truth
- shared experiment registry
- published research analytics datasets
- app/backend serving `/api/research-analytics/<dataset>`
- renderer consuming calendar and deep-dive analytics directly

## Main Structural Changes

### Legacy ATP evaluator

`strategy_universe_retest._evaluate_atp_lane(...)` now uses the shared ATP substrate for both:

- rolling ATP current truth
- completed-5m comparator truth

The completed comparator remains only as a compatibility mode. It no longer owns its own private ATP trade-generation path.

### Registry standardization

ATP experiment families now register through `mgc_v05l.app.atp_experiment_registry.register_atp_report_output(...)`.

Registered ATP families:

- `full_history_review`
- `exit_drawdown_matrix`
- `us_fast_fail_review`
- `us_early_invalidation_refinement`
- `us_late_pocket_refinement`
- `drawdown_limit_governance`
- `production_shaping_review`
- `gc_production_track_execution_realism`

### Analytics serving

Published analytics datasets for app use:

- `strategy_catalog`
- `daily_pnl`
- `strategy_summaries`
- `equity_curve`
- `drawdown_curve`
- `trade_blotter`
- `exit_reason_breakdown`
- `session_breakdown`

The operator dashboard now serves these through:

- `/api/research-analytics/<dataset>`

### Renderer consumption

Direct analytics-driven surfaces now include:

- P/L Calendar via `daily_pnl`
- Strategy Deep Dive via:
  - `strategy_summaries`
  - `equity_curve`
  - `drawdown_curve`
  - `trade_blotter`
  - `exit_reason_breakdown`
  - `session_breakdown`

## Runtime Findings

Bounded optimized full-history smoke:

- window: `2024-01-02T00:00:00+00:00` -> `2024-01-03T00:00:00+00:00`
- previous tranche-2 smoke wall time: `177.139776s`
- tranche-3 smoke wall time: `29.238194s`

Key deltas:

- `coverage_seconds`: `26.306033s` -> `0.000013s`
- `GC feature_seconds`: `0.163321s` -> `0.024862s`
- `MGC feature_seconds`: `0.159149s` -> `0.025387s`
- `GC Asia scope eval`: `0.130505s` -> `0.048720s`
- `MGC Asia scope eval`: `0.114045s` -> `0.047702s`

Current dominant bounded-run offender:

- `source_discovery_seconds = 28.927150s`

That means the remaining bounded ATP wall time is now mostly source-discovery scan cost, not bundle recomputation, report writing, registry, or analytics publishing.

## Inheritance Risk Check

### MGC ghosts still present

- legacy MGC benchmark defaults in `src/mgc_automation/settings.py`
- benchmark-centered tracked strategy assumptions in `src/mgc_v05l/app/tracked_paper_strategies.py`
- some display/default fallbacks in `src/mgc_v05l/app/operator_dashboard.py`

### ATP ghosts still at risk

- treating `AtpEntryState` and `AtpTimingState` as project-wide primitives
- leaking ATP session semantics into shared platform contracts

### Intentionally kept strategy-specific

- ATP feature definitions
- ATP candidate semantics
- ATP timing semantics
- ATP outcome semantics
- ATP overlay/control logic

### Intentionally promoted to project-level infrastructure

- source/context bundles
- dataset bundle persistence helpers
- experiment registry/catalog
- analytics publishing/serving
- app-facing analytics API shape

## Remaining Debt

- source discovery is still expensive and should become a cached/platformized inventory rather than a repeated grouped SQLite scan
- some ATP runners still have partially hardcoded control catalogs even though hashing/registry identity is cleaner
- the renderer still has one major gap: the dedicated Strategy History Review page is not fully analytics-native yet
- non-ATP research families have not yet been migrated onto the same platform path
