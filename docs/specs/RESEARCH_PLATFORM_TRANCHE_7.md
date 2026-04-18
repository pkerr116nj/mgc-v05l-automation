# Research Platform Tranche 7

## Summary

This tranche closes out the warehouse `2024+2025` population cycle as an operating-policy milestone rather than a new data-generation push.

The main outcome is that cumulative `2024+2025` warehouse analytics are now the explicit default app-visible warehouse tenant state, while isolated quarter diagnostics remain the interpretation and debug path.

## Default Warehouse State

The default warehouse tenant state is:

- strategy family: `warehouse_historical_evaluator`
- app-visible publication mode: `cumulative`
- coverage expectation: continuous cumulative warehouse truth across completed quarters
- current promoted span: `2024Q1` through `2025Q4`

Isolated quarterly review artifacts remain diagnostic-only:

- publication mode: `diagnostic`
- purpose: shard validation, debugging, and quarter-specific interpretation
- not part of the shared app-visible warehouse root

The shared analytics family index and combined app payload now carry family publication metadata so this distinction is explicit instead of implicit.

## Warehouse Operating Policy

Warehouse operates on a cumulative-plus-diagnostic model:

- cumulative publication:
  - publish warehouse truth into the shared family analytics root
  - this is the default tenant view used by the app and dashboard
- isolated quarter diagnostics:
  - publish the requested quarter into an isolated diagnostic root under the run output
  - use this for quarter-level validation, interpretation, and debugging

Year-boundary handling:

- keep one continuous warehouse tenant root across adjacent years when the same lane family remains valid
- do not split the warehouse tenant by year unless a future regime break or schema break justifies it

When to extend into a new quarter or year:

- extend when the most recent isolated quarter remains rich and cumulative publication stays clean
- keep using `--publish-mode both` for quarter additions so cumulative app state and quarter diagnostics stay aligned

When to rerun a prior quarter:

- rerun only if warehouse family logic changed, source truth changed, or quarter diagnostics need explicit repair validation
- otherwise rely on published cumulative truth and existing quarter diagnostics

Cache expectations and invalidation:

- quarter-stage cache reuse is expected on repeat runs of the same shard when source truth, lane logic, and upstream stage keys are unchanged
- invalidate quarter caches when:
  - the underlying SQLite/source signatures change
  - warehouse family-event or closed-trade logic changes
  - stage cache version/fingerprint changes

## Tenant Policy

Current tenant policy is:

- `atp_companion`
  - stable rich tenant
  - no rerun unless a shared defect appears
- `approved_quant`
  - structurally integrated but thin under the currently admitted baseline specs
  - do not rerun on the same admitted baseline specs without a new family hypothesis
- `warehouse_historical_evaluator`
  - active rich tenant for broader historical population
  - cumulative `2024+2025` is the default app-visible warehouse state

## Performance Baseline

Current warehouse performance baseline:

- cold quarter runs:
  - still dominated by quarter-local raw/materialized reads
  - especially `raw_bars_1m` and repeated downstream materialized reads
- warm repeat-quarter runs:
  - materially cheaper because quarter-stage cache manifests now bound recomputation
- shared platform overhead:
  - small relative to warehouse materialization

Future targeted performance work is justified only if repeat-quarter reruns become common enough that the remaining warm cost matters operationally.

The next narrow performance candidate is cache-stability cleanup for stages that still miss reuse on warm reruns:

- `derived_bars_5m`
- `derived_bars_10m`
- `shared_features_1m_timing`
- `shared_features_5m`
- `lane_compact_results`

## 2026Q1 Readiness

If warehouse expansion continues, the next clean empirical step is `2026Q1` using the same model:

- run the quarter with `--publish-mode both`
- publish cumulative shared warehouse truth into the existing warehouse family root
- write isolated quarter diagnostics into the run-local diagnostic platform root
- validate isolated richness before committing to further `2026` quarters

No additional year-transition handling change is required before `2026Q1`.
