# Track B Market Data Listener Agent V1

## Purpose

The Market Data Listener Agent is a PAPER-only, display-neutral runtime market-data consumer. It observes canonical runtime market-data artifacts, evaluates their availability and freshness, and emits listener health/status artifacts for canonical readiness, maintenance decisioning, operator status, and dashboard display.

The listener does not own strategy behavior, broker state, route governance, lifecycle mutation, or repair execution.

## Authority Boundaries

- The dashboard remains display-only and consumer-only.
- Canonical readiness remains the authority for readiness classification.
- The maintenance supervisor may use listener output for advisory decisioning only unless an explicit bounded repair delegation is granted separately.
- Runtime consumers must read canonical runtime market-data artifacts, never research artifacts.
- The listener is PAPER-only and must keep `live_money_eligible=false`.
- The listener must not call submit, cancel, close, `placeOrder`, or any broker mutation API.
- The listener must not change strategy behavior, lane approvals, lifecycle state, or handoff state.

## Shared Symbol Namelist

The authoritative runtime market-data coverage source is:

`config/track_b_live_market_data_symbols.yaml`

It is loaded and validated by:

`src/mgc_v05l/execution_core/track_b_live_market_data_symbols.py`

The listener must consume this namelist instead of embedding a local futures ticker list. Every listener cycle should load the namelist once at cycle start and use it to derive:

- enabled runtime symbols: rows with `enabled=true`
- readiness-blocking symbols: enabled rows with `required_for_readiness=true`
- optional/degrading symbols: enabled rows with `required_for_readiness=false`
- disabled rows for reporting only
- Databento selection metadata: `databento_symbol`, `dataset`, `schema`, `venue`, and `timezone`
- freshness requirements: `min_confirmed_bars` and `freshness_threshold_seconds`
- explicit execution/reference relationships, including `MGC/GC`, `MES/ES`, and `MNQ/NQ`

Disabled symbols are ignored operationally but still reported in listener health output so operators can see that coverage was intentionally excluded.

## Validation Contract

The namelist loader fails fast before the listener evaluates artifacts when:

- an enabled symbol lacks a Databento mapping
- duplicate enabled Databento mappings are present
- duplicate symbol rows are present
- the required execution/reference pairs are absent or malformed
- required row fields are missing or use the wrong type

These validation failures should be surfaced as listener configuration blockers. They are not broker blockers and must not trigger repair execution.

## Listener Cycle Design Integration

1. Load `track_b_live_market_data_symbols.yaml` through the pure loader.
2. Build the enabled-symbol worklist from `enabled=true` rows.
3. For each enabled row, inspect only canonical runtime market-data artifacts for that symbol and schema.
4. Compare confirmed bar count against `min_confirmed_bars`.
5. Compare latest confirmed runtime bar age against `freshness_threshold_seconds`.
6. Classify required rows as readiness-blocking when stale or missing.
7. Classify optional rows as degraded when stale or missing without blocking all readiness.
8. Include disabled rows in the emitted report as `disabled` or equivalent display state.
9. Emit a listener health artifact that canonical readiness and the dashboard can consume.

The listener health artifact should include the loaded config path, config version, symbol row summaries, enabled/required/optional/disabled partitions, per-symbol freshness evidence, and a clear distinction between required blockers and optional degradations.

## Migration Targets

The shared namelist is intended to become the single source of truth for live runtime market-data coverage across:

- Phase-1 Databento live listener
- Market Data Listener Agent
- canonical readiness market-data freshness checks
- maintenance supervisor market-data decisions
- runtime lane config validation
- dashboard display
- preflight/status scripts
- runtime market-data provenance validation
- future runtime/backfill mapping where appropriate

This V1 design slice only introduces the namelist contract and listener consumption plan. It does not refactor existing consumers, restart runtime processes, mutate lifecycle state, or change strategy behavior.
