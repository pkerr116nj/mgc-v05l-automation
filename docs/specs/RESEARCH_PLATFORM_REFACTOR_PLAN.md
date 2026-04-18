# Research Platform Refactor Plan

## Current State

- Canonical bars are durable in `mgc_v05l.replay.sqlite3`.
- ATP feature/state logic is relatively centralized in `src/mgc_v05l/research/trend_participation`.
- Candidate and outcome logic exist, but persisted reusable intermediate datasets do not.
- Multiple runners still rebuild `bars -> features -> candidates -> outcomes -> overlays`.
- `src/mgc_v05l/app/strategy_universe_retest.py` keeps a duplicate ATP trade reconstruction path instead of consuming one shared outcome engine.

## Target State

The platform should separate:

1. canonical raw/context data
2. persisted feature rows
3. persisted candidate rows
4. one authoritative outcome layer
5. overlays operating on persisted truth
6. standardized manifests and experiment comparability

## Current Dependency Graph

`bars -> _load_symbol_context -> build_feature_states -> classify_entry_states -> classify_timing_states -> _rebuild_atp_trades / simulate_timed_entries -> overlay runner -> report files`

## Proposed Dependency Graph

`bars -> normalized symbol context -> feature bundle -> scope bundle (entry states, timing states, trade records) -> overlay/governance runner -> standardized experiment manifest/report`

## Recomputation Boundaries

- Raw/context: rerun when source DB, source selection, or date span changes.
- Feature bundle: rerun when feature definitions change.
- Scope bundle: rerun when candidate/timing/outcome engine assumptions change.
- Overlay runs: rerun when only overlay config changes.

## Project-wide Inheritance Risks

### Legacy MGC ghosts still present

- `src/mgc_automation/settings.py` still preserves `SINGLE_SYMBOL_MGC` and `symbol="MGC"` in legacy baseline settings.
- `src/mgc_v05l/app/tracked_paper_strategies.py` still hard-codes the frozen ATP tracked paper surface as the MGC benchmark lane.
- `src/mgc_v05l/app/operator_dashboard.py` still contains several fallback instrument defaults of `"MGC"` in display paths.

### ATP-specific ghosts we could create if careless

- Treating phase-2/phase-3 ATP artifacts as if they were the universal platform schema.
- Baking ATP session assumptions (`ASIA`, `US`, London diagnostic-only) into shared dataset infrastructure.
- Promoting ATP-specific overlay vocabulary into project-wide platform primitives.

### What should be generalized now

- dataset/manifest storage
- provenance hashing
- queryable bundle catalogs
- reusable feature/candidate/outcome persistence patterns

### What should remain strategy-specific

- ATP entry-state semantics
- ATP timing-state semantics
- ATP execution model labels
- ATP overlay families and governance controls

## Platform Boundary Recommendation

### Shared project-level infrastructure

- dataset bundle writer/reader
- manifest hashing/versioning
- DuckDB/Parquet registration
- experiment registry/catalog primitives

### ATP-specific layers on top

- `FeatureState` definitions for ATP
- `AtpEntryState`
- `AtpTimingState`
- ATP forward-outcome rules
- ATP overlays

### Where over-generalization would be a mistake

- forcing all strategy families into ATP candidate/timing schemas
- making ATP overlay parameters part of a shared platform API now

### Where under-generalization would create debt

- leaving manifests/report directories ATP-specific
- keeping persistence helpers buried under ATP modules
- keeping multiple forward-outcome engines alive

## First Tranche

1. Add project-level dataset/manifest helpers.
2. Add ATP feature/scope substrate bundles.
3. Route ATP trade reconstruction through one outcome engine.
4. Refactor the full-history ATP review to consume persisted scope bundles.
5. Preserve backward compatibility for legacy callers while deprecating duplicate outcome logic.
