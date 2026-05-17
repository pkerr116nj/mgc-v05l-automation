# Track B Advisory Layer Backfill v1 Design

Status: architecture/design only. This document does not add a runtime producer, broker activity, lane execution, paper activity, generated outputs, historical batch launch, Parquet generation, or SQLite migration.

## 1. Scope And Goals

Track B now has four reusable advisory layers:

- Lifecycle Awareness
- Participation / Pressure, currently implemented through the Participation Quality compatibility contract
- Regime State
- Sizing / Position Management

The backfill goal is to create a bounded historical advisory-state archive that attaches these advisory states to historical trade episodes and replay windows. The archive should help research, replay, attribution, and operator explainability answer questions such as:

- What lifecycle state did a trade pass through before a fixed 36-bar exit?
- Did participation pressure decay before the exit window?
- Was the regime trending, compressed, thin, or choppy during the episode?
- Would sizing context have been base, reduced, blocked, hold-full, or reduce-partial, advisory only?

The archive is not intended to become runtime truth. It is a research and replay artifact built from explicit historical inputs and versioned advisory evaluators.

## 2. Episode-Centric Versus Global-Bar-Centric Design

The archive should be episode-centric rather than global-bar-centric.

Episode-centric means each advisory row is attached to a known trade episode, entry candidate, managed position, or bounded replay window. The row describes what the four advisory layers saw at a relative point in that episode.

Global-bar-centric would generate advisory states for every bar across every instrument and session, regardless of whether a trade candidate or position existed. That is broader, costlier, and less useful for the current Track B goal.

Recommended v1 scope:

- attach rows to trade episodes, entry candidates, and replay windows.
- include relative offsets such as `bars_since_entry`, `window_bar_index`, and `bars_to_exit`.
- include enough market context to reproduce the advisory evaluation window.
- avoid creating a global market-state lake.

This keeps storage and compute bounded while still supporting fixed 36b, adaptive 24/36, participation-aware exits, and future sizing research.

## 3. Recommended Storage

### Parquet

Use Parquet as the primary historical archive format.

Reasons:

- efficient columnar scans for research.
- good compression for repeated enum labels and context fields.
- natural partitioning by strategy family, instrument, year, and layer.
- works with Python, DuckDB, Polars, Spark, and pandas.

Parquet files should contain advisory state rows, not raw candle data. Raw market bars should remain in their existing source datasets and be referenced through provenance fields.

### Manifests

Each build should write a manifest describing:

- build id.
- git commit.
- evaluator versions.
- input dataset ids and checksums where available.
- strategy family and instrument universe.
- episode selection query or source.
- time range.
- row counts by layer, year, instrument, and strategy family.
- fail-closed counts.
- known limitations.

The manifest is the audit anchor for comparing historical archives.

### Optional SQLite Index/Catalog

SQLite should be optional and introduced later as an index/catalog, not as the primary storage.

Useful catalog tables later:

- `backfill_builds`
- `episode_index`
- `artifact_files`
- `schema_versions`
- `advisory_state_counts`
- `invalidations`

SQLite can make lookup and UI browsing easier, but v1 should not require a migration.

## 4. Proposed Directory Structure

Proposed future artifact root:

```text
outputs/track_b_execution_core/advisory_backfill/
  manifests/
    build_id=YYYYMMDDTHHMMSSZ__track_b_advisory_v1.json
  parquet/
    layer=lifecycle_awareness/
      strategy_family=exact_baseline/
        instrument=MGC/
          year=2020/
            part-000.parquet
    layer=participation_pressure/
      strategy_family=exact_baseline/
        instrument=MGC/
          year=2020/
            part-000.parquet
    layer=regime_session/
      strategy_family=exact_baseline/
        instrument=MGC/
          year=2020/
            part-000.parquet
    layer=sizing_position_management/
      strategy_family=exact_baseline/
        instrument=MGC/
          year=2020/
            part-000.parquet
  catalog/
    advisory_backfill_index.sqlite
```

No part of this structure is created by this design slice.

## 5. Core Schema Fields

All layers should share a core envelope:

- `schema_version`
- `layer_name`
- `layer_contract`
- `build_id`
- `build_created_at`
- `source_git_commit`
- `evaluator_module`
- `evaluator_version`
- `strategy_family`
- `strategy_id`
- `exit_profile`
- `instrument`
- `root_symbol`
- `contract_symbol`
- `timeframe`
- `session_bucket`
- `episode_id`
- `candidate_id`
- `position_id`
- `replay_window_id`
- `entry_timestamp`
- `evaluation_timestamp`
- `exit_timestamp`
- `window_start_timestamp`
- `window_end_timestamp`
- `bars_since_entry`
- `bars_to_exit`
- `window_bar_index`
- `lifecycle_window_bars`
- `input_source_category`
- `input_dataset_id`
- `input_dataset_version`
- `input_row_hash`
- `provenance_status`
- `freshness_status`
- `runtime_eligible`
- `confidence`
- `failure_reasons`
- `warning_reasons`
- safety flags, all false.

Layer-specific state fields should remain explicit:

- Lifecycle Awareness: `lifecycle_awareness_state`, `hold_quality_context`, `exit_urgency_context`, `reduce_size_context`, `add_size_context`, `patience_context`, MFE/MAE/progress fields.
- Participation / Pressure: `pressure_state`, `directional_bias`, `hold_quality_context`, `exit_urgency_context`, `pressure_confidence`, `pressure_reasons`.
- Regime State: `market_regime_state`, `volatility_range_state`, `trend_chop_state`, `liquidity_state`, `directional_context`.
- Sizing / Position Management: `initial_size_context`, `in_position_size_context`, `add_size_context`, `strategy_family`, `exit_profile`, `instrument`, `timeframe`.

Nested original evaluator output may be stored as compact JSON for audit, but research-facing fields should be promoted to typed columns.

## 6. Provenance, Freshness, And Runtime-Boundary Rules

Backfilled advisory rows must be marked as historical research or replay artifacts.

Required rules:

- no row may claim runtime eligibility.
- no row may mutate lifecycle state.
- no row may create order intent.
- no row may submit, cancel, close, place, or route orders.
- all safety flags must remain false.
- input source category must distinguish research, replay, test fixture, and historical runtime capture.
- freshness status must describe historical as-of validity, not live market freshness.
- missing provenance must fail closed.
- stale, thin, incomplete, mixed-timeframe, or unreconciled inputs must produce fail-closed advisory states.

Historical backfills may use historical truth that would not have been available in live runtime only when the manifest labels the build as post-hoc research. Runtime replay builds must use only the information available as of the evaluation timestamp.

## 7. Replay And Backtest Integration Points

The archive should integrate with replay/backtest tools at episode boundaries.

Expected integration points:

- entry-candidate replay: attach Entry Acceptance, Participation / Pressure, Regime State, and Initial Sizing context at candidate time.
- position replay: attach Lifecycle Awareness, Exit Context, Participation / Pressure, Regime State, and In-Position Size Management at each evaluation offset.
- exit-candidate replay: compare fixed 36b, adaptive 24/36, participation-driven exits, and advisory reduce/hold labels.
- attribution notebooks: join advisory states to realized outcome, MFE, MAE, drawdown, time-in-trade, session, and regime.
- operator UI prototypes: browse advisory histories for representative episodes without touching live runtime paths.

Replay consumers should read the archive as advisory annotations. They should not treat it as a strategy authority source.

## 8. Bounded-Partition Strategy

V1 should partition by the dimensions most likely to bound scans:

- `layer`
- `strategy_family`
- `instrument`
- `year`

Optional later partitions:

- `exit_profile`
- `timeframe`
- `session_bucket`
- `build_id`

The first build should avoid over-partitioning. Many tiny Parquet files would make local research slower and noisier. Target annual files per layer, strategy family, and instrument unless row counts demand monthly partitioning.

Recommended row grain:

- one row per layer per episode evaluation timestamp.
- one row per layer per entry candidate when evaluating initial sizing.
- one row per layer per 48-bar replay window offset when evaluating in-position context.

## 9. Rebuild And Invalidation Policy

Backfills should be immutable by build id.

Create a new build when any of these change:

- evaluator code.
- schema version.
- source input data.
- episode selection logic.
- candle normalization.
- timeframe derivation.
- strategy family mapping.
- provenance or fail-closed rules.

Invalidate or supersede a build when:

- a source dataset is corrected.
- a bug is found in advisory evaluation.
- a manifest checksum does not match.
- a replay source used future information unintentionally.
- a schema contract changes incompatibly.

The optional SQLite catalog can record supersession relationships later. Parquet files should not be edited in place except during a failed build cleanup before publication.

## 10. Safety Boundaries

The backfill architecture is research/offline/advisory only.

Forbidden:

- broker commands.
- runtime producers.
- lane execution.
- paper activity.
- order intent creation.
- lifecycle mutation.
- strategy behavior changes.
- generated outputs in design slices.
- treating historical advisory rows as live authorization.

Required safety flags for every advisory state:

- `strategy_authority=false`
- `broker_state_mutated=false`
- `submit_attempted=false`
- `order_intent_created=false`
- `lifecycle_mutated=false`
- `runtime_trade_eligible=false`

If future schemas include additional flags such as `cancel_attempted`, `close_attempted`, or `place_order_attempted`, those must also remain false.

## 11. Suggested First Backfill Target

Recommended first bounded target:

- strategy family: exact baseline.
- instruments: GC and MGC, normalized to a shared root where appropriate.
- years: 2020 through 2026.
- lifecycle window: 48 bars from entry.
- timeframe: the baseline evaluation timeframe used by the strategy replay.
- exit comparison: fixed 36b baseline first; adaptive 24/36 as a later join.

Why this target:

- exact baseline provides the cleanest reference behavior.
- GC/MGC continuity supports enough episode density for useful distributions.
- 2020-2026 includes varied volatility, trend, compression, and session regimes.
- 48 bars covers newly opened, working, fixed 36b exit horizon, late decay, and post-exit comparison windows.

The first run should be a dry-run design implementation with tiny sample output, then a bounded yearly build, then the full 2020-2026 build only after manifest validation.

## 12. Expected Compute And Storage Footprint

Expected footprint should be modest because the archive is episode-centric.

Illustrative sizing:

- 10,000 episodes.
- 48 evaluation offsets per episode.
- 4 advisory layers.
- about 1.9 million advisory rows.

With Parquet compression and enum-heavy columns, this is likely in the tens to low hundreds of megabytes, depending on nested JSON retention. If compact JSON evaluator outputs are stored for every row, storage could grow substantially. V1 should promote key fields to columns and keep nested blobs optional.

Compute should be bounded by:

- number of episodes.
- number of offsets per episode.
- candle window size required by each evaluator.
- source data read cost.

The first implementation should measure:

- rows per second.
- fail-closed rate.
- average serialized row size.
- Parquet file count.
- manifest generation time.

## 13. Future Extension Points

Future extensions:

- add Exit Context as a fifth advisory archive layer.
- add operator-facing episode browser backed by Parquet or SQLite catalog.
- add DuckDB query examples for research notebooks.
- add incremental backfill by strategy family and year.
- add schema registry for advisory contracts.
- add row-level hashes for reproducibility.
- add source candle window fingerprints.
- add comparison builds across evaluator versions.
- add replay adapters that join advisory rows into backtest traces.
- add compaction jobs for tiny Parquet files.
- add runtime capture comparison, where live advisory artifacts can be compared against later historical replay without making either one authoritative.

The guiding principle remains unchanged: advisory archives explain historical context; they do not authorize trading behavior.
