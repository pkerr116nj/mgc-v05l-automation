# Track B Advisory Layer Backfill v1 Design

Status: architecture/design only. This document does not add a runtime producer, broker activity, lane execution, paper activity, generated outputs, historical batch launch, Parquet generation, or SQLite migration.

Key decision: v1 uses episode-centric backfill first, not global-bar backfill. Advisory states are attached to exact-baseline trade episodes and bounded replay windows before any broader market-state archive is considered.

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

V1 should be optimized for the first target:

- instruments: GC and MGC.
- strategy family: exact baseline.
- years: 2020 through 2026.
- replay window: 48 bars after entry.
- base market data: canonical 1m Parquet hot research cache when available, with SQLite retained as source/provenance.
- derived market data: deterministic replay windows derived from the hot cache, not from runtime captures.

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

V1 should explicitly avoid producing lifecycle, participation, regime, or sizing rows for bars that are not part of an entry candidate, trade episode, or bounded replay window. A later global-bar regime archive may be useful, but it is not the first implementation.

## 3. Recommended Storage

### Parquet

Use Parquet as the primary historical archive format.

Reasons:

- efficient columnar scans for research.
- good compression for repeated enum labels and context fields.
- natural partitioning by strategy family, instrument, year, and layer.
- works with Python, DuckDB, Polars, Spark, and pandas.

Parquet files should contain advisory state rows, not raw candle data. Raw market bars should remain in their existing source datasets and be referenced through provenance fields.

The preferred source for historical bars is the hot research cache:

```text
outputs/research_warehouse/base_1m/futures/<SYMBOL>/<YEAR>/Q<q>/bars.parquet
```

SQLite remains the provenance source. Advisory backfill manifests should record both the Parquet partition paths used for speed and the original SQLite source metadata carried by those partitions.

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
    layer=lifecycle_awareness/strategy_family=exact_baseline/instrument=MGC/year=2020/part-000.parquet
    layer=participation_pressure/strategy_family=exact_baseline/instrument=MGC/year=2020/part-000.parquet
    layer=regime_session/strategy_family=exact_baseline/instrument=MGC/year=2020/part-000.parquet
    layer=sizing_position_management/strategy_family=exact_baseline/instrument=MGC/year=2020/part-000.parquet
  episodes/
    strategy_family=exact_baseline/instrument=MGC/year=2020/episodes.parquet
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
- `base_timeframe`
- `derived_timeframe`
- `session_bucket`
- `episode_id`
- `candidate_id`
- `position_id`
- `replay_window_id`
- `episode_sequence`
- `entry_side`
- `entry_price`
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
- `input_parquet_partition_path`
- `input_partition_manifest_path`
- `source_sqlite_path`
- `input_row_hash`
- `source_window_hash`
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

The episode index should be a separate reusable table with one row per exact-baseline episode:

- `episode_id`
- `strategy_family`
- `instrument`
- `entry_timestamp`
- `entry_side`
- `entry_price`
- `fixed_exit_timestamp_36b`
- `episode_year`
- `source_candidate_id`
- `source_trade_id`
- `source_backtest_run_id`
- `source_parquet_partitions`
- `episode_status`
- `episode_failure_reasons`

The advisory layer rows should reference `episode_id`; they should not duplicate all episode metadata.

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

When using `outputs/research_warehouse/base_1m`, provenance must include:

- base 1m partition path.
- base 1m partition manifest path.
- source SQLite path from the base partition manifest.
- base export run id.
- query bounds used to assemble the 48-bar window.
- whether any derived 5m or higher timeframe window was produced during backfill.

If a required base partition is missing, stale, inconsistent with its manifest, or fails row/timestamp validation, the advisory build must fail closed for that episode batch.

## 7. Replay And Backtest Integration Points

The archive should integrate with replay/backtest tools at episode boundaries.

Expected integration points:

- entry-candidate replay: attach Entry Acceptance, Participation / Pressure, Regime State, and Initial Sizing context at candidate time.
- position replay: attach Lifecycle Awareness, Exit Context, Participation / Pressure, Regime State, and In-Position Size Management at each evaluation offset.
- exit-candidate replay: compare fixed 36b, adaptive 24/36, participation-driven exits, and advisory reduce/hold labels.
- attribution notebooks: join advisory states to realized outcome, MFE, MAE, drawdown, time-in-trade, session, and regime.
- operator UI prototypes: browse advisory histories for representative episodes without touching live runtime paths.

Replay consumers should read the archive as advisory annotations. They should not treat it as a strategy authority source.

Backtests should join advisory rows by `episode_id` and `evaluation_timestamp`, not by raw bar timestamp alone. This avoids accidental use of advisory states from unrelated candidate families or global market contexts.

Fixed 36b replay should first consume:

- `episode_index` for exact-baseline entries and fixed exits.
- Lifecycle Awareness rows for bars 0 through 48.
- Participation / Pressure rows aligned to each evaluation timestamp.
- Regime State rows aligned to each evaluation timestamp.
- Sizing / Position Management rows at entry and in-position offsets.

Adaptive 24/36 replay can later compare its decisions against the same advisory timeline without changing the baseline archive.

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

For the first target, annual layer partitions should be sufficient:

```text
outputs/track_b_execution_core/advisory_backfill/parquet/
  layer=lifecycle_awareness/strategy_family=exact_baseline/instrument=GC/year=2024/part-000.parquet
  layer=participation_pressure/strategy_family=exact_baseline/instrument=GC/year=2024/part-000.parquet
  layer=regime_session/strategy_family=exact_baseline/instrument=GC/year=2024/part-000.parquet
  layer=sizing_position_management/strategy_family=exact_baseline/instrument=GC/year=2024/part-000.parquet
```

If annual files become too large, shard by quarter using the same year-quarter convention as the base 1m cache. Do not shard by episode unless a specific consumer needs small random reads.

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
- source bars: read from `outputs/research_warehouse/base_1m/futures/GC` and `outputs/research_warehouse/base_1m/futures/MGC` when partitions exist and validate.
- fallback source: SQLite-backed canonical 1m only for missing or invalid hot-cache partitions, and only through a read-only export/rebuild path reviewed separately.

Why this target:

- exact baseline provides the cleanest reference behavior.
- GC/MGC continuity supports enough episode density for useful distributions.
- 2020-2026 includes varied volatility, trend, compression, and session regimes.
- 48 bars covers newly opened, working, fixed 36b exit horizon, late decay, and post-exit comparison windows.

The first run should be a dry-run design implementation with tiny sample output, then a bounded yearly build, then the full 2020-2026 build only after manifest validation.

Suggested first implementation order:

1. Build a pure episode indexer for exact-baseline GC/MGC episodes.
2. Build a dry-run advisory backfill planner that resolves required base 1m partitions and estimates advisory row counts.
3. Backfill one symbol/year sample to a temporary reviewed output root.
4. Backfill GC/MGC 2020-2026 only after validation and manifest review.

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

Expected first-target sizing should be calculated from the exact-baseline episode index, not from raw 1m bar counts. A simple planning formula is:

```text
episode_count * 49 evaluation points * 4 advisory layers
```

The 49 evaluation points include entry offset 0 plus 48 post-entry bars. For example:

- 1,000 episodes -> 196,000 advisory rows.
- 5,000 episodes -> 980,000 advisory rows.
- 10,000 episodes -> 1,960,000 advisory rows.

Enum-heavy Parquet rows should remain modest. Expect tens to low hundreds of megabytes for GC/MGC exact-baseline v1 unless full nested evaluator payloads are retained per row. Store compact nested payloads only when they are needed for audit.

The 21-symbol futures universe is a later expansion. The base 1m hot cache currently makes it feasible to plan, but advisory row count should still be based on episode density, not global bars. If 21-symbol exact-baseline episodes are roughly 5x to 10x GC/MGC, expect about 10 million to 20 million advisory rows for the same 49-point, four-layer archive.

## 13. Future Extension Points

Future extensions:

- add Exit Context as a fifth advisory archive layer.
- expand from GC/MGC exact-baseline to the 21-symbol futures universe after episode-index validation.
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
