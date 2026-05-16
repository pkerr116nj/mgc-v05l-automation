# Historical Data Warehouse Backfill Pipeline

## Status

- Track: research infrastructure
- Document type: architecture/design proposal for review
- Implementation status: design only
- Scope: durable historical data warehouse and repeatable backfill pipeline
- Initial target period: mid-2019 through latest available historical coverage
- Initial universe: current Track B futures universe, including GC/MGC and the broader 11+ ticker set
- Future universe: stocks, ETFs, and optional crypto support

This design is research/offline only. It must not issue broker commands, restart runtime services, run lanes, run paper proof, mutate strategy behavior, stage generated outputs, or use live/runtime artifacts as inputs.

## Goals

The pipeline should make historical research repeatable instead of ad hoc. It should create a durable canonical 1m warehouse, deterministic derived timeframe warehouses, feature warehouses, and candidate/envelope archives that downstream research can reuse without rescanning live/runtime artifacts or compact strategy-study outputs.

Primary goals:

- Durable backfill from mid-2019, where minute granularity becomes consistently useful, through latest available historical coverage.
- Repeatable partitions by asset class, instrument, year, quarter, and timeframe.
- Explicit vendor, provenance, coverage, and quality metadata on every partition.
- A consistent canonical 1m base warehouse as the only base truth for derived research bars.
- Derived timeframe warehouses for 5m first, then 10m, 12m, 15m, and 30m after approved anchor rules exist.
- Feature, candidate, and envelope consumers that can regenerate Track B research substrates without using runtime artifacts.
- Idempotent dry-runs and partition rebuilds before any broad historical backfill is allowed.

Non-goals:

- This is not a broker integration.
- This is not a runtime lane path.
- This is not a strategy behavior change.
- This is not a PnL/backtest runner by itself.
- This does not promote research artifacts into runtime truth.

## Instrument Identity

Instrument identity must be explicit before data enters the warehouse. Every row and manifest should identify both the trading object and the normalized research identity.

### Futures

Required futures identity fields:

- `asset_class`: `futures`
- `symbol_root`: root such as `GC`, `MGC`, `MNQ`, `MES`, `CL`, `PL`, or `NG`
- `local_symbol`: vendor or exchange local symbol when available, such as `GCM4`
- `contract_month`
- `contract_year`
- `expiry`
- `exchange`
- `dataset`
- `instrument_id` or vendor instrument key when available
- `contract_mode`: `CONTRACT_SPECIFIC` or `CONTINUOUS`
- `continuous_symbol` when applicable
- `roll_policy_id` when continuous data is built
- `roll_adjustment_policy`: none, ratio, difference, or vendor-adjusted

Contract-specific data is preferred for raw vendor retention. Continuous series can be derived later only with an explicit roll policy. The first pass should avoid silent continuous/contract mixing. A GC/MGC research row should always make clear whether it came from a specific contract or a continuous construction.

### Stocks And ETFs

Required equity/ETF identity fields:

- `asset_class`: `stock` or `etf`
- `ticker`
- `exchange`
- `currency`
- `corporate_action_adjustment_policy`: raw, split-adjusted, split-and-dividend-adjusted, or vendor-adjusted
- `primary_listing`
- `vendor_symbol`

Equity and ETF support must not reuse futures roll semantics. Corporate action adjustment policy is the equity equivalent of futures roll policy and must be explicit in every manifest.

### Crypto

Crypto can be added later with:

- `asset_class`: `crypto`
- `venue`
- `base_asset`
- `quote_asset`
- `symbol`
- `timezone_policy`
- `session_policy`: likely continuous 24/7

Crypto should wait until futures and equities have stable source, partition, and validation contracts.

## Data Source Layer

### Futures

Databento is the preferred initial futures source when coverage and licensing support the target contracts. The source layer should separate raw vendor acquisition from canonical normalization.

Source acquisition responsibilities:

- Pull raw 1m vendor bars or tick-derived 1m bars when 1m bars are unavailable.
- Write raw vendor artifacts with immutable source metadata.
- Record vendor dataset, schema, instrument ids, request parameters, source coverage, and retrieval timestamps.
- Preserve source payloads long enough to reproduce canonical partitions.

### Equities And ETFs

Future equity/ETF source options should be evaluated before implementation. Candidate options include a paid market-data vendor with minute bars, official exchange/vendor feeds, or an existing internal research provider if licensing allows long-horizon storage. The design requires the same source coverage audit and adjustment policy regardless of vendor.

### Raw Vendor Artifact Retention

Raw source artifacts are research history, not runtime truth. They should be retained or cold-archived with enough metadata to rebuild canonical 1m partitions. Raw artifacts should not be read by runtime, preflight, operator dashboard, or broker paths.

### Source Coverage Audit

Every source pull should produce a coverage audit:

- requested instrument and time range
- delivered start/end
- missing days or partial days
- vendor warnings
- contract/roll metadata
- duplicate timestamps
- row count
- source artifact checksums

## Canonical Base Bars

The canonical base warehouse is the normalized 1m table. All derived research bars should come from it, not from strategy-study artifacts or runtime captures.

Required fields:

- `instrument`
- `symbol_root`
- `local_symbol` or `ticker`
- `asset_class`
- `timestamp_utc`
- `session_date`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `source`
- `source_start`
- `source_end`
- `data_quality_flags`
- `generated_at`

Additional recommended fields:

- `exchange`
- `currency`
- `vendor`
- `vendor_dataset`
- `vendor_schema`
- `vendor_instrument_id`
- `contract_mode`
- `roll_policy_id`
- `corporate_action_adjustment_policy`
- `source_artifact_path`
- `source_artifact_checksum`
- `partition_id`
- `code_version`

Canonical 1m bars must be completed bars. Incomplete bars belong in runtime capture or source staging, not canonical research partitions.

## Derived Timeframe Bars

The default derived timeframe is 5m. Future intervals such as 10m, 12m, 15m, and 30m are allowed only after an approved candle builder and anchor rules exist.

Required derived-bar fields:

- all identity fields from the base row
- `timeframe`
- `base_timeframe`: `1m`
- `timeframe_source`: `DERIVED`
- `aggregation_method`
- `anchor_rule`
- `completed_bar_only`
- `base_start`
- `base_end`
- `base_row_count`
- `expected_base_row_count`
- `gap_policy`
- `data_quality_flags`
- `generated_at`

Aggregation policy:

- open: first base 1m open
- high: max base high
- low: min base low
- close: last base 1m close
- volume: sum base volume
- timestamp: explicit end timestamp in UTC

Anchor policy must name the boundary rule. Example: `UTC_5M_BOUNDARY_END_TS`, or a session-aware rule if later needed. Silent mixed timeframe inputs must fail closed. A derived 10m, 12m, or 15m bar is valid only when `base_timeframe=1m`, `aggregation_method`, and `anchor_rule` are present.

Gap handling:

- `STRICT_COMPLETE`: emit only when all expected 1m bars are present.
- `ALLOW_MARKED_GAP`: emit with quality flags when research explicitly permits sparse bars.
- `DROP_INCOMPLETE`: omit incomplete derived bars and record the gap in the manifest.

The first pass should use `STRICT_COMPLETE` or `DROP_INCOMPLETE`, not silent partial aggregation.

## Feature Warehouse

The feature warehouse stores deterministic features computed from derived bars. It should start with `shared_features_5m`, then add `shared_features_10m`, `shared_features_12m`, `shared_features_15m`, and `shared_features_30m` only after the derived bars exist.

Initial feature families:

- ATR
- bar range
- body size
- close location
- VWAP or session VWAP
- EMA fast/slow
- velocity and velocity delta
- session label and session segment
- range expansion ratio
- stretch from VWAP/EMA
- Track B session features used by current candidate families

Required provenance fields:

- `feature_version`
- `feature_config_id`
- `input_partition_id`
- `input_timeframe`
- `base_timeframe`
- `anchor_rule`
- `generated_at`
- `code_version`
- `source_manifest_path`
- `data_quality_flags`

Feature partitions must be rebuildable from canonical base bars and derived bars. They must not require runtime state.

## Candidate And Envelope Warehouse

The candidate/envelope warehouse materializes setup metadata for all candidate bars and near-miss research rows.

Initial tables:

- `lane_candidates`
- family-specific candidate metadata tables
- enriched candidate/envelope archives for Entry Acceptance
- future participation context tables
- future exit context tables

The Asia breakout/retest/hold pilot needs fields such as:

- `candidate_family`
- `side`
- `candidate_flag`
- `current_exact_rule_flag`
- `breakout_breaks_prior_1_high`
- `signal_retests_and_holds_breakout_level`
- `breakout_level`
- `retest_depth`
- `hold_margin`
- `range_expansion_ratio`
- `close_location`
- `body_to_range_ratio`
- `churn_score`
- `snap_turn_conflict_strength`
- `source_feature_refs`
- `entry_acceptance_payload_version`

Compatibility targets:

- Entry Acceptance scoring should read candidate/envelope rows directly.
- Participation context can join on instrument, timestamp, timeframe, and candidate id.
- Exit profile research can join on candidate id, feature bar id, and derived-bar partition id.
- Backtest joins should use candidate id and timestamp, not compact accepted-entry strategy-study artifacts.

Candidate/envelope artifacts are research truth only. They do not create strategy authority or order intent.

## Partition Layout

Recommended durable layout:

```text
outputs/research_warehouse/base_1m/futures/GC/2024/Q1/bars.parquet
outputs/research_warehouse/derived_5m/futures/GC/2024/Q1/bars.parquet
outputs/research_warehouse/features_5m/futures/GC/2024/Q1/features.parquet
outputs/research_warehouse/candidates/futures/GC/2024/Q1/candidates.parquet
outputs/research_warehouse/envelopes/futures/GC/2024/Q1/entry_acceptance_envelopes.parquet
```

General pattern:

```text
outputs/research_warehouse/{dataset}/{asset_class}/{instrument}/{year}/Q{quarter}/{artifact}
```

Examples:

```text
outputs/research_warehouse/base_1m/futures/MGC/2024/Q2/bars.parquet
outputs/research_warehouse/derived_5m/futures/MGC/2024/Q2/bars.parquet
outputs/research_warehouse/features_5m/futures/MGC/2024/Q2/features.parquet
outputs/research_warehouse/candidates/futures/MGC/2024/Q2/candidates.parquet
```

Each partition directory should also contain a small `partition_manifest.json` or be referenced by a run-level manifest.

## Backfill Manifest

Each run should write:

- `manifest.json`
- `coverage_summary.json`
- `missing_bar_report.json`
- `source_provenance.json`
- `quality_checks.json`
- `row_counts.json`
- optional markdown summary

Required manifest fields:

- run id
- generated_at
- command
- code version / git head
- dirty worktree indicator
- source vendor and dataset
- requested instruments
- requested period
- produced partitions
- row counts by dataset/instrument/partition
- source coverage start/end
- quality check summary
- warnings
- error list

The manifest should make a run reproducible without depending on shell history.

## Validation

Minimum validation checks:

- row count expectations by instrument and session calendar
- duplicate timestamp checks per instrument/timeframe
- OHLC validity: high >= max(open, close), low <= min(open, close), high >= low
- non-negative volume
- gap diagnostics against expected 1m cadence
- session boundary checks
- derived bar reproducibility from base 1m
- derived bar base-row count checks
- feature null-rate checks
- candidate count sanity checks
- partition schema checks
- provenance completeness checks
- no mixed timeframe inputs

Validation should fail closed for missing required provenance or silent timeframe mixing. Warnings are acceptable for known market closures, but they must be explicit.

## Incremental Updates

The pipeline should support:

- appending newly available periods
- rebuilding a specific instrument/year/quarter partition
- rebuilding derived bars from unchanged base bars
- rebuilding features from unchanged derived bars
- rebuilding candidates/envelopes from unchanged features
- manifest-level idempotency checks

Idempotency requirements:

- Re-running the same command against the same source and code version should produce the same partition row counts and checksums.
- Rebuilds should write to a temporary staging path before replacing a partition.
- Partial failures should leave the previous partition intact.
- Hot-path runtime artifacts must not be read or overwritten.

No unbounded latest-state artifacts should be introduced. Durable research partitions are acceptable; unbounded hot-path JSON histories are not.

## First Dry-Run Recommendation

Do not run the full backfill until the design and dry-run command are reviewed.

Recommended first dry-run:

- instrument: `GC`
- asset class: `futures`
- period: `2024Q2`
- source: Databento futures historical 1m, if available
- stages: base 1m -> derived 5m -> shared features 5m -> candidate/envelope archive
- mode: dry-run, write generated outputs under `outputs/research_warehouse_dry_runs/`, not the durable warehouse

Proposed command for review before execution:

```text
./.venv/bin/python -m mgc_v05l.app.historical_data_warehouse_backfill \
  --mode dry-run \
  --asset-class futures \
  --instrument GC \
  --year 2024 \
  --quarter Q2 \
  --base-timeframe 1m \
  --derived-timeframe 5m \
  --source databento \
  --output-root outputs/research_warehouse_dry_runs \
  --stages base_1m,derived_5m,features_5m,candidates,envelopes
```

If GC Q2 source access is blocked, fallback dry-run should use MGC 2024Q2 only after confirming source availability. The dry-run should print the planned partitions, source coverage query, expected output files, and validation checks before writing anything.

## Risks And Open Questions

- Equity/ETF vendor: source, licensing, and historical minute retention must be chosen before equity backfill.
- Equity/ETF adjustment policy: split and dividend handling must be explicit before comparing long histories.
- Futures roll policy: continuous series design is required before multi-contract research can be treated as one instrument.
- Contract-specific vs continuous joins: Track B may need both raw contract-specific history and continuous research series.
- Storage volume: mid-2019 through current coverage across futures plus equities may require cold archive policy and disk planning.
- Computation time: full derived bars, features, and candidate archives should be chunked by partition and resumable.
- Source gaps: market closures and vendor gaps need explicit distinction.
- Existing research bundles: some broader 5m sources exist for GC, but they lack candidate/exact-flag metadata and should not be mixed into Entry Acceptance baseline comparisons without a rebuild.
- Backward compatibility: current `outputs/warehouse_historical_evaluator_*` artifacts are useful references but should not become the durable layout.
- Review gate: the first dry-run command should be reviewed before execution.
