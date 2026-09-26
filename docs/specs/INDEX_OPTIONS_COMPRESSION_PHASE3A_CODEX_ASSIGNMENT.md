# Codex Assignment — Phase 3A: Multi-Instrument Data Inventory and Acquisition Manifest

Branch: `codex/ndxp-multidte-credit-research`

Read and obey:

1. `docs/specs/INDEX_OPTIONS_COMPRESSION_RESEARCH_EXECUTION_PROGRAM_V1.md`
2. `docs/specs/NDXP_MULTIDTE_RICH_CREDIT_DEEP_DIVE_V1.md`

This assignment is **planning and inventory only**.

## Objective

Design the exact data foundation required to extend the compression research from NDXP to:

- NDXP
- SPX/SPXW
- QQQ
- RUT/RUTW

while also deepening NDXP execution research.

## Required work

### 1. Local inventory

Inspect the repository and available local research/data directories.

For each relevant instrument/data source found, record:

- symbol/instrument;
- schema/granularity;
- date range;
- row count or file count;
- storage size if practical;
- source;
- known data-quality issues;
- whether it is sufficient for structural, path, or execution research.

Do not mutate or delete anything.

### 2. Historical-access design

For each instrument, define the preferred Databento/other-supported data requests for three layers:

#### Structural layer
Long historical period, lower resolution.

#### High-resolution path layer
Second-level history around candidate periods.

#### Event-level execution layer
Targeted trade/quote/event windows around entries and exits.

For each layer specify:

- dataset;
- schema;
- symbol/stype;
- date range;
- expected request strategy;
- caching plan;
- whether full-chain or selected-contract retrieval is intended.

### 3. Instrument-specific mechanics

Document:

- correct parent/root symbols;
- settlement style;
- exercise style;
- expiration conventions;
- multiplier;
- relevant daily/weekly series;
- any differences that affect comparability.

Do not assume SPX, QQQ, RUT, and NDXP are mechanically identical.

### 4. Cost and request-complexity estimator

Build a **bounded estimator** that can price/sample the proposed acquisitions without buying them.

Requirements:

- no giant continuous OPRA query unless justified;
- no thousands of serial metadata calls;
- bounded concurrency;
- monthly/sample-based estimation where exact pricing would be too costly/slow;
- report estimated API-call count;
- report expected storage footprint;
- report likely dollar cost by instrument/layer.

If exact cost cannot be obtained safely, provide a labeled estimate and methodology.

### 5. Acquisition manifest

Produce:

`output/ndxp_multidte/program_v1/data_acquisition_manifest.json`

and a human-readable:

`output/ndxp_multidte/program_v1/data_acquisition_plan.md`

The manifest must separate:

- already-owned data;
- recommended new data;
- optional later data;
- unnecessary data.

### 6. Recommended sequencing

Provide a staged recommendation such as:

1. NDXP second-level targeted opening/target-crossing study;
2. SPX structural baseline;
3. QQQ structural baseline;
4. RUT structural baseline;
5. targeted high-resolution follow-up by whichever instruments show evidence.

Do not assume that is the final answer; derive it from cost/value.

### 7. Upgrade analysis

Assess whether a Databento plan upgrade appears economically justified.

Compare:

- usage-based acquisition;
- any relevant higher-tier historical access economics;
- expected reuse of data;
- storage/time implications.

Do not purchase or change subscriptions.

## Hard constraints

- No market-data purchase.
- No subscription change.
- No live broker actions.
- No modifications to live trading code.
- No deletion/overwrite of raw caches.
- No rule tuning on holdout data.
- No unbounded serial API loops.
- No large event-level download.

## Tests / verification

Add tests for any new estimator/planning code.

At minimum verify:

- estimator makes bounded calls;
- no download path is invoked;
- manifest schema is deterministic;
- instrument configuration is explicit;
- costs are labeled exact vs estimated.

## Completion response

Report:

1. files created/changed;
2. tests run and pass/fail counts;
3. data already available locally;
4. data gaps by instrument;
5. estimated cost by instrument/layer;
6. estimated storage by instrument/layer;
7. recommended acquisition order;
8. whether Databento upgrade appears justified;
9. anything requiring operator decision;
10. commit SHA.

Commit Phase 3A work to the current branch.
