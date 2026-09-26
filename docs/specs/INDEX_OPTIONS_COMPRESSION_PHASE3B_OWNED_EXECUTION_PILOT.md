# Codex Assignment — Phase 3B: NDXP Owned-Data Execution Pilot

Branch: `codex/ndxp-multidte-credit-research`

Read and obey:
1. `docs/specs/INDEX_OPTIONS_COMPRESSION_RESEARCH_EXECUTION_PROGRAM_V1.md`
2. `docs/specs/NDXP_MULTIDTE_RICH_CREDIT_DEEP_DIVE_V1.md`
3. `docs/specs/INDEX_OPTIONS_COMPRESSION_PHASE3A_CODEX_ASSIGNMENT.md`
4. `output/ndxp_multidte/program_v1/data_acquisition_plan.md`
5. `output/ndxp_multidte/program_v1/data_acquisition_manifest.json`

## Objective

Use **owned local NDXP data first** to determine how much high-resolution evidence already exists for the rich-credit 2DTE/3DTE compression opportunity, especially the question:

> When a resting BTC target is near $3, are one-minute midpoint target touches typically supported by genuine intraminute quote/trade behavior, or are they transient/non-executable artifacts?

This is an evidence-building pilot, not a strategy-optimization exercise.

## Scope

Development period only for conditional/execution analysis: through 2024-12-31.

Primary frozen family:
- NDXP short put verticals
- 10-point width
- literal 2DTE / 3DTE
- opening credit cohorts approximately $5 / $6 / $7
- primary diagnostic focus $6 -> $3
- preserve the existing candidate IDs and frozen baseline definitions

Do not tune rules using 2025 or 2026.

## Phase 0 — exact owned-data coverage index

Before any analysis, index the existing local NDXP stores identified in Phase 3A.

For every frozen candidate, determine:
- exact short and long OSI/raw symbols;
- entry timestamp/window;
- expiration;
- calendar DTE;
- whether each owned CMBP-1 store contains each leg;
- exact start/end timestamps;
- event counts;
- gaps;
- overlap with the entry window;
- overlap with each one-minute target-crossing/adverse window;
- settlement availability.

Produce a candidate-level coverage matrix.

Do not infer coverage from filenames. Inspect DBN/header/symbol content.

## Pilot cohort

Construct a deterministic 60-session development-only pilot **before examining execution outcomes**.

Requirements:
- month-spaced / stratified across 2023-03-28 through 2024-12-31;
- include 2DTE and 3DTE;
- include $5/$6/$7 cohorts;
- ensure the $6 cohort is adequately represented;
- selection based on date/coverage/stratification, not future P/L;
- save the selected session/candidate manifest before outcome analysis.

If owned data cannot support 60 sessions, use the maximum defensible cohort and report the shortfall. Do not buy missing data.

## Reconstruct high-resolution spread state

For covered candidates, reconstruct each leg from CMBP-1 and derive synchronized spread-level state at the finest defensible resolution.

At minimum preserve:
- event timestamps;
- bid/ask;
- sizes where present;
- spread bid, ask and midpoint;
- quote width;
- trade events/context where available;
- stale/unmatched leg state;
- crossed/locked/invalid conditions;
- second-level aggregates derived from events.

Do not forward-fill across long gaps.

## Entry analysis

Determine whether the frozen minute-level opening credit corresponds to high-resolution executable states.

For each candidate measure:
- first/last/median spread midpoint around entry;
- spread bid/ask range;
- duration/time fraction where a $5/$6/$7 STO limit would be marketable or plausibly fillable;
- number of crossings of the intended entry credit;
- immediate 5s/15s/30s/60s/2m/5m repricing;
- quote width and update intensity.

Keep entry executability distinct from exit executability.

## Resting BTC target analysis

Model a BTC order as **resting immediately after the modeled STO fill**, rather than submitted after observing a target print.

For $6 -> $3 primary analysis, and retain $5/$6/$7 matrix:
- locate every minute-level target touch;
- inspect the underlying event window around it;
- measure time at/through the target;
- count target crossings;
- determine whether spread ask, midpoint, and bid cross the target;
- inspect leg quote sizes and trades where informative;
- measure next-event / +1s / +5s / +15s / +30s / +60s behavior.

Create evidence classes, not fake certainty:
1. **strong fill evidence**
2. **plausible fill evidence**
3. **ambiguous**
4. **unlikely fill**

Define the classification rules before computing aggregate outcomes and save them in machine-readable form.

Do not claim OPRA proves complex-order queue position or a 20-lot fill.

## False-negative analysis

Explicitly test the user's microstructure hypothesis:

> A real fast sweep may cross $3 and permit a resting fill even when a 1-minute persistence rule rejects the trade or minute sampling misses the excursion.

Measure:
- event-level target crossings not represented by a minute target touch;
- one-minute touches rejected by 2/3-minute persistence but supported by event-level crossing evidence;
- persistence-approved touches lacking strong event-level support;
- fast sequences resembling $6 -> $5 -> $4 -> $3 -> rebound.

## Path taxonomy

For the pilot classify:
- clean compression;
- adverse-then-success;
- severe adverse ($9+) then success;
- failure/no target;
- censored/insufficient coverage.

For each category report event-level path characteristics and timing.

## Settlement and terminal-value audit

Use owned XQC settlement data where valid:
- verify source/date joins;
- compare last-quote proxy with intrinsic settlement;
- identify uncovered expiration dates;
- do not silently substitute terminal quote for official settlement.

## Outputs

Write under:
`output/ndxp_multidte/program_v1/phase3b_owned_execution_pilot/`

Required:
- `coverage_matrix.csv`
- `pilot_manifest.json`
- `execution_classification_rules.json`
- `entry_microstructure.csv`
- `target_microstructure.csv`
- `false_negative_analysis.csv`
- `path_taxonomy.csv`
- `settlement_audit.csv`
- `summary.json`
- `research_report.md`
- `completion_evidence.json`

Prefer Parquet additionally for large event-derived tables; do not commit huge decoded raw event files.

## Decision output

The report must answer:

1. How much of the 60-session pilot was covered by already-owned data?
2. How often do minute-level target touches have strong/plausible/ambiguous/unlikely event-level fill evidence?
3. How often does event data reveal target crossings that minute data missed?
4. How many touches rejected by persistence nevertheless show plausible/strong resting-order evidence?
5. Does the existing PF~3.5 touch result look materially overstated, materially understated, or unresolved after event evidence?
6. What exact data gaps remain?
7. What is the **minimum next acquisition** needed to resolve them?
8. Give a bounded cost estimate for that minimum acquisition, but do not purchase it.

## Hard constraints

- No paid acquisition.
- No subscription changes.
- No live broker/order actions.
- No live trading-code modifications.
- No 2025/2026 rule tuning.
- No changing the frozen candidate definitions to improve results.
- No assumption that midpoint touch equals fill.
- No assumption that persistence failure equals non-fill.
- No claim of queue priority/20-lot execution without evidence.
- Preserve raw caches read-only.
- Avoid unbounded API/metadata loops.

## Tests

Add tests for:
- deterministic pilot selection;
- coverage indexing;
- event synchronization;
- no long-gap forward fill;
- resting-order target logic;
- classification determinism;
- false-negative detection;
- development-only enforcement;
- settlement joins.

Run the relevant existing NDXP tests as regression tests.

## Completion response

Report:
1. commit SHA;
2. files changed/created;
3. tests/pass counts;
4. owned-data coverage;
5. primary $6 -> $3 execution findings;
6. false-negative/persistence findings;
7. settlement findings;
8. remaining data gaps;
9. minimum next acquisition and bounded estimated cost;
10. blockers/operator decisions.

Commit all code, compact derived outputs, and reports to the current branch.
