# Codex Assignment — Phase 3C: Re-Quote and Acquire Complete 60-Session NDXP Event Panel

Branch: `codex/ndxp-multidte-credit-research`

Read and obey:
1. `docs/specs/INDEX_OPTIONS_COMPRESSION_RESEARCH_EXECUTION_PROGRAM_V1.md`
2. `docs/specs/INDEX_OPTIONS_COMPRESSION_PHASE3B_OWNED_EXECUTION_PILOT.md`
3. `output/ndxp_multidte/program_v1/phase3b_owned_execution_pilot/minimum_acquisition_manifest.json`
4. `output/ndxp_multidte/program_v1/phase3b_owned_execution_pilot/research_report.md`

## Objective

Complete the frozen 60-session development-only NDXP event panel for the exact selected option legs needed to resolve entry-to-exit executability.

This phase may purchase data, but only after a bounded fresh preflight quote and only below the hard ceiling defined here.

## Scope

Frozen panel only:
- the deterministic 60 development sessions from Phase 3B;
- exact frozen candidate IDs;
- exact 2DTE / 3DTE selected legs;
- no 2025/2026 conditional expansion;
- no new strategy tuning;
- no new instruments.

Use the existing Phase 3B minimum acquisition manifest as the starting point. Recompute only if required by exchange calendar correction, cache subtraction, or deterministic request merging.

## Step 1 — Finalize request manifest

Before pricing or downloading:
- validate all 501 planned requests against the actual exchange calendar;
- correct half-day session ends;
- merge overlapping requests for the same symbol/date where safe;
- subtract all already-owned exact-symbol event intervals;
- ensure no full-chain or parent-level event download remains;
- produce a final immutable request manifest and SHA-256.

Write:
`output/ndxp_multidte/program_v1/phase3c_complete_event_panel/final_request_manifest.json`

## Step 2 — Fresh bounded cost quote

Use Databento metadata pricing only.

Requirements:
- bounded concurrency <= 4;
- no serial thousands-of-calls pattern;
- cache quote results;
- disclose exact metadata call count;
- no download during this step;
- calculate expected billable size and cost.

### Hard purchase ceiling

**Do not purchase if the fresh estimated total exceeds USD $10.00.**

If the estimate is <= $10.00, proceed automatically to acquisition.

If the estimate is > $10.00, STOP and report the exact estimate and drivers. Do not purchase.

This ceiling applies to the unique missing Phase 3C event data only.

## Step 3 — Acquire missing event data

If and only if the preflight estimate is <= $10.00:

- download exact selected legs only;
- schema: CMBP-1 unless a clearly equivalent owned/preferred schema is justified;
- preserve raw DBN files immutably;
- use canonical cache keys by dataset/schema/date/symbol/time;
- bounded concurrency;
- resume safely from partial completion;
- no duplicate repurchase of cached intervals;
- save receipts/response metadata/quality warnings;
- do not decode to huge CSVs.

Recommended namespace:
`output/ndxp_multidte/program_v1/phase3c_complete_event_panel/raw/`

## Step 4 — Coverage verification

After acquisition:
- re-run exact-symbol/time coverage indexing;
- verify each frozen candidate has entry support and full required event-path coverage according to the Phase 3B freshness rules;
- report remaining gaps;
- do not fabricate completeness.

## Step 5 — Re-run execution analysis

Using the now-complete panel:

Re-run the Phase 3B logic for:
- entry executability;
- resting BTC target evidence;
- event-level target crossings;
- persistence false negatives;
- fast $6->$5->$4->$3->rebound sequences;
- clean/adverse/severe/failure path taxonomy;
- settlement-based terminal outcomes.

Primary focus remains $6->$3, with the full $5/$6/$7 x 2DTE/3DTE matrix retained.

## Step 6 — Execution models

Report at least these distinct models:

1. **Minute midpoint touch** baseline.
2. **2-minute persistence** stress.
3. **3-minute persistence** stress.
4. **Event-supported resting BTC**:
   - strong evidence only;
   - strong + plausible evidence.
5. **Conservative event-supported** model with explicit slippage stress.

Do not equate legged OPRA quotes with guaranteed complex-order queue priority. Keep "supported by market evidence" distinct from "proven 20-lot fill."

## Step 7 — Outputs

Write under:
`output/ndxp_multidte/program_v1/phase3c_complete_event_panel/`

Required:
- `final_request_manifest.json`
- `preflight_quote.json`
- `acquisition_receipts.json`
- `coverage_after_acquisition.csv`
- `entry_execution_results.csv`
- `target_execution_results.csv`
- `execution_model_comparison.csv`
- `false_negative_analysis.csv`
- `path_taxonomy.csv`
- `settlement_results.csv`
- `summary.json`
- `research_report.md`
- `completion_evidence.json`

Parquet is preferred for large derived tables.

## Decision questions

The final report must answer:

1. What was the actual fresh quoted cost and billable size?
2. How much was actually purchased?
3. What proportion of the 60-session panel is now complete?
4. For $6->$3, how often does the modeled entry have event-level support?
5. Conditional on supported entry, how often does a resting $3 BTC have strong/plausible event evidence?
6. How many event-level $3 crossings are missed by minute sampling?
7. How often do persistence tests reject event-supported opportunities?
8. What does the event-supported expectancy / win rate / profit factor look like under clearly stated assumptions?
9. How do results differ between 2DTE and 3DTE?
10. Does the original PF~3.5 minute-touch result appear materially overstated, materially understated, or reasonably representative?
11. What uncertainty remains specifically because OPRA lacks complex-order queue/fill data?
12. What should be the next research step before expanding to SPXW?

## Hard constraints

- Hard purchase ceiling: $10.00.
- No subscription change.
- No live broker/order actions.
- No live trading-code changes.
- No expansion to 2025/2026 tuning.
- No SPX/QQQ/RUT purchase in this phase.
- No rule optimization.
- No overwrite/delete of prior raw caches.
- No unbounded API loops.

## Tests

Add/extend tests for:
- final request merging and cache subtraction;
- price ceiling enforcement;
- safe resume;
- no duplicate repurchase;
- coverage verification;
- event-supported execution model calculations;
- settlement handling;
- deterministic outputs.

Run all relevant existing NDXP and Phase 3A/3B regression tests.

## Completion response

Report:
1. commit SHA;
2. files changed/created;
3. tests/pass counts;
4. fresh estimate and actual spend;
5. completed coverage;
6. $6->$3 event-supported findings;
7. persistence false-negative findings;
8. settlement findings;
9. remaining microstructure uncertainty;
10. recommended next phase.

Commit all code, compact outputs and reports to the current branch.
