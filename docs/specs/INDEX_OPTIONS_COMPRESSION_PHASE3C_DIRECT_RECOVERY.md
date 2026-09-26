# Codex Recovery Assignment — Phase 3C Direct Retrieval

Branch: `codex/ndxp-multidte-credit-research`

Read and obey:
1. `docs/specs/INDEX_OPTIONS_COMPRESSION_PHASE3C_COMPLETE_EVENT_PANEL.md`
2. `docs/specs/INDEX_OPTIONS_COMPRESSION_PHASE3C_CONTINUATION.md`
3. `output/ndxp_multidte/program_v1/phase3c_complete_event_panel/final_request_manifest.json`
4. `output/ndxp_multidte/program_v1/phase3c_complete_event_panel/submitted_jobs.json`
5. `output/ndxp_multidte/program_v1/phase3c_complete_event_panel/preflight_quote.json`

## Objective

Bypass the provider-side queued batch bottleneck and complete the frozen 60-session Phase 3C panel using **direct historical retrieval**.

The existing 501 batch jobs must remain untouched and must not be resubmitted.

## Core recovery design

Replace the dependency on 501 queued batch jobs with a consolidated direct-retrieval plan:

- same frozen 60 development sessions;
- same exact selected option legs;
- same time bounds needed for the Phase 3C execution study;
- at most **one direct historical request per pilot session** whenever technically possible;
- merge all same-session symbols/time intervals aggressively;
- tolerate harmless overlap/duplicate bytes to reduce job count and wall-clock time;
- cache one immutable DBN artifact per session or another similarly coarse unit.

Priority order:
1. correctness,
2. elapsed time,
3. operational simplicity,
4. reasonable cost,
5. byte efficiency.

Do not optimize for minimum bytes if doing so materially increases request count or runtime.

## Existing 501 batch jobs

Hard rules:
- Do not cancel them.
- Do not resubmit them.
- Do not wait on them.
- Do not treat them as the active acquisition path.
- Preserve their receipts for later archival reconciliation.

## Step 1 — Build consolidated session manifest

From the frozen Phase 3C manifest:
- group requests by pilot session;
- union exact symbols required that day;
- union/merge requested intervals conservatively;
- preserve exchange-session boundaries;
- allow extra harmless covered time if it reduces fragmentation;
- target <=60 direct requests total.

Write:
`output/ndxp_multidte/program_v1/phase3c_direct_recovery/session_request_manifest.json`

Report:
- original fragment count;
- consolidated request count;
- symbol count distribution;
- total requested minutes;
- extra overlap introduced versus the surgical manifest.

## Step 2 — Coarse bounded price check

Do **not** individually price every session.

Use a bounded estimator based on a small representative sample of consolidated sessions, stratified by activity and request size.

Requirements:
- no more than 12 metadata pricing calls total;
- extrapolate a conservative total estimate;
- label methodology and uncertainty;
- hard new-spend ceiling: **$10.00**.

If the conservative estimated total exceeds $10.00, STOP for operator review.

If <= $10.00, proceed immediately.

## Step 3 — Direct historical retrieval

Use Databento direct historical retrieval, not batch jobs.

Preferred:
`client.timeseries.get_range(...)`

Requirements:
- at most one retrieval per consolidated session request;
- bounded concurrency;
- safe resume;
- immutable per-session DBN cache;
- no duplicate re-download of valid completed session artifacts;
- no huge CSV expansion;
- verify exact symbols/timestamps after retrieval;
- preserve quality warnings.

Recommended raw namespace:
`output/ndxp_multidte/program_v1/phase3c_direct_recovery/raw/`

## Step 4 — Coverage verification

Re-run exact candidate coverage against the newly downloaded direct-retrieval cache plus already-owned local data.

Report:
- entry support coverage;
- path coverage;
- target-window coverage;
- full candidate/session completeness;
- any remaining missing sessions/symbols.

## Step 5 — Complete Phase 3C scientific analysis

Once coverage is adequate, run the frozen Phase 3C analysis without changing strategy definitions.

Produce:
- entry_execution_results.csv
- target_execution_results.csv
- execution_model_comparison.csv
- false_negative_analysis.csv
- path_taxonomy.csv
- settlement_results.csv
- summary.json
- research_report.md
- completion_evidence.json

Use the same frozen execution models:
1. minute touch;
2. 2-minute persistence;
3. 3-minute persistence;
4. strong event-supported resting BTC;
5. strong+plausible event-supported resting BTC;
6. conservative event-supported/slippage stress.

Primary focus: $6->$3, with full $5/$6/$7 x 2DTE/3DTE matrix retained.

## Required decision outputs

Answer:
1. consolidated direct-request count;
2. new quoted/estimated spend;
3. actual new spend if exposed;
4. wall-clock acquisition time;
5. downloaded bytes;
6. panel completeness;
7. $6->$3 event-supported entry findings;
8. $6->$3 target-fill evidence;
9. minute-sampling misses;
10. persistence false negatives;
11. event-supported expectancy/win rate/PF;
12. 2DTE vs 3DTE;
13. remaining queue/complex-order uncertainty;
14. recommended next phase.

## New permanent architecture rule

Add a reusable acquisition-planning guardrail to the research code/tests:

If projected acquisition cost is small relative to the approved ceiling, the planner must prefer materially fewer/larger requests over surgical fragmentation.

At minimum, planning output must include:
- projected cost,
- projected bytes,
- metadata call count,
- provider/download request count,
- consolidation alternative,
- expected operational complexity.

Add a test that rejects highly fragmented low-cost plans unless fragmentation is explicitly justified.

## Hard constraints

- New-spend ceiling: $10.00.
- No new batch submissions.
- No subscription changes.
- No SPX/QQQ/RUT acquisition.
- No live broker actions.
- No live trading-code changes.
- No holdout tuning.
- Preserve all prior raw caches and receipts.

## Tests

Add/extend tests for:
- session consolidation;
- <=12 pricing calls;
- spend ceiling;
- safe resume;
- no duplicate retrieval;
- fragmented-plan rejection;
- coverage verification;
- frozen execution-model reproducibility.

Run all relevant Phase 3A/3B/3C regression tests.

## Completion response

Report:
1. commit SHA;
2. files created/changed;
3. tests/pass counts;
4. consolidated request count;
5. estimate and actual spend;
6. acquisition time;
7. coverage;
8. $6->$3 findings;
9. persistence/missed-touch findings;
10. remaining uncertainty;
11. next phase.

Commit all code and compact outputs to the current branch.
