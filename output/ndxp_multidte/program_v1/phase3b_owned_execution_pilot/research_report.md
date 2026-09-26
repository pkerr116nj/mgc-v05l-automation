# Phase 3B — owned-data execution pilot

## Decision

**PF ≈3.5 remains unresolved.** The maximum defensible fully covered execution cohort is **0 of 60 requested sessions**, a shortfall of 60. A separate, deterministic **60-session diagnostic cohort / 246 frozen candidates** uses the partial owned evidence. None of these candidates has a supported modeled entry or a complete replay. Missing data are not evidence of non-fill, and persistence failure is not evidence of non-fill.

Conditional exit quotes do support brief compression in some observed fragments. They do not establish whether the original entry filled, whether a resting complex order had queue priority, or whether 20 contracts could execute. This is Level 2/partial Level 3 evidence, not a validated strategy or realized trade count.

## Coverage and selection

Indexed 8,657 CMBP-related files by DBN headers, date-scoped instrument mappings and exact OSI symbols. The scan attempted 6,195 relevant decodes; 6,162 files yielded exact-contract events inside candidate windows. Quarantined files and decoding errors are listed in `store_inventory.json` and excluded. The coverage matrix retains all 3,738 original IDs, including 2025/2026 for coverage only; 2,790 have some observations of both legs. No candidate has 57 of 60 seconds of fresh paired entry coverage.

Full-chain opening acquisitions generally stop **at 09:31 exclusive**, the frozen entry time. Many later stores contain same-day-expiring contracts, which can overlap our candidates on expiry day, but do not cover their earlier entry. A symbol appearing in a header is never treated as proof of actual observations. `candidate_store_coverage.parquet` gives per-candidate, per-leg, per-store event counts and timestamps; absence means zero actual matching events. Header memberships, including stores with no observations, remain in `store_inventory.json`. Repeated timestamps across overlapping stores are disclosed separately from raw event counts.

The pilot is month-stratified from March 2023 through December 2024, with farthest-date spacing, and retains available $5/$6/$7 and 2DTE/3DTE candidates. Selection uses only dates and timestamp coverage, not prices or P/L. Candidates expiring after 2024-12-31 are excluded. The manifest and machine-readable classification rules were written and hashed before the final outcome pass; hashes were checked afterward. All 246 paths remain censored. No selection or threshold was optimized from these results.

88 candidates have some event overlap with a target-touch window. Across all cohorts, **4,128/46,635 (8.85%)** baseline minute-touch windows contain fresh valid synchronized event observations. This is fragment coverage, not continuous path coverage. `minute_window_coverage.parquet` also records each crossing into raw midpoint $3, baseline $2.95, and adverse entry+$0.50/$7/$8/$9, including candidates outside the pilot without computing their execution outcomes.

## Entry and exit evidence

The frozen baseline STO remains entry midpoint minus $0.05 at the original timestamp. It is a hypothetical fill assumption. The separate entry-support diagnostic searches the next 60 seconds for a fresh midpoint at the nominal cohort credit; it found zero modeled fills, so it changes no baseline trades. Entry tables retain first/last/median midpoint, bid/ask range, marketable/plausible durations, crossings, quote width, updates, and +5/15/30/60/120/300-second observations. Blank repricing values mean unavailable, never zero.

Receive-time synchronization uses the last observed state of each exact leg, requires both legs to be at most one second old, and caps duration at the next update or either leg's freshness deadline. Missing, stale, invalid, locked and crossed conditions are retained. Raw selected-leg events remain in the disposable local cache; compact crossing witnesses retain prices, sizes and trade context, and `second_aggregates.parquet` retains observed extrema and valid durations. Quotes do not persist across long gaps. Windows are conservative about a state first observed before their left boundary, so a boundary fragment can be undercounted by up to one second. Same-timestamp updates use the last receive/event-sorted update per leg. Single-leg prints do not prove a spread fill.

Under the **supported-entry replay**, all 46,635 minute touches are ambiguous: strong 0%, plausible 0%, ambiguous 100%, unlikely 0%.

Under the **separate conditional exit diagnostic**, assume the frozen baseline entry filled and the BTC order rested immediately afterward. Every touch window is evaluated as a potential resting-order opportunity, conditional on the order still being unfilled. Multiple windows can belong to the same candidate and must not be counted as separate executed trades. Classes are:

- Strong: 3,370 (7.226% of all minute touches).
- Plausible: 758 (1.625%).
- Ambiguous: 42,507 (91.148%), predominantly uncovered.
- Unlikely: 0 (0.0%).

Observed windows are heavily selected by the coverage of the earlier same-day-expiry studies and overlap in time. Their class frequencies must not be extrapolated to uncovered entries, paths, or the whole baseline.

The strong class is a sufficient legged-quote diagnostic (spread ask ≤$3 and positive executable-side sizes), not the primary complex-order price model. The plausible class uses midpoint ≤$3 and positive quoted sizes. Neither proves queue priority or a 20-lot fill. Unlikely requires ≥95% fresh window coverage and no bid/mid/ask crossing. No coverage cannot qualify as unlikely.

| DTE | Credit | Candidates | Minute touches | Conditional strong | Conditional plausible | Ambiguous | Unlikely |
|---|---|---|---|---|---|---|---|
| 2 | $5 | 43 | 11816 | 420 | 110 | 11286 | 0 |
| 2 | $6 | 43 | 7361 | 631 | 50 | 6680 | 0 |
| 2 | $7 | 43 | 5600 | 1071 | 179 | 4350 | 0 |
| 3 | $5 | 39 | 9450 | 382 | 111 | 8957 | 0 |
| 3 | $6 | 39 | 7463 | 586 | 271 | 6606 | 0 |
| 3 | $7 | 39 | 4945 | 280 | 37 | 4628 | 0 |

For the primary **$6→$3** family: 82 candidates, 14,824 minute touches; conditional strong 1,217, plausible 321, and ambiguous 13,286. There are zero complete entry-to-exit observations.

## False negatives and persistence

Conditional on the baseline entry, **12,016 confirmed event-level midpoint crossings** occur in minute bins with no raw midpoint ≤$3 observation. A confirmed crossing requires a continuous fresh previous state above $3; a quote first appearing below $3 after a gap is not counted. This measures sampling mismatches in covered fragments, not incremental winning trades or an unbiased miss rate. A separate baseline comparator uses midpoint+$0.05 ≤$3 and is labeled `conditional_missed_minute_mid_crossings`; it mixes sampling and the five-cent threshold difference and must not be interpreted as a pure sampling effect.

127 minute touches rejected by the forward 2-minute persistence test nevertheless have strong/plausible conditional evidence; the 3-minute count is 209. These tests are per-touch forward runs, not the candidate's eventual first persistence exit. Conversely, 41,834 / 40,953 persistence-approved touches lack strong evidence, chiefly because windows are missing; those counts are not failures to fill. For $6 only, the corresponding detailed counts are `{"conditional_fast_6_5_4_3_rebound_sequences": 849, "conditional_missed_minute_mid_crossings": 5121, "conditional_missed_raw_minute_ask_crossings": 193, "conditional_missed_raw_minute_mid_crossings": 4498, "conditional_p2_approved_not_strong": 13142, "conditional_p2_rejected_supported": 60, "conditional_p3_approved_not_strong": 12858, "conditional_p3_rejected_supported": 100}`.

Fast $6→$5→$4→$3→rebound sequences use a 60-second limit, allow a single update to sweep several levels, and reset at stale/invalid gaps. Conditional count: 2,440. They are listed in `fast_sequences.parquet`. The supported-entry replay cannot test these quantities because no entry is supported; zeros in its columns mean untestable here, not rejection of the hypothesis.

The path taxonomy is **246 censored/insufficient coverage**. Available event timing and extrema are retained, but none is promoted to clean compression, adverse recovery, severe recovery, or failure on an incomplete path. The completeness denominator conservatively uses 390 minutes per observed trading date, including shortened sessions; it cannot create false complete paths in this sample.

## Settlement audit

All 246 pilot expirations join owned XQC values with recognized source tags, finite positive values and no conflicting duplicates. Uncovered pilot expiry dates: []. The owned downloader's implementation explicitly requests FRED's `NASDAQXQC` series and writes the observed date and value without forward-fill; its source path is recorded in completion evidence. The 3 owned settlement files are hashed. This verifies local provenance and joins, not independent re-fetching of official daily values.

Put-vertical intrinsic is max(short strike − XQC, 0) − max(long strike − XQC, 0), bounded to $0–$10. **77** candidates differ from their last-quote proxy; maximum absolute difference is **$10**. Quote timestamp and age remain visible. No last quote silently replaces missing settlement. These differences concern hypothetical expiration outcomes, not a recalculated target-exit PF.

## Remaining gaps and minimum next acquisition

The missing foundation is post-09:31 entry coverage and the exact selected contracts' intervening trading sessions through expiry. Target-only snippets cannot resolve excursions missed entirely by minute sampling. The minimum proposed complete panel therefore uses only the pilot's exact symbols and regular-session intervals, removing already-owned header intervals. This is minimal for this fixed 60-session panel and coverage rule, not a claim that 60 sessions is a statistically sufficient experiment.

`minimum_acquisition_manifest.json` lists **501 explicit requests**, each at most 24 symbols, with timezone-aware bounds; no request was submitted. Total missing symbol-minutes: 543,335.36; owned symbol-minutes reused: 25,284.64. Existing owned intervals are not repurchased. No full-chain or full-market acquisition is proposed. Minute-calendar session bounds conservatively extend to 16:00 on half-days and should be shortened against the exchange calendar before authorization.

The saved Phase 3A provider quote was $0.001112389565 / 7,465,120 billable bytes for six exact NDXP legs over 30 minutes on 2024-06-12. Linear scaling gives **$3.36 central**, with a deliberately broad **$0.84–$33.58 planning envelope** (0.25×–10×), and 20.99 billable GiB central. Billable bytes are not compressed disk bytes. This is a sample-based estimate, not a binding price cap; differing quote intensity and provider rounding can change it. A small bounded metadata re-quote is required before any separately authorized purchase. No subscription upgrade is justified by this pilot.

No purchase, subscription change, broker action or live-code modification occurred. The operator decision is whether to authorize a separately reviewed acquisition; until then, the execution/PF question remains blocked by coverage, not by a demonstrated absence of opportunity.

## Reproduction and verification

Python commands run from the repository root, using the existing environment:

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_owned_index --scan-root /Users/patrick/Dev/MGC-v05l-ndxp-research/outputs --headers /tmp/phase3b_headers.json --cache /tmp/phase3b_events
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_owned_pilot --cache /tmp/phase3b_events
PYTHONPATH=src .venv/bin/python -m pytest -q tests/unit/test_ndxp_owned_execution.py tests/unit/test_ndxp_multidte*.py tests/unit/test_mgc_v05l_ndxp_naive*.py tests/unit/test_index_options_phase3a.py
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_owned_report --test-count 104
```

`--reuse-coverage` reruns the pilot outcome tables only after validating frozen input hashes, rules and owned-source size/mtime metadata. No command uses a data API. The extracted ~4.1 GiB event cache and full header cache are temporary, not committed. Compact outputs, research code and tests are committed.

**104 tests passed.** All output invariants passed. Full SHA-256 checks confirm 2,430 original input files unchanged; size/mtime checks confirm 8,656 indexed valid DBN sources unchanged. Evidence includes the manifest/rule hashes, output hashes and source checks. No 2025/2026 execution or conditional results were computed.
