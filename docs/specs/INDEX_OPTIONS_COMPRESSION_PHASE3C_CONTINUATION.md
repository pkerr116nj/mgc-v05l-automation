# Codex Continuation — Phase 3C Delivery Completion and Replay

Branch: `codex/ndxp-multidte-credit-research`

Phase 3C purchase submission is already complete. Do NOT create new paid requests.

Read:
- `docs/specs/INDEX_OPTIONS_COMPRESSION_PHASE3C_COMPLETE_EVENT_PANEL.md`
- `output/ndxp_multidte/program_v1/phase3c_complete_event_panel/submitted_jobs.json`
- `output/ndxp_multidte/program_v1/phase3c_complete_event_panel/acquisition_receipts.json`
- `output/ndxp_multidte/program_v1/phase3c_complete_event_panel/preflight_quote.json`
- `output/ndxp_multidte/program_v1/phase3c_complete_event_panel/research_report.md`

## Objective

Finish the already-authorized Phase 3C delivery and scientific replay from the existing 501 Databento batch jobs.

## Hard rule

**Reuse existing provider jobs only. Do not resubmit or repurchase any request already represented in submitted_jobs.json.**

If provider batch artifacts are not yet ready, poll existing jobs within the existing bounded polling logic. If some jobs fail provider-side, record them and stop for operator review rather than silently purchasing replacements.

## Steps

1. Resume `ndxp_panel_acquire` against saved job IDs.
2. Download all completed job artifacts into the existing immutable Phase 3C raw cache.
3. Verify checksums / provider metadata / exact request mapping.
4. Continue until:
   - all existing jobs are downloaded, or
   - provider-side failures are identified and explicitly reported.
5. Re-run exact-symbol/time coverage verification.
6. Run:
   - `ndxp_panel_analysis`
   - `ndxp_panel_report`
7. Produce all originally required scientific outputs:
   - coverage_after_acquisition.csv
   - entry_execution_results.csv
   - target_execution_results.csv
   - execution_model_comparison.csv
   - false_negative_analysis.csv
   - path_taxonomy.csv
   - settlement_results.csv
   - updated summary.json
   - updated research_report.md
   - updated completion_evidence.json
8. Run full Phase 3C regression suite.
9. Commit the completed Phase 3C results to the current branch.

## Completion requirements

Report:
- provider jobs completed / failed / pending;
- actual billed cost if provider exposes it;
- downloaded bytes;
- panel coverage;
- $6->$3 entry support;
- $6->$3 strong/plausible resting-target support;
- event crossings missed by minute sampling;
- persistence false negatives;
- event-supported expectancy / win rate / PF under each frozen model;
- 2DTE vs 3DTE;
- remaining queue/complex-order uncertainty;
- recommended next phase.

No new purchase, no subscription change, no SPX/QQQ/RUT acquisition, no live broker changes.
