# Phase 3C — provider-pending acquisition; assignment incomplete

The frozen manifest and bounded pricing are complete. The provider accepted all **501 exact-leg CMBP-1 batch jobs**, but only **0** were downloadable at the end of this attempt. Current provider states: `{"queued": 501}`. **Execution analysis and completion evidence remain incomplete.** This report does not relabel Phase 3B results as new Phase 3C findings.

## Purchase authorization and evidence

Fresh quoted cost: **$2.123182499411**, below the explicitly authorized **$10.00** ceiling. Billable size: **14,248,436,560 bytes**. Pricing used **1019 metadata calls**, including 17 bounded timeout retries, with concurrency four and no download during pricing. The immutable final manifest SHA-256 is `9c38ac22de0f45cb4c79126776180e79e3a8e9cf4c179647e2efc87af1e74181`. It retains the exact 60 development sessions and 246 frozen candidates, corrects two early-close bounds, merges requests safely and subtracts already-owned exact-symbol intervals. A supplementary scan of 1,364 other DBNs found no missed CMBP-1 stores.

The accepted requests constitute authorized purchase commitments, not completed delivery. **Actual final provider charges are unknown** while the jobs expose null `cost_usd`; unknown cost is not reported as zero. No additional subscriptions, instruments, streaming fallback or duplicate purchases were used. `submitted_jobs.json` preserves all provider IDs, exact request parameters, receipt timestamps and warnings. `acquisition_receipts.json` records completion state and remaining provider jobs.

The initial submission pass encountered errors consistent with the documented [20-per-minute limit](https://databento.com/docs/reference-historical/basics/); the original exception records do not prove each HTTP status. Lost responses were reconciled against provider job listings; four receipts were recovered. Uncertain intents were resubmitted only after two provider snapshots, five seconds apart, showed no matching job and the original intent was at least 60 seconds old. The remaining submission pass was paced at 3.2 seconds and completed without further errors. Existing jobs are reused. Under the [provider's batch billing rules](https://databento.com/docs/faqs/usage-pricing-and-data-credits), retrieving the same submitted job does not require another purchase.

## Unfinished scientific work

Completed delivery: **0/501** requests. The new 60-session panel's source and freshness completeness cannot yet be verified. The Phase 3B requirements remain fixed: exact source intervals, ≥95% fresh valid paired path duration and ≥57/60 fresh entry seconds, with maximum leg age one second. Buying source coverage will not itself prove freshness completeness.

New $6→$3 supported-entry rates, resting-target evidence, missed event crossings, persistence false negatives, event-model expectancy/win rate/PF and 2DTE-versus-3DTE differences are **not available**. The original PF≈3.5 cannot be reassessed from undelivered data. Existing owned XQC sources are available for all 246 candidates, but the settlement-based Phase 3C rerun has not been performed. No scientific CSV placeholders were created.

The replay and reporting code is implemented and tested on synthetic and owned decoder fixtures. Rules were frozen in `execution_rules.json` before purchased outcomes. Planned models retain minute touch, 2/3-minute persistence, strong event evidence, strong/plausible evidence and conservative explicit slippage. They distinguish hypothetical evidence-supported fills from proven 20-lot executions. OPRA leg quotes cannot establish complex-order queue priority, simultaneous accessible size, broker latency or actual complex fills.

## Validation and safe continuation

**130 tests pass**, including existing NDXP/Phase 3A/3B regressions, ceiling enforcement, merging/subtraction, no duplicate purchase, free resume, unknown pending cost, quote synchronization, coverage censoring, event models, settlement and determinism. Full SHA-256 checks confirm 2,430 original inputs and three Phase 3B source files are unchanged. The owned real-DBN decoder fixture validates integer price/nanosecond handling. No live trading code or broker actions were changed.

Run from the repository after the provider finishes:

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_acquire
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_analysis
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_report
```

Acquisition is bounded to 240 status polls by default (`--max-polls 1` gives one snapshot). It reuses all locally saved job receipts even after the quote expires; any new purchase still requires a fresh quote. Provider batch downloads expire, so retrieve promptly when ready. Local receipts and immutable raw/cache directories remain available and ignored by Git; compact submitted-job evidence is committed. If these local receipts are lost, reconstruct them from `submitted_jobs.json` before resuming. Never substitute a new paid stream for already-submitted batch jobs.

The immediate next phase is **finish this Phase 3C delivery and replay**, then assess a frozen NDXP prospective complex-order paper/shadow protocol before any SPXW expansion. No new purchase or holdout tuning is authorized by this report.
