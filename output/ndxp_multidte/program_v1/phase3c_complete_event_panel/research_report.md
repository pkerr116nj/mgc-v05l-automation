# Phase 3C — provider-blocked acquisition; assignment incomplete

The frozen manifest and bounded pricing are complete. The provider accepted all **501 exact-leg CMBP-1 batch jobs**, but only **0** were downloadable at the end of this attempt. Current provider states: `{"queued": 501}`. This continuation made **0 new paid requests**, using only the committed provider IDs, and performed **240 bounded status polls**. Provider/integrity errors: `[]`. **Execution analysis and completion evidence remain incomplete.** This report does not relabel Phase 3B results as new Phase 3C findings.

## Purchase authorization and evidence

Fresh quoted cost: **$2.123182499411**, below the explicitly authorized **$10.00** ceiling. Billable size: **14,248,436,560 bytes**. Pricing used **1019 metadata calls**, including 17 bounded timeout retries, with concurrency four and no download during pricing. The immutable final manifest SHA-256 is `9c38ac22de0f45cb4c79126776180e79e3a8e9cf4c179647e2efc87af1e74181`. It retains the exact 60 development sessions and 246 frozen candidates, corrects two early-close bounds, merges requests safely and subtracts already-owned exact-symbol intervals. A supplementary scan of 1,364 other DBNs found no missed CMBP-1 stores.

The accepted requests constitute authorized purchase commitments, not completed delivery. **Actual final provider charges are unknown** while the jobs expose null `cost_usd`; unknown cost is not reported as zero. No additional subscriptions, instruments, streaming fallback or duplicate purchases were used. `submitted_jobs.json` preserves all provider IDs, exact request parameters, receipt timestamps and warnings. `acquisition_receipts.json` records completion state and remaining provider jobs.

The initial submission pass encountered errors consistent with the documented [20-per-minute limit](https://databento.com/docs/reference-historical/basics/); the original exception records do not prove each HTTP status. Lost responses were reconciled against provider job listings; four receipts were recovered. Uncertain intents were resubmitted only after two provider snapshots, five seconds apart, showed no matching job and the original intent was at least 60 seconds old. The remaining submission pass was paced at 3.2 seconds and completed without further errors. Existing jobs are reused. Under the [provider's batch billing rules](https://databento.com/docs/faqs/usage-pricing-and-data-credits), retrieving the same submitted job does not require another purchase.

## Unfinished scientific work

Completed delivery: **0/501** requests. The new 60-session panel's source and freshness completeness cannot yet be verified. The Phase 3B requirements remain fixed: exact source intervals, ≥95% fresh valid paired path duration and ≥57/60 fresh entry seconds, with maximum leg age one second. Buying source coverage will not itself prove freshness completeness.

New $6→$3 supported-entry rates, resting-target evidence, missed event crossings, persistence false negatives, event-model expectancy/win rate/PF and 2DTE-versus-3DTE differences are **not available**. The original PF≈3.5 cannot be reassessed from undelivered data. The independent owned-XQC settlement audit is complete: 246 date/source joins and strike-based intrinsic payoffs verified against 3 unchanged source files. 77 last-quote proxies differ from intrinsic, by up to 10.0 spread points. `settlement_results.csv` contains genuine recalculated settlement results; its hold-to-settlement P/L assumes the frozen entry and does not prove entry or exit execution. No scientific CSV placeholders were created.

The replay and reporting code is implemented and tested on synthetic and owned decoder fixtures. Rules were frozen in `execution_rules.json` before purchased outcomes. Planned models retain minute touch, 2/3-minute persistence, strong event evidence, strong/plausible evidence and conservative explicit slippage. They distinguish hypothetical evidence-supported fills from proven 20-lot executions. OPRA leg quotes cannot establish complex-order queue priority, simultaneous accessible size, broker latency or actual complex fills.

## Independent frozen minute baselines

The four frozen minute/settlement models have been rerun for all 246 candidates using owned minute paths. These are **not event-supported results** and assume the original frozen entry. The table is the $6 cohort (43 2DTE and 39 3DTE candidates); it is a different sample from the original five-year PF≈3.5. Assumptions: frozen midpoint minus $0.05 entry, midpoint plus $0.05 exit on the $3 target, 20 contracts, and $1.324 per contract per side in fees. Persistence requires consecutive 2/3-minute observations. Non-target exits use owned XQC intrinsic, except the original quote-terminal model uses last midpoint plus $0.05 capped at $10. No conclusion about executable fills follows from this table. Full matrix and candidate outcomes are in `minute_baseline_comparison.csv` and `minute_baseline_outcomes.parquet`.

| DTE | Model | Trades | Mean P/L ($) | Win rate | PF |
|---|---|---|---|---|---|
| 2 | minute_touch_original_quote_terminal | 43 | 2,316.807 | 69.767% | 1.929 |
| 2 | minute_touch_settlement | 43 | 2,316.807 | 69.767% | 1.929 |
| 2 | persist_2_settlement | 43 | -2,345.053 | 39.535% | 0.512 |
| 2 | persist_3_settlement | 43 | -2,153.193 | 39.535% | 0.552 |
| 3 | minute_touch_original_quote_terminal | 39 | 795.758 | 58.974% | 1.244 |
| 3 | minute_touch_settlement | 39 | 716.271 | 58.974% | 1.215 |
| 3 | persist_2_settlement | 39 | -1,814.498 | 43.590% | 0.603 |
| 3 | persist_3_settlement | 39 | -1,745.268 | 43.590% | 0.619 |

## Validation and safe continuation

**175 tests pass**, including existing NDXP/Phase 3A/3B regressions, ceiling enforcement, merging/subtraction, no duplicate purchase, free resume, unknown pending cost, quote synchronization, coverage censoring, event models, settlement and determinism. Full SHA-256 checks confirm 2,430 original inputs and three Phase 3B source files are unchanged. The owned real-DBN decoder fixture validates integer price/nanosecond handling. No live trading code or broker actions were changed.

Run from the repository after the provider finishes:

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_acquire
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_analysis
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_report
```

Acquisition is bounded to 240 status polls by default (`--max-polls 1` gives one snapshot). The continuation command reads only the committed `submitted_jobs.json` IDs, validates their exact manifest mapping before contacting the provider, and has no submission path. Expired pricing does not prevent free retrieval of these existing jobs. Provider failures or request mismatches stop for operator review. Provider batch downloads expire, so retrieve promptly when ready. Local receipts and immutable raw/cache directories remain available and ignored by Git; compact submitted-job evidence is committed. The continuation can resume from committed `submitted_jobs.json` even if the ignored per-job receipts are unavailable. Never substitute a new paid stream for already-submitted batch jobs.

The immediate next phase is **finish this Phase 3C delivery and replay**, then assess a frozen NDXP prospective complex-order paper/shadow protocol before any SPXW expansion. No new purchase or holdout tuning is authorized by this report.
