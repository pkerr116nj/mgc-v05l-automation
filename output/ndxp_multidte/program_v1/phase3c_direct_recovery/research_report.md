# Phase 3C — direct recovery and frozen scientific replay

## Acquisition, cost and consolidation

Completed **60/60 direct requests** for the exact 60 frozen development sessions and 246 candidates. The original 501 batch jobs were not polled, cancelled, modified or resubmitted during this recovery; their prior receipts remain unchanged for archival reconciliation. This assignment explicitly authorized the separate direct-retrieval path despite those queued commitments.

The plan uses one entry-to-latest-expiry stream per pilot session, with only that session's exact selected legs. First/last bounds follow the exchange calendar; intermediate overnight and extended-session padding is harmless acquisition overhead and excluded from the regular-session replay. Symbol-count distribution: `{"10": 1, "11": 5, "12": 16, "5": 1, "6": 37}`. Requested regular-session minutes: 71,160; requested leg-symbol minutes: 618,386. Relative to the surgical manifest, extra unique regular leg-symbol minutes are 75,143.642, plus 3,507 duplicated across streams. Wall-clock envelope leg-symbol minutes, including closed-market padding: 2,098,076. Every original missing symbol/interval is covered by the consolidated envelopes.

**Conservative estimated new spend: $5.847243**, below the new **$10** ceiling. Exactly **12 metadata pricing calls** sampled six sessions. Method: stratify by prior pricing activity, select the largest symbol-minute exposure within each stratum, then apply twice the highest sampled cost per regular symbol-minute to the entire plan, including duplication. Projected billable bytes under the analogous stress estimate: 39,240,181,433. This is an uncertainty-aware extrapolation, not an exact provider quote for all 60 streams; no additional pricing calls were used. Sampling uses acquisition size/activity, not strategy outcomes.

**Actual new billed cost is not exposed by the SDK streaming response.** It is unknown, not zero and not the estimate. Acquisition wall time: **3824.4 seconds**; downloaded compressed DBN bytes: **3,679,853,774**. Each session had at most one direct network attempt. Valid cached files are reused, successful-transfer markers permit local validation recovery, and uncertain or partial paid attempts stop rather than redownload. Raw artifacts are immutable and ignored by Git. All session metadata, exact request mappings and SHA-256 hashes were verified. All 185,888,177 decoded records were checked against their exact requested symbols and receive-time bounds. Recorded warnings: 69; raw warning strings are preserved in acquisition receipts, including SDK resource warnings where present.

The permanent planner guardrail exposes cost, bytes, pricing-call count, retrieval count, a consolidation alternative and operational complexity. It rejects highly fragmented plans whose cost is small relative to the approved ceiling unless an explicit justification is supplied. This replaces byte-minimization as the default for small-cost research acquisitions.

## Coverage and limitations of completeness

Exact source intervals are complete for **246/246 candidates** and **60/60 sessions**. Strict Phase 3B freshness completeness is **1/246 candidates** and **0/60 sessions**. Entry freshness reaches 57 of 60 seconds for 97 candidates. Of 46,635 target-touch windows, 9,546 meet 95% fresh valid duration. `coverage_after_acquisition.csv` reports remaining source-leg seconds and observed fresh fractions for every candidate; window boundaries and fresh fractions are in `touch_window_evidence.parquet`.

Complete source delivery does not guarantee a quote update every second. The frozen policy rejects invalid/crossed/undefined states and pairs whose leg age exceeds one second. It requires 95% valid fresh path duration and 57/60 seconds at entry for strict completeness. Censored paths remain censored; no absence of evidence is converted into a failed fill. Coverage is evaluated only inside cash sessions, including early closes. No 2025/2026 holdout tuning or new instruments were introduced.

## Frozen execution assumptions

The rule hash is `70a22312ad5b7dbcb783a0333a0f504f710c9cefde5efc8dacd22e7a18ad80f3`, identical to the original Phase 3C file. No strategy definitions were optimized. Entry support means the first fresh valid midpoint at or above the nominal $5/$6/$7 cohort credit in the first minute. It is a hypothetical midpoint entry, not an observed complex fill. Event models use nominal credit minus $0.05, or minus $0.25 under conservative stress; minute models retain the original frozen midpoint minus $0.05. Entry tables disclose natural marketability, positive sizes, durations, quote ranges and repricing.

A hypothetical $3 BTC rests strictly after modeled entry. Strong evidence requires spread ask ≤$3 and positive executable-side leg sizes; strong/plausible also accepts midpoint ≤$3 with positive sizes on all four sides. Conservative evidence requires ask+$0.25 ≤$3 and applies $0.25 entry slippage. Event exit debit is the $3 limit. Only the first qualifying event closes each modeled candidate. If none is observed, an eligible scenario holds to owned XQC settlement; this is an assumption, not proof that a real order could not fill.

Event P/L requires supported entry and complete source intervals. The main table uses the same supported-entry subset for each model; all-candidate and strict-freshness-complete matched subsets are also reported. All P/L assumes 20 contracts and $1.324 per contract per side. Overlapping candidates are individual scenarios, not a portfolio/capital-capacity simulation. OPRA leg quotes do not establish simultaneous accessible liquidity, complex-order queue priority, routing latency or proven 20-lot fills.

## Primary $6→$3 findings

- 2DTE: 43/43 midpoint entries supported, but 0 naturally marketable at the nominal credit. Median entry spread quote width: $29.30. Conditional on supported entry: 16 strong, 43 strong/plausible, and 16 conservative target observations. Missed minute crossings: 45,185 midpoint and 105 ask. Event-supported touch windows rejected by 2/3-minute persistence: 193/307.
- 3DTE: 39/39 midpoint entries supported, but 0 naturally marketable at the nominal credit. Median entry spread quote width: $22.00. Conditional on supported entry: 15 strong, 39 strong/plausible, and 15 conservative target observations. Missed minute crossings: 32,757 midpoint and 105 ask. Event-supported touch windows rejected by 2/3-minute persistence: 190/293.

Neither DTE cohort has a strict-freshness-complete $6 candidate, so the strict matched $6 results are unavailable. None of the first strong or strong/plausible target observations has 20 contracts displayed on both executable exit legs. The observed positive-size rule is weaker than a 20-lot fill requirement. Wide leg-derived spread quotes make midpoint support especially fragile.

| DTE | Model | Trades | Mean P/L ($) | Win rate | PF |
|---|---|---|---|---|---|
| 2 | minute_touch_original_quote_terminal | 43 | 2,316.807 | 69.767% | 1.929 |
| 2 | minute_touch_settlement | 43 | 2,316.807 | 69.767% | 1.929 |
| 2 | persist_2_settlement | 43 | -2,345.053 | 39.535% | 0.512 |
| 2 | persist_3_settlement | 43 | -2,153.193 | 39.535% | 0.552 |
| 2 | event_strong | 43 | -2,796.216 | 37.209% | 0.438 |
| 2 | event_strong_plausible | 43 | 5,847.040 | 100.000% | undefined |
| 2 | event_conservative | 43 | -3,196.216 | 37.209% | 0.388 |
| 3 | minute_touch_original_quote_terminal | 39 | 795.758 | 58.974% | 1.244 |
| 3 | minute_touch_settlement | 39 | 716.271 | 58.974% | 1.215 |
| 3 | persist_2_settlement | 39 | -1,814.498 | 43.590% | 0.603 |
| 3 | persist_3_settlement | 39 | -1,745.268 | 43.590% | 0.619 |
| 3 | event_strong | 39 | -2,768.345 | 38.462% | 0.448 |
| 3 | event_strong_plausible | 39 | 5,847.040 | 100.000% | undefined |
| 3 | event_conservative | 39 | -3,168.345 | 38.462% | 0.398 |

The full $5/$6/$7 × 2DTE/3DTE matrix is retained in `execution_model_comparison.csv`; exact candidate outcomes are in `candidate_model_outcomes.csv`. Minute touch and consecutive 2/3-minute persistence use midpoint+$0.05 ≤$3. The original quote-terminal variant retains last midpoint+$0.05 capped at $10; other non-target exits use audited intrinsic settlement. This separates settlement corrections from execution assumptions.

## Sampling, persistence, paths and settlement

Across all cohorts, fresh continuous $3 midpoint/ask crossings absent from raw-minute ≤$3 observations in the same minute bin number **258,644/556**. Initial below-target observations after gaps are not counted as crossings. The equal $3 threshold isolates sampling; these are event counts, not extra winning trades.

There are **1,158/1,829** overlapping minute-touch windows with event support that fail 2/3-minute persistence. Conversely, **13,118/12,483** persistence-approved windows lack strong evidence. Windows are conditional on an entry but counterfactually retain an order even after earlier evidence; model P/L uses only the first fill. Absence of strong evidence is not proof of non-fill.

Fast $6→$5→$4→$3→rebound sequences within 60 seconds: **74,020**. Taxonomy: `{"adverse-then-success": 1, "censored/insufficient coverage": 245}`. Incomplete freshness paths retain extrema but are not labeled proven clean/adverse recoveries or failures.

All **246** settlements use owned, date/source-validated XQC observations. Source hashes and strike-based intrinsic values were reverified; **77** last-quote proxies differ from intrinsic. These are validations of the owned source labels, not a new independent official settlement acquisition.

## Decision and next phase

2DTE minute-settlement PF 1.929 falls to 0.438 under strong evidence and 0.388 under conservative stress; 3DTE minute-settlement PF 1.215 falls to 0.448 under strong evidence and 0.398 under conservative stress. Both strong models have negative expectancy and win rates below 40%. Conversely, the midpoint-plausible scenario has 100% modeled wins in both cohorts, with PF undefined because there are no modeled losses. That extreme divergence is execution-assumption sensitivity, not evidence of a guaranteed strategy. The $6 strict-freshness subset is empty, and neither natural entry marketability nor displayed 20-lot exit liquidity supports the optimistic interpretation.

The original five-year PF≈3.5 therefore looks materially optimistic relative to this panel's strong/conservative execution scenarios. This selected development panel is a different sample, so it cannot quantify the five-year PF's bias or establish whether actual complex fills would match any scenario. The original PF is not validated as executable at 20-lot size; midpoint-plausible profitability remains unproven.

Before SPXW expansion, preserve these thresholds and conduct frozen NDXP replay plus prospective paper/shadow observations with timestamped complex-order submissions, acknowledgements, partial fills and fills. Calibrate entry/exit evidence and the one-second freshness policy prospectively; do not tune on the 2025/2026 holdouts.

## Verification and reproduction

**189 tests pass.** Two full frozen replays produced byte-identical hashes for all ten scientific artifacts, with zero network calls during reproduction. All 2,430 original input hashes, prior manifests, batch-job receipts and frozen rules remain unchanged. The direct raw files pass local SHA-256 and metadata validation. No batch API calls, live trading-code changes, broker actions, subscription changes or new instruments occurred during recovery.

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_direct_recovery acquire
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_panel_analysis --direct
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_direct_report
```

Existing session artifacts are reused; uncertain partial streams require operator review. Large local DBN/Parquet caches are not committed.
