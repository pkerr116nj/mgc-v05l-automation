# Phase 3A: index-options data acquisition plan

Planning only. No downloads, purchases, subscription changes, broker actions, live-code changes or holdout tuning. All proposed purchases remain subject to operator approval.

## Decision

Reuse the existing NDXP event and settlement stores first, then run a small development-only execution pilot. The prior midpoint edge deteriorated under persistence, so additional broad history has less immediate value than resolving whether transient marks can support fills. SPXW is the first new structural comparator, then QQQ, then RUTW. Do not upgrade Databento yet.

## Available locally

Scanned 17,688 market/research files across four named roots: 411.7 GiB logical footprint. Copies, derived CSVs and overlaps are included, not unique purchased bytes. DBN headers were sampled first/middle/last by directory; file counts and sizes are exact at scan time, coverage is not inferred from filenames alone. Full provenance is in local_inventory.json.

| Store | Files/rows | Dates | Use and limitation |
|---|---|---|---|
| Frozen NDXP multi-DTE | 3,738 candidates; 8,979,954 minute option rows; 913/915 path sessions | 2021-09-24–2026-09-24 requested | Structural baseline owned; nine candidates missing paths, two failed sessions |
| NDXP event openings | 873 CMBP-1 files, 40.95 GiB | 2023-03-28–2026-09-18 | Full-parent 09:30–09:31 samples; header end is exclusive, so do not assume coverage at 09:31 entry |
| NDXP stop paths | 665 CMBP-1 files, 44.06 GiB | 2023-03-28–2026-01-08 | Selected-contract intraday paths, sampled symbols predominantly same-day expiry; do not assume 2/3DTE coverage |
| Other NDXP studies | Definitions, later-entry snapshots and selected event paths | Mainly 2024–2026 | Metadata counts in inventory; quarantine excluded; exact contract/time intersection still required |
| XQC settlement | 873 daily rows | 2023-03-28–2026-09-18 | Local source-tagged settlement CSV exists; verify holidays/source and join to expirations before replacing terminal proxies |
| QQQ equity | 681 one-second DBN windows plus two minute-history files | 2024-01-02–2026-09-18 | XNAS.ITCH equity, not QQQ options; one-second windows roughly first 90 cash minutes |
| VIX | 1,663 daily rows, 50,455 bytes | 2020-01-02–2026-07-01 | Official-Cboe-source-tagged parquet; use prior available close for morning features |
| NQ/MNQ/ES/MES | Local 1m backfill parquet plus earlier coverage audits | Physical backfill mainly 2026-04–07; MNQ/MES also 2019 | Earlier audits report ~2.22m bars each, 2020–2026-04, but backing store not re-counted; roll/gaps need audit |
| SPX/SPXW, QQQ options, RUT/RUTW | No historical option cache identified in scoped search | Unknown | All three option layers missing; absence claim is scoped to named roots |

One running terminal diagnostic JSONL may grow externally during inventory. The scanner opens raw caches read-only, records size/mtime changes, and hashes key evidence and sampled cache prefixes. It neither stops live processes nor treats their logs as a historical option feed.

## Instrument mechanics

| Instrument | Parent/root | Exercise and settlement | Expirations/comparability |
|---|---|---|---|
| NDXP | NDXP.OPT; preferred NDXP | European; cash, PM; official XQC; multiplier 100 | Currently Mon–Fri including PM monthly series; use actual historical listings and holidays. Keep NDX AM/XQO separate. Frozen 10-point put vertical, literal 2/3 calendar DTE, credits 5/6/7. Earlier root-history extension needs listing audit; 2016 sample failed and is not zero-cost evidence. [ndxp](https://www.nasdaq.com/NDX_NDXP_Factsheet) [ndxp_history](https://nasdaqtrader.com/MicroNews.aspx?id=2018-04) |
| SPX | SPX.OPT, SPXW.OPT; preferred SPXW | European; SPX traditional AM (SET); SPXW PM cash; multiplier 100 | SPX traditional third-Friday monthly; SPXW daily/weekly, PM third-Friday and end-of-month where listed. AM last trading day generally precedes settlement. Use SPXW for PM comparison. SPX AM is a separately budgeted optional cohort; never silently merge roots. [spx](https://www.cboe.com/tradable_products/sp_500/spx_weekly_options/specifications) [settlement](https://www.cboe.com/index_settlement_values/weeklys_settlement_values/) |
| QQQ | QQQ.OPT; preferred QQQ | American; physical delivery of normally 100 ETF shares; early exercise/assignment possible; multiplier 100 | Monthly and weekday short-term expirations where historically listed; verify holidays, adjusted contracts and deliverables. QQQ equity OHLCV is not option history. Dividends, early assignment, pin/after-hours risk, corporate actions and physical delivery break index-settlement equivalence; a 10-point width is not equal underlying-normalized risk. [qqq](https://www.nasdaq.com/products/north-american-markets/nasdaq-100-options-xnd-ndx) |
| RUT | RUT.OPT, RUTW.OPT; preferred RUTW | European; RUT AM (RLS); RUTW PM cash; multiplier 100 | RUT monthly third Friday; RUTW weekly/EOM and Mon–Fri daily suite since January 8, 2024. Older specification page lists Friday only; dated daily-launch notice resolves that omission. Historical daily availability differs; keep AM RUT separate. Smaller underlying level changes relative width and risk. [rut](https://www.cboe.com/tradable_products/ftse_russell/russell_2000_index_options/rut_specifications) [rut_daily](https://ir.cboe.com/news/news-details/2023/Cboe-TO-OFFER-DAILY-EXPIRIES-FOR-RUSSELL-2000-INDEX-OPTIONS-SUITE-BEGINNING-JANUARY-8-2024/default.aspx) [settlement](https://www.cboe.com/index_settlement_values/weeklys_settlement_values/) |

### Gaps and supporting sources

**NDXP:** Two multi-DTE minute path sessions missing (2026-09-22/23). Owned event windows require exact 2/3DTE symbol/time intersection; sampled old stop paths are 0DTE and openings often stop before 09:31. XQC CSV covers 2023-03-28..2026-09-18; frozen baseline settlement dates outside this span remain gaps. No verified cash NDX intraday store found; reuse NQ/MNQ and QQQ proxies with timestamp/roll checks.

**SPX:** No SPX/SPXW option history or official settlement series identified. SPXW PM first; AM SPX optional separately. ES/MES minute proxies exist, not cash SPX second-level or complex-book data.

**QQQ:** No QQQ option history identified; owned QQQ equity is venue-specific context only. Equity history before 2024 and after 2026-09-18 not verified in scoped caches. Dividend, OCC adjustment and assignment/deliverable records need validation; no index cash-settlement shortcut.

**RUT:** No RUT/RUTW option history or official settlement series identified. No verified cash RUT/RTY/IWM tape in scoped inventory; do not assume NQ or ES is interchangeable. Daily-expiry coverage changes in 2024 and historical AM/PM series must remain separate.

The manifest also specifies settlement-history and underlying-context gap requests. These ancillary external requests are unquoted, not priced as zero or included in the option-layer totals; local reuse costs zero acquisition dollars. Daily official settlement series should be under 1 MiB each. No ancillary retrieval was performed.

Parent requests use stype=parent; selected options use exact historical OSI raw symbols and stype=raw_symbol. Preserve padded symbols, daily mappings and adjusted deliverables; parent names do not make settlement conventions interchangeable. [Databento symbology](https://databento.com/docs/examples/options/equity-options-introduction/using-parent-symbology-to-fetch-an-option-chain)

## Date and resolution boundaries

Catalog metadata captured for this assignment reports CBBO-1m/definitions from 2013-04-01, CMBP-1/TCBBO from 2023-03-28, and direct CBBO-1s only from 2025-02-20. Published general history is not a per-schema guarantee. Before 2023-03-28 OPRA history is subsampled, with different timestamp/venue limitations. Development-era seconds must come from CMBP-1, not a nonexistent 2024 CBBO-1s request. [OPRA specification](https://databento.com/docs/venues-and-datasets/opra-pillar)

Initial structural comparators use 2021-09-24–2026-09-24. Optional extensions use 2013-04-01 onward for SPXW/QQQ/RUTW, and 2018-02-02 onward for a conservative NDXP weekly-root design pending listing audit. The failed 2016 NDXP sample is not treated as free data. Earlier AM NDX is not a substitute. Freeze development through 2024, validation in 2025 and holdout in 2026 before new conditional work.

## Cost, storage and request budget

43 actual metadata calls (3 catalog + 40 sample), maximum four workers, 20-second HTTP timeout, no retries and zero download calls. The reusable estimator rejects workloads above 64 calls before sending anything; cache hits do not call the API. Failures are listed in the manifest, never replaced with zero.

Costs below are USD low / central / high **estimates**, not purchase quotes. Structural estimates scale three era-spaced parent opening samples (two for NDXP) plus a dense-minute selected-leg and definition budget. High-resolution estimates use one measured NDXP selected-leg size anchor with explicit activity scenarios for other instruments: those cross-instrument multipliers are assumptions, not measured quotes. Small windows can be overestimated by the provider; use full 10-minute bins for final approval estimates. [Metadata pricing semantics](https://databento.com/docs/api-reference-historical?historical=http)

| Instrument/layer | Scope | USD low / central / high | Billable GiB central | Compressed GiB low / central / high | Future request cap |
|---|---|---|---|---|---|
| NDXP_structural | 2-session gap only | 0.020 / 0.040 / 0.080 | 0.017 | 0.002 / 0.004 / 0.009 | 6 |
| NDXP_path | 60-session pilot | 0.067 / 0.267 / 2.670 | 1.669 | 0.042 / 0.417 / 8.343 | 240 |
| NDXP_execution | 60-session pilot | 0.076 / 0.306 / 3.060 | 0.224 | 0.006 / 0.056 / 1.119 | 480 |
| SPX_structural | 1,254-session structural | 13.847 / 27.693 / 55.386 | 12.010 | 1.201 / 3.002 / 6.005 | 3762 |
| SPX_path | 60-session pilot | 0.334 / 1.335 / 13.349 | 8.343 | 0.209 / 2.086 / 41.715 | 240 |
| SPX_execution | 60-session pilot | 0.112 / 0.448 / 4.484 | 1.114 | 0.028 / 0.278 / 5.568 | 480 |
| QQQ_structural | 1,254-session structural | 10.729 / 21.457 / 42.915 | 8.892 | 0.889 / 2.223 / 4.446 | 3762 |
| QQQ_path | 60-session pilot | 0.334 / 1.335 / 13.349 | 8.343 | 0.209 / 2.086 / 41.715 | 240 |
| QQQ_execution | 60-session pilot | 0.112 / 0.448 / 4.484 | 1.114 | 0.028 / 0.278 / 5.568 | 480 |
| RUT_structural | 1,254-session structural | 9.585 / 19.170 / 38.340 | 7.748 | 0.775 / 1.937 / 3.874 | 3762 |
| RUT_path | 60-session pilot | 0.134 / 0.534 / 5.340 | 3.337 | 0.083 / 0.834 / 16.686 | 240 |
| RUT_execution | 60-session pilot | 0.085 / 0.342 / 3.416 | 0.446 | 0.011 / 0.111 / 2.231 | 480 |

Path and execution budgets overlap: execution CMBP windows are a subset of the path pilot and must not be added twice; only TCBBO trade-context costs are incremental. Raw compressed storage is not the billable size; billing is based on uncompressed binary bytes. Reserve another 2–4× compressed capacity for derived Parquet, indexes and temporary decoding, and avoid broad CSV expansion. Network lower bound at 100 MB/s is 10 seconds per GB; symbol selection, request latency and decode work can dominate.

The unit-price metadata returned 2 for CBBO-1m/1s, 0.16 for CMBP-1, 210 for TCBBO, and 5 for definitions; cost/size sample pairs reconcile using 2^30 bytes per quoted unit. TCBBO is expensive per byte and one NDXP two-minute sample had zero trades: never extrapolate that zero across the pilot.

### Request design by layer

Structural: one preferred-root parent chain at 09:30–09:32 ET per listed trading day, then only selected put legs through expiry; root-filter daily definitions for listing/deliverables. At most three requests/session before reuse, with four workers. Full chains are confined to opening discovery and definitions; no continuous multi-year event query.

Path: 60 development sessions, deterministic month-spaced dates fixed before outcomes. Up to 12 legs; 30 minutes around opening plus up to three 10-minute crossing/control windows, 60 minutes/session total. Up to 240 requests. Reconstruct second-level NBBO from local/acquired CMBP for development dates; use direct CBBO-1s only where its coverage permits and no owned event window already suffices.

Execution: four two-minute entry/target/adverse/control windows per session, preserving quote/trade conditions and sizes. Up to 240 CMBP and 240 TCBBO requests standalone; reuse covered CMBP from the path pilot. The same windows must include time-matched failures/controls to avoid survivorship bias. OPRA cannot establish complex-order queue or 20-contract fills; proprietary complex-book data or prospective observations are a separate later gate. [OPRA feed scope](https://databento.com/docs/venues-and-datasets/opra-pillar)

Cache all approved new requests by canonical dataset/schema/root/date/symbol/time hash in immutable DBN; save request manifests, hashes, receipts and quality warnings. Index exact contract/time coverage before any request, merge overlapping windows and retain originals. No acquisition function is implemented in this assignment.

### Optional longer structural history

| Instrument | Additional session planning assumption | Estimated USD low / central / high | Compressed GiB low / central / high |
|---|---|---|---|
| NDXP | 900 | 9.028 / 18.055 / 36.110 | 0.771 / 1.927 / 3.855 |
| SPX | 2138 | 23.608 / 47.215 / 94.430 | 2.048 / 5.119 / 10.238 |
| QQQ | 2138 | 18.292 / 36.584 / 73.167 | 1.516 / 3.790 / 7.580 |
| RUT | 2138 | 16.342 / 32.684 / 65.368 | 1.321 / 3.303 / 6.605 |

Extension session counts are coarse planning assumptions, not verified exchange calendars; actual listings, holidays, gaps and prior ownership determine the final manifest. AM SPX/RUT cohorts and proprietary complex-book sources are optional later and require separate instrument-specific quotes; no invented vendor prices are assigned.

## Acquisition order

1. 0. Index existing NDXP headers/contracts/time windows and XQC settlement dates; audit exact 2/3DTE overlap, repair only approved gaps. No acquisition cost to reuse.
2. 1. NDXP development-only 60-session second/event pilot: disprove transient-mark fills before expanding history. Reuse first; new selected windows only after approval.
3. 2. SPXW structural baseline 2021-09-24..2026-09-24, PM only; best like-for-like cash-settled comparator.
4. 3. QQQ structural baseline with exercise/dividend/physical-delivery model frozen first; cheaper Nasdaq-linked control but mechanically different.
5. 4. RUTW structural baseline; preserve pre/post daily-expiry listing differences.
6. 5. Only instruments retaining evidence receive 60-session path/execution pilots; earlier structural history is optional, not an automatic purchase.

## Upgrade economics

The initial option-layer sequence has a central scenario of approximately $68.90, after avoiding duplicate NDXP execution CMBP. This excludes optional historical extensions, ancillary unquoted gaps and proprietary complex data. A $199 subscription would need to remove more payable cost than this central scenario; it is not justified merely for this staged acquisition.

Keep usage-based access for now. The OPRA-specific announcement advertises Standard at $199/month; confirm today’s account offer and which historical schemas/dates it includes. The generic page renders CME pricing, so its Plus/Unlimited figures are not used for OPRA. Current OPRA Plus/Unlimited commitment and license quotes remain unverified. [OPRA plan announcement](https://databento.com/blog/introducing-new-opra-pricing-plans)

Compare incremental subscription/fees over its full commitment against only unique payable data after reuse. Metadata get_cost respects existing plan discounts, so do not mistake a zero sample for universally free data or buy a plan for access already included. Repeated local replay has no provider charge; another subscription does not solve timestamp, settlement, physical-delivery or complex-fill evidence gaps.

## Operator decisions before any acquisition

- Approve a concrete selected-contract/date manifest and spending cap before any purchase.
- Confirm current OPRA plan, fees, older-history entitlements and Plus/Unlimited quotes; no upgrade now.
- Approve 60-session development pilot definition and prioritization; no strategy selection from 2026.
- Confirm portable cache location/disk headroom and source licensing; do not move/delete existing caches.
- Choose whether/when to fund optional AM cohorts, older structural history, other-instrument event pilots or proprietary complex-book data.

## Reproduce and verification

The checked-in JSON evidence makes plan regeneration entirely offline. The inventory CLI reads named roots only. The estimator CLI is offline unless --online is explicit, and accepts only a fixed metadata endpoint allowlist. See completion_evidence.json for tests and exact changed files.

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.index_options_acquisition_plan --evidence-dir output/ndxp_multidte/program_v1 --output-dir /tmp/index-options-phase3a-rebuild
PYTHONPATH=src .venv/bin/python -m pytest -q tests/unit/test_index_options_phase3a.py
```
