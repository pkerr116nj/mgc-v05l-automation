# Phase 3D — acquisition started; stopped on observed budget risk

**Status: incomplete, blocked on the authorized $100 ceiling.** The acquisition code is implemented and tested, and the first broad opening session was downloaded and validated. No expanded entry-to-expiry panel or morphology optimization has been performed.

## Scope and architecture

The manifest covers 877 exchange sessions from 2023-03-28 through 2026-09-24, OPRA.PILLAR CMBP-1, NDXP.OPT, with calls and puts. The full and opening manifests are alternative coarse capture plans, not submitted jobs. Exactly two direct requests were attempted. No batch API was called.

The first operational check requested one full session, with a server record cap of 100 million. Databento returned HTTP 504 before any file arrived. The second request used the explicitly authorized broad-opening alternative, 09:30–10:00 ET. It completed successfully. We did not retry the failed request or redownload a completed unit.

The immutable raw file preserves quotes, sizes, trades, flags and event timestamps. The first session contains 43,625,045 verified NDXP records and 13,744 observed symbols. All record receive times lie within the requested opening bounds. 2,962 symbols and 13,132,959 records lie within 0–5 calendar DTE. No wrong-universe records were found. First/last timestamps and exact raw SHA-256 are preserved in `acquisition_evidence.json`.

A durable intent precedes every paid request. Successful-transfer markers permit local validation recovery. Completed SHA-verified artifacts are reused; uncertain attempts stop instead of being silently retried. A single-writer file lock serializes the ledger. Each request reserves its maximum bounded exposure before contacting the provider. Raw files, individual operational receipts and transient locks are excluded from Git; compact aggregate evidence is committed.

## Budget control and reason for stopping

No cost estimator, get_cost call, get_billable_size call, or extra metadata sampling was run. One inexpensive `list_unit_prices` lookup returned $0.16/GB for historical-streaming CMBP-1. The SDK's CMBP-1 record size is 80 bytes. The provider supports a server-side record limit and bills uncompressed binary data; see [Databento historical API documentation](https://databento.com/docs/api-reference-historical?historical=http).

The code uses decimal GB and includes raw metadata in completed-byte accounting, making this a conservative metered upper value rather than an invoice. Each 100-million-record request reserves $1.28256, including a 16 MB metadata allowance. Before new requests, it checks the remaining $100 budget and sufficient disk space for the bounded stream plus 5 GB. A unit-rate snapshot older than 24 hours blocks new paid requests pending a fresh single-rate lookup.

The completed opening file is **983,074,708 compressed bytes** and **3,490,801,526 uncompressed bytes including metadata**. Its metered upper cost is **$0.558528**. The failed full-session attempt retains a conservative **$1.28256** reservation; combined completed metering plus uncertain reservation is **$1.841088**. Actual invoice charges are not exposed and remain unknown.

The observed 0–5 DTE subset alone represents **$0.168102** of opening records at that rate. Across 877 openings at the same density, this is **$147.43 before any selected-contract paths**. The broad parent alternative at the observed density is approximately **$489.83** and 862 GB compressed. Only about 25 GiB of local storage remained during this check.

These are arithmetic checks on already-purchased data, not another provider quote or representative cost-estimation exercise. One session does not prove the full-period bill. It does provide concrete evidence that even the simple 0–5 DTE opening alternative could exceed $100, meeting the assignment's explicit stop condition. No further paid requests were started. `budget_stop.json` prevents new CLI acquisition requests while that stop remains unresolved.

## Progress and coverage

- Opening sessions completed: **1/877**, date 2023-03-28.
- Complete expanded entry-to-expiry sessions: **0/877**.
- Failed direct requests: **1**, HTTP 504, full-session alternative.
- Successful opening download time: **262.9 seconds**.
- Successful opening including validation: **367.6 seconds**.
- Elapsed from first request start through opening validation: **498.9 seconds**.
- Completed opening sessions by year: **2023: 1; 2024: 0; 2025: 0; 2026: 0**.

Observed 0–5 DTE **record counts**, not spread-candidate counts:

| Calendar DTE | Side | Opening records |
|---|---|---:|
| 0 | C | 1,233,070 |
| 0 | P | 1,334,859 |
| 1 | C | 1,574,322 |
| 1 | P | 1,526,680 |
| 2 | C | 1,610,224 |
| 2 | P | 1,719,524 |
| 3 | C | 1,884,590 |
| 3 | P | 2,249,690 |

For this Tuesday session, calendar DTE 4 and 5 fall on the weekend; there are no such NDXP expirations in the captured chain. This is not a claim that those DTEs are unavailable on other sessions.

Candidate counts by year/DTE/side/opening-credit region are **not yet available**, not zero. The expanded spread selector and selected-contract subsequent paths have not been built/acquired after the budget stop. Consequently this checkpoint does not claim coverage of the approximately $2-through-first-$10 surface over all 877 sessions or a complete 5/10/20-point vertical panel. The acquired raw opening is reusable for that work.

## Research semantics and unchanged benchmark

The frozen $5/$6/$7, 2/3 calendar DTE, 10-point short-put benchmark and all prior raw caches/receipts remain unchanged; reference hashes are recorded and verified. Phase 3D's intended primary execution proxy is spread midpoint with modest adverse slippage. Displayed leg naturals, sizes and persistence are diagnostics, not candidate rejection criteria. Nothing in this checkpoint downgrades a spread for lack of a displayed 20-lot natural fill. Prospective complex-order execution begins at size 1 before scaling. No new execution backtest or parameter/morphology optimization was run.

## Verification and continuation

**203 tests passed**, including Phase 3A/3B/3C regressions and Phase 3D calendar bounds, wrong-universe/time checks, record-cap censoring, empty responses, spend/disk admission, stale rates, failure evidence, duplicate prevention, transfer-only recovery, corruption rejection and the durable budget stop. The owned record profile reproduced exactly with zero network calls.

Resolve a correct narrower opening-universe design from owned data, or revise the authorized budget/storage, before additional acquisition. A narrower selector must retain structures emerging throughout 09:30–10:00 and enough adjacent strikes; simply selecting from a single 09:31 snapshot would not fulfill the assignment. No new estimator is required to inspect the owned event data. The current checkpoint deliberately leaves the expanded panel incomplete rather than silently narrowing coverage or exceeding the ceiling.

Local-only reproduction of the acquired opening's DTE/side record profile:

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_phase3d --profile-owned
```

No subscription changes, live broker actions or live trading-code changes occurred.
