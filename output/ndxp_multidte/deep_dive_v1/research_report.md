# NDXP rich-credit research: Phases 1 and 2

Research only. Fixed rules; no acquisition, live changes, feature fitting, or holdout tuning. All dollar P/L uses 20 contracts and $1.324 per contract per side.

Path store: 3,738 candidate rows, 3,729 usable paths, 9 explicitly missing. Legacy baseline match: True across 30 groups.

## Development-only $6 → $3 path anatomy

| DTE | Category | n / family n | Median elapsed minutes to $3 | Median max debit before $3 | Median observed underwater minutes |
|---|---|---|---|---|---|
| 2 | clean_success | 40 / 396 | 47.50 | 6.10 | 0.50 |
| 2 | adverse_then_success | 252 / 396 | 1,500.00 | 8.55 | 236.50 |
| 2 | failure | 104 / 396 | — | — | — |
| 3 | clean_success | 55 / 382 | 64.00 | 6.05 | 1.00 |
| 3 | adverse_then_success | 230 / 382 | 1,652.50 | 8.35 | 169.50 |
| 3 | failure | 97 / 382 | — | — | — |

Success here means a midpoint touch, not a guaranteed fill or positive net P/L. Clean success never exceeds entry midpoint + $0.50 before $3. Adverse-then-success exceeds that level; severe $9-before-$3 counts are separately recorded in failure_anatomy.csv. Elapsed timing includes nights/weekends; underwater minutes exclude unobserved intervals. Failures are right-censored at their last quote, not necessarily settlement failures.

## Prespecified $6 → $3 execution sensitivity (all available dates)

| DTE | Adverse cents/side | Exit rule | n | Win rate | Target rate | Mean P/L | PF | Drawdown | Losing streak |
|---|---|---|---|---|---|---|---|---|---|
| 2 | 0 | next_minute | 643 | 62.2% | 76.2% | $574.49 | 1.27 | $75,141.76 | 7 |
| 2 | 0 | persist_2 | 643 | 62.1% | 59.6% | $1,366.87 | 1.48 | $87,353.60 | 8 |
| 2 | 0 | persist_3 | 643 | 59.4% | 56.5% | $982.27 | 1.32 | $88,953.60 | 8 |
| 2 | 0 | touch | 643 | 79.8% | 79.2% | $4,219.75 | 3.79 | $56,676.64 | 5 |
| 2 | 5 | next_minute | 643 | 60.7% | 75.4% | $396.26 | 1.18 | $80,712.48 | 7 |
| 2 | 5 | persist_2 | 643 | 61.0% | 58.8% | $1,180.94 | 1.40 | $91,053.60 | 8 |
| 2 | 5 | persist_3 | 643 | 58.6% | 55.8% | $816.32 | 1.26 | $89,453.60 | 8 |
| 2 | 5 | touch | 643 | 78.7% | 78.2% | $3,981.95 | 3.47 | $57,176.64 | 5 |
| 2 | 10 | next_minute | 643 | 60.3% | 74.7% | $248.67 | 1.11 | $94,037.12 | 5 |
| 2 | 10 | persist_2 | 643 | 60.2% | 58.0% | $934.13 | 1.30 | $95,853.60 | 8 |
| 2 | 10 | persist_3 | 643 | 58.0% | 55.4% | $633.04 | 1.20 | $99,839.44 | 8 |
| 2 | 10 | touch | 643 | 78.2% | 77.8% | $3,816.95 | 3.27 | $58,376.64 | 5 |
| 2 | 15 | next_minute | 643 | 58.9% | 74.2% | $69.51 | 1.03 | $115,387.12 | 7 |
| 2 | 15 | persist_2 | 643 | 59.6% | 57.5% | $775.11 | 1.25 | $106,487.12 | 8 |
| 2 | 15 | persist_3 | 643 | 57.2% | 54.7% | $464.46 | 1.14 | $104,152.24 | 8 |
| 2 | 15 | touch | 643 | 77.4% | 77.0% | $3,606.45 | 3.06 | $59,476.64 | 5 |
| 2 | 25 | next_minute | 643 | 56.3% | 73.4% | $-253.43 | 0.90 | $220,509.28 | 8 |
| 2 | 25 | persist_2 | 643 | 58.8% | 56.9% | $437.40 | 1.13 | $132,037.12 | 9 |
| 2 | 25 | persist_3 | 643 | 56.6% | 54.3% | $165.31 | 1.05 | $156,952.24 | 9 |
| 2 | 25 | touch | 643 | 76.5% | 76.2% | $3,236.08 | 2.73 | $77,318.08 | 7 |
| 3 | 0 | next_minute | 600 | 63.8% | 77.2% | $743.04 | 1.35 | $70,347.68 | 5 |
| 3 | 0 | persist_2 | 600 | 62.2% | 60.8% | $1,400.12 | 1.50 | $84,847.68 | 6 |
| 3 | 0 | persist_3 | 600 | 59.7% | 57.2% | $1,038.79 | 1.34 | $87,447.68 | 6 |
| 3 | 0 | touch | 600 | 79.0% | 79.3% | $4,240.29 | 3.74 | $30,223.68 | 3 |
| 3 | 5 | next_minute | 600 | 61.7% | 76.5% | $565.57 | 1.25 | $75,447.68 | 5 |
| 3 | 5 | persist_2 | 600 | 61.7% | 60.0% | $1,235.49 | 1.43 | $93,147.68 | 6 |
| 3 | 5 | persist_3 | 600 | 59.5% | 57.0% | $935.32 | 1.30 | $91,747.68 | 6 |
| 3 | 5 | touch | 600 | 78.5% | 78.8% | $4,099.66 | 3.55 | $31,623.68 | 4 |
| 3 | 10 | next_minute | 600 | 60.3% | 76.3% | $391.57 | 1.17 | $81,400.64 | 5 |
| 3 | 10 | persist_2 | 600 | 61.2% | 59.7% | $1,055.91 | 1.35 | $96,747.68 | 6 |
| 3 | 10 | persist_3 | 600 | 58.8% | 56.3% | $745.91 | 1.24 | $97,825.28 | 6 |
| 3 | 10 | touch | 600 | 78.0% | 78.3% | $3,895.49 | 3.32 | $33,023.68 | 4 |
| 3 | 15 | next_minute | 600 | 59.2% | 75.8% | $194.24 | 1.08 | $108,183.84 | 5 |
| 3 | 15 | persist_2 | 600 | 60.8% | 59.5% | $928.57 | 1.31 | $100,847.68 | 6 |
| 3 | 15 | persist_3 | 600 | 58.5% | 56.0% | $607.91 | 1.19 | $112,242.32 | 6 |
| 3 | 15 | touch | 600 | 77.7% | 78.2% | $3,748.16 | 3.18 | $34,323.68 | 4 |
| 3 | 25 | next_minute | 600 | 56.2% | 74.2% | $-165.26 | 0.94 | $176,133.84 | 5 |
| 3 | 25 | persist_2 | 600 | 60.3% | 58.5% | $611.82 | 1.19 | $148,092.32 | 6 |
| 3 | 25 | persist_3 | 600 | 57.8% | 55.3% | $289.66 | 1.09 | $196,042.32 | 7 |
| 3 | 25 | touch | 600 | 76.5% | 77.0% | $3,339.16 | 2.79 | $50,823.68 | 5 |

2DTE: mean P/L falls from $3,982 on a five-cent touch to $816 with three-minute persistence. At 25 cents and next-minute execution it is $-253. The apparent edge is highly sensitive to transient marks; midpoint-touch results alone are insufficient evidence of executable profit.

3DTE: mean P/L falls from $4,100 on a five-cent touch to $935 with three-minute persistence. At 25 cents and next-minute execution it is $-165. The apparent edge is highly sensitive to transient marks; midpoint-touch results alone are insufficient evidence of executable profit.

The full $5/$6/$7 × 2/3DTE × four targets × five slippages × four persistence modes is in execution_robustness.csv. These unconditional fixed sensitivities do not select a rule or constitute final-holdout validation.

## Caveats and phase boundary

The two missing acquisition sessions are 2026-09-22 and 2026-09-23. Known degraded dates 2024-06-03 and 2025-10-22 are flagged both at entry and when crossed by a path; source is the governing specification because original warning logs were not found. Prices outside [0,10] and unmatched leg minutes are excluded and counted. No long-gap forward fills are used.

Path diagnostics count 238,028 out-of-bounds spread observations and 10,348 unmatched minutes across candidate paths (overlapping candidates can repeat observations); 911 candidates end more than one minute before expiration close.

Terminal prices are quote proxies, not PM settlement. Next-minute missing quotes fall back to that proxy and are counted. Drawdown is entry-ordered realized P/L per family, not portfolio mark-to-market risk. Repeated credits and overlapping trades are not independent observations.

Phase 3 must inventory local underlying/volatility histories and audit causal availability. No acquisition is authorized. Settlement-source verification and the two missing paths remain data gaps. Entry, regime, matched-pair, validation and holdout-rule work are deferred.

## Reproduce

```sh
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_multidte_validation
PYTHONPATH=src .venv/bin/python -m mgc_v05l.research.ndxp_multidte_deep_dive
PYTHONPATH=src .venv/bin/python -m pytest -q tests/unit/test_ndxp_multidte*.py tests/unit/test_mgc_v05l_ndxp_naive*.py
```

Both commands use local files only and verify raw SHA-256 manifests. Derived outputs are written exclusively under output/ndxp_multidte/deep_dive_v1/.
