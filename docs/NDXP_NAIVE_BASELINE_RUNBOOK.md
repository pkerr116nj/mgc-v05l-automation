# NDXP naive 0DTE baseline

This research lane tests one mechanical question: what happens when a 10-point
NDXP put credit spread is opened at the cash-market open and held intact through
official PM settlement?

## Frozen baseline

- Entry window: 09:30:00 through 09:30:59 ET.
- Entry price: earliest valid synchronized two-leg NBBO midpoint.
- Quote-quality limit: no leg or parity quote more than two seconds old.
- Instrument: same-day NDXP put vertical, 10 points wide.
- Exit: official XQC settlement.
- No profit target, stop, adjustment, re-entry, tape filter, or volatility filter.
- Unit size: one spread.
- Entry execution cases: midpoint, midpoint less $0.25, and midpoint less $0.50.
- Per-spread entry cost: $1.324. This is an editable research assumption.

Every valid spread is retained before creating two predeclared comparisons:

1. Short-strike offset from the causal opening reference: -50, -25, 0, +25,
   and +50 NDX points.
2. Opening midpoint credit nearest $2, $3, $4, $5, $6, $7, and $8.

The opening reference uses supplied spot when available. Otherwise it uses the
median European call-put parity level across at least three synchronized
strikes. It never uses quotes arriving after a candidate's entry time.

## Data sources

- Opening option NBBOs: Databento `OPRA.PILLAR`, `cbbo-1s`, requested through
  parent symbol `NDXP.OPT` for one minute per session.
- Settlement: `NASDAQXQC`, Nasdaq's NDXP PM settlement series as distributed by
  FRED.

Databento charges for historical retrieval. `download-quotes` first estimates
the request and requires an explicit `--max-cost`; it aborts before download if
the estimate exceeds that amount. Cached DBN files prevent paid re-downloads.

## Commands on Mars

Use the repository's Python environment and load the existing Databento key
without displaying it.

```bash
python -m mgc_v05l.research.ndxp_naive_data estimate \
  --start 2023-03-01 --end 2026-09-18
```

After reviewing the estimate, download with a deliberately chosen ceiling:

```bash
python -m mgc_v05l.research.ndxp_naive_data download-quotes \
  --start 2023-03-01 --end 2026-09-18 \
  --output outputs/ndxp_naive_baseline/opening_quotes.csv \
  --cache-dir outputs/ndxp_naive_baseline/dbn_cache \
  --max-cost 0.00
```

Replace `0.00` only after reviewing the estimate. Then retrieve settlement
values and run the analysis:

```bash
python -m mgc_v05l.research.ndxp_naive_data download-settlements \
  --start 2023-03-01 --end 2026-09-18 \
  --output outputs/ndxp_naive_baseline/xqc_settlements.csv

python -m mgc_v05l.research.ndxp_naive_baseline \
  --quotes outputs/ndxp_naive_baseline/opening_quotes.csv \
  --settlements outputs/ndxp_naive_baseline/xqc_settlements.csv \
  --output-dir outputs/ndxp_naive_baseline/results
```

The results directory contains the complete candidate set, selected daily
outcomes, summaries for the full sample and each calendar year, and a JSON
report with exclusions. Missing or invalid sessions remain visible rather than
being silently filled.
