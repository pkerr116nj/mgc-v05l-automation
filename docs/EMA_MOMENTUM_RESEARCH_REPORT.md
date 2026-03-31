# EMA Momentum Research Report

This report/export layer is a research-only inspection path for the persisted EMA momentum evaluator results. It does not place trades, gate production entries, alter Bull Snap / Bear Snap / VWAP reclaim behavior, or act as a backtest.

## What The Report Summarizes

For a chosen `experiment_run_id`, the report summarizes:

- total analyzed bars
- baseline long context count
- baseline short context count
- filter-track pass counts
- math-trigger counts
- structure-label counts for:
  - compression
  - reclaim/failure
  - separation
  - combined structure candidates
- overlap counts between baseline context and the research tracks
- overlap counts between structure labels and the math-trigger track
- overlap counts between structure candidates and baseline context
- bars where math-trigger fired without baseline context
- bars where baseline context existed but the filter track blocked it

Baseline context currently comes from the persisted raw production-context fields already stored in `signal_evaluations`:

- `bull_snap_raw`
- `bear_snap_raw`
- `asia_vwap_reclaim_raw`

## What The CSV Export Contains

The per-bar CSV export includes:

- `ticker`
- `timeframe`
- `timestamp`
- baseline raw context fields
- EMA momentum interpretation flags
- filter-track and math-trigger-track labels
- structure-label columns:
  - `compression_long`
  - `compression_short`
  - `reclaim_long`
  - `failure_short`
  - `separation_long`
  - `separation_short`
  - `structure_long_candidate`
  - `structure_short_candidate`
- research-only quality and size placeholders
- `warmup_complete`
- key continuous EMA momentum feature values from `derived_features`

## How To Use It

Use the report to compare three parallel views of the same bars:

- baseline context from the current production strategy family
- filter-track labels showing where the math layer would have allowed or blocked baseline context
- math-trigger labels showing where the math layer would have identified a turn on its own
- structure labels showing where derivative-based turns appear in a practical sequence:
  - compression present
  - reclaim/failure present
  - separation present
  - combined structure candidate present

In the report:

- `compression` represents a first-pass derivative-based weakening or tightening move before a turn
- `reclaim_long` / `failure_short` represent first-pass structural recovery or loss zones using VWAP or trailing micro-range context
- `separation` represents moving away from the reclaim/failure area without immediately losing it
- `structure_*_candidate` represents the combined research-only structural candidate

CLI usage:

```bash
mgc-v05l research-ema-eval-report \
  --config config/base.yaml \
  --config config/replay.yaml \
  --experiment-run-id 1 \
  --output /tmp/ema_eval_report.csv
```

If `--output` is omitted, the command still prints a JSON summary to the terminal.

## What Is Intentionally Not Implemented Yet

- production entry gating
- live sizing logic
- trailing regression
- backtesting logic
- production trigger rewrites

This layer is intentionally additive and read-only.
It is still research-only, not a backtest and not a production signal path.
